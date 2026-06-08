"""Typed Python facade for the Rust sandbox runtime."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import gzip
import hashlib
import json
import logging
import math
import os
import struct
import tempfile
import traceback
import urllib.request
import weakref
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from importlib import resources
from importlib.abc import Traversable
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, cast

from . import _native

DEFAULT_WALL_TIME_SECONDS = 30.0
DEFAULT_EVENT_QUEUE_SIZE = 4096
LOGGER = logging.getLogger(__name__)
STANDARD_PACKAGE_NAMES = (
    "coreutils",
    "bash",
    "grep",
    "sed",
    "find",
    "tar",
    "gzip",
    "python",
)


class _WallTimeSecondsUnset:
    """Sentinel for omitted per-process wall-time overrides."""


_WALL_TIME_SECONDS_UNSET = _WallTimeSecondsUnset()
_DEFAULT_WALL_TIME_SECONDS_ARG = cast("float | None", _WALL_TIME_SECONDS_UNSET)


class SandboxError(RuntimeError):
    """Error raised when a sandbox operation cannot be completed."""


@dataclass(frozen=True)
class File:
    """A file to place in the sandbox filesystem.

    :ivar data: File contents as bytes.
    """

    data: bytes

    @classmethod
    def text(cls, text: str, encoding: str = "utf-8") -> File:
        """:param text: Text to encode into the file.
        :param encoding: Encoding to use.
        :returns: File instance containing encoded text.
        """
        return cls(text.encode(encoding))

    @classmethod
    def bytes(cls, data: bytes) -> File:
        """:param data: File bytes.
        :returns: File instance containing the supplied bytes.
        """
        return cls(data)


@dataclass(frozen=True)
class Directory:
    """A directory to create in the sandbox filesystem."""


@dataclass(frozen=True)
class HostMount:
    """A live host directory mount inside the sandbox filesystem.

    :ivar source: Host directory to expose.
    :ivar target: Absolute sandbox directory path.
    :ivar read_only: Whether sandbox processes can only read from the mount.
    """

    source: str | Path
    target: str
    read_only: bool = True

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when a mount path is invalid."""
        if "\0" in str(self.source):
            raise ValueError("host mount source cannot contain NUL bytes")
        if "\0" in self.target:
            raise ValueError("host mount target cannot contain NUL bytes")
        if not self.target.startswith("/"):
            raise ValueError("host mount target must be absolute")

    def _native_tuple(self) -> tuple[str, str, bool]:
        """:returns: Native mount configuration tuple."""
        return (str(Path(self.source).expanduser()), self.target, self.read_only)


class _StringEnum(str, Enum):
    """String-valued enum with ``StrEnum``-style string conversion."""

    def __str__(self) -> str:
        """:returns: The enum value."""
        return cast(str, self.value)


class PackageSource(_StringEnum):
    """Sources from which a Wasmer package can be loaded."""

    BUNDLED = "bundled"
    LOCAL = "local"
    URL = "url"


@dataclass(frozen=True)
class PackageCommandAlias:
    """An additional command name exposed for a package command.

    :ivar alias: Command name to expose on the sandbox PATH.
    :ivar command: Package command that should run when the alias is invoked.
    """

    alias: str
    command: str

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when a command name is invalid."""
        _validate_package_command_name(self.alias, "alias")
        _validate_package_command_name(self.command, "command")


if TYPE_CHECKING:
    PackageCommandAliasInput = PackageCommandAlias | tuple[str, str]
else:
    PackageCommandAliasInput = object


@dataclass(frozen=True)
class WasmerPackage:
    """A Wasmer WEBC package available inside a sandbox image.

    :ivar name: Logical package name used by the sandbox image.
    :ivar source: Package source kind.
    :ivar path: Local WEBC path for local packages.
    :ivar url: WEBC URL for downloaded packages.
    :ivar sha256: Expected expanded WEBC SHA-256 digest.
    :ivar command_aliases: Additional command aliases exposed on PATH.
    """

    name: str
    source: PackageSource
    path: str | Path | None = None
    url: str | None = None
    sha256: str | None = None
    command_aliases: tuple[PackageCommandAlias, ...] = ()

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when the package configuration is invalid."""
        if len(self.name) == 0:
            raise ValueError("package name cannot be empty")
        if "\0" in self.name:
            raise ValueError("package name cannot contain NUL bytes")

        source = PackageSource(self.source)
        object.__setattr__(self, "source", source)
        object.__setattr__(
            self,
            "command_aliases",
            _normalize_package_command_aliases(self.command_aliases),
        )

        if self.sha256 is not None:
            _validate_sha256(self.sha256, "package sha256")

        if source is PackageSource.BUNDLED:
            if self.path is not None or self.url is not None:
                raise ValueError("bundled packages cannot define path or url")
            return

        if source is PackageSource.LOCAL:
            if self.path is None:
                raise ValueError("local packages require a path")
            if self.url is not None:
                raise ValueError("local packages cannot define url")
            return

        if self.url is None:
            raise ValueError("url packages require a url")
        if self.path is not None:
            raise ValueError("url packages cannot define path")
        if self.sha256 is None:
            raise ValueError("url packages require sha256")

    @classmethod
    def bundled(
        cls,
        name: str,
        *,
        command_aliases: Iterable[PackageCommandAliasInput] = (),
    ) -> WasmerPackage:
        """:param name: Bundled package name.
        :param command_aliases: Additional command aliases exposed on PATH.
        :returns: Package loaded from the package's bundled assets.
        """
        return cls(
            name=name,
            source=PackageSource.BUNDLED,
            command_aliases=_normalize_package_command_aliases(command_aliases),
        )

    @classmethod
    def local_webc(
        cls,
        name: str,
        path: str | Path,
        *,
        sha256: str | None = None,
        command_aliases: Iterable[PackageCommandAliasInput] = (),
    ) -> WasmerPackage:
        """:param name: Logical package name.
        :param path: Local WEBC path.
        :param sha256: Expected WEBC SHA-256 digest.
        :param command_aliases: Additional command aliases exposed on PATH.
        :returns: Package loaded from a local WEBC file.
        """
        return cls(
            name=name,
            source=PackageSource.LOCAL,
            path=path,
            sha256=sha256,
            command_aliases=_normalize_package_command_aliases(command_aliases),
        )

    @classmethod
    def url_webc(
        cls,
        name: str,
        url: str,
        *,
        sha256: str,
        command_aliases: Iterable[PackageCommandAliasInput] = (),
    ) -> WasmerPackage:
        """:param name: Logical package name.
        :param url: URL for a WEBC package.
        :param sha256: Expected WEBC SHA-256 digest.
        :param command_aliases: Additional command aliases exposed on PATH.
        :returns: Package downloaded from a URL-backed WEBC file.
        """
        return cls(
            name=name,
            source=PackageSource.URL,
            url=url,
            sha256=sha256,
            command_aliases=_normalize_package_command_aliases(command_aliases),
        )


@dataclass(frozen=True)
class SandboxImage:
    """Composable package image used to create sandbox process environments.

    :ivar packages: Ordered Wasmer packages available in the image.
    """

    packages: tuple[WasmerPackage, ...] = ()

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when package names are duplicated."""
        packages = tuple(self.packages)
        names: set[str] = set()
        for package in packages:
            if package.name in names:
                raise ValueError(f"package name configured more than once: {package.name}")
            names.add(package.name)
        object.__setattr__(self, "packages", packages)

    @classmethod
    def standard(cls) -> SandboxImage:
        """:returns: Standard UNIX-like image bundled with this package."""
        return cls(
            tuple(
                WasmerPackage.bundled(
                    name,
                    command_aliases=(
                        (PackageCommandAlias("python3", "python"),)
                        if name == "python"
                        else ()
                    ),
                )
                for name in STANDARD_PACKAGE_NAMES
            )
        )

    @classmethod
    def empty(cls) -> SandboxImage:
        """:returns: Empty image with no Wasmer packages."""
        return cls()

    @classmethod
    def from_packages(cls, packages: Iterable[WasmerPackage]) -> SandboxImage:
        """:param packages: Packages to include.
        :returns: Image containing the supplied packages.
        """
        return cls(tuple(packages))

    def with_packages(self, *packages: WasmerPackage) -> SandboxImage:
        """:param packages: Packages to append to the image.
        :returns: Image with the supplied packages appended.
        """
        return type(self)((*self.packages, *packages))

    def without(self, *names: str) -> SandboxImage:
        """:param names: Package names to remove.
        :returns: Image without packages matching the supplied names.
        """
        removed = frozenset(names)
        return type(self)(
            tuple(package for package in self.packages if package.name not in removed)
        )


class SandboxEventKind(_StringEnum):
    """Filesystem event kinds emitted by a sandbox."""

    FILE_CREATED = "file_created"
    FILE_MODIFIED = "file_modified"
    FILE_METADATA_MODIFIED = "file_metadata_modified"
    FILE_REMOVED = "file_removed"
    DIRECTORY_CREATED = "directory_created"
    DIRECTORY_REMOVED = "directory_removed"
    PATH_RENAMED = "path_renamed"
    EVENTS_DROPPED = "events_dropped"


@dataclass(frozen=True)
class SandboxEvent:
    """A filesystem event emitted by a sandbox.

    :ivar sequence: Monotonic event sequence number for this sandbox.
    :ivar kind: Event kind.
    :ivar path: Primary sandbox path associated with the event.
    :ivar target_path: Destination path for rename events.
    :ivar dropped_count: Number of events dropped before an overflow notification.
    """

    sequence: int
    kind: SandboxEventKind
    path: str
    target_path: str | None = None
    dropped_count: int = 0

    @classmethod
    def _from_native(cls, event: tuple[int, str, str, str | None, int]) -> SandboxEvent:
        """:param event: Native event tuple.
        :returns: Python event object.
        """
        sequence, kind, path, target_path, dropped_count = event
        return cls(
            sequence=sequence,
            kind=SandboxEventKind(kind),
            path=path,
            target_path=target_path,
            dropped_count=dropped_count,
        )


if TYPE_CHECKING:
    FilesystemEventHandler = Callable[[SandboxEvent], Awaitable[None] | None]
else:
    FilesystemEventHandler = Callable[[SandboxEvent], object]


@dataclass
class _EventHandlerRegistration:
    """A registered event handler and its delivery filter.

    :ivar handler: Handler to invoke for matching events.
    :ivar event_types: Event kinds to deliver.
    :ivar path_prefix: Sandbox path prefix to deliver.
    :ivar queue: Pending events for this handler.
    :ivar worker_task: Task delivering queued events to the handler.
    :ivar dropped_count: Number of queued events dropped due to handler backpressure.
    """

    handler: FilesystemEventHandler
    event_types: frozenset[SandboxEventKind] | None
    path_prefix: str | None
    queue: asyncio.Queue[SandboxEvent]
    worker_task: asyncio.Task[None]
    dropped_count: int = 0


class EventSubscription:
    """A handle for removing a sandbox event handler."""

    _sandbox: Sandbox
    _token: int
    _closed: bool

    def __init__(self, sandbox: Sandbox, token: int) -> None:
        """:param sandbox: Sandbox that owns the handler.
        :param token: Handler token to remove.
        """
        self._sandbox = sandbox
        self._token = token
        self._closed = False

    @property
    def closed(self) -> bool:
        """:returns: Whether the subscription has been closed."""
        return self._closed

    def close(self) -> None:
        """Remove the handler from its sandbox."""
        if self._closed:
            return
        self._closed = True
        self._sandbox._remove_event_subscription(self._token)

    async def aclose(self) -> None:
        """Remove the handler from its sandbox."""
        self.close()


@dataclass(frozen=True)
class CompletedProcess:
    """A finished sandbox process.

    :ivar args: Command arguments.
    :ivar returncode: Process return code.
    :ivar stdout: Captured stdout bytes.
    :ivar stderr: Captured stderr bytes.
    """

    args: tuple[str, ...]
    returncode: int
    stdout: bytes
    stderr: bytes

    @property
    def stdout_text(self) -> str:
        """:returns: Standard output decoded as UTF-8."""
        return self.stdout.decode()

    @property
    def stderr_text(self) -> str:
        """:returns: Standard error decoded as UTF-8."""
        return self.stderr.decode()

    def check_returncode(self) -> None:
        """:raises SandboxError: Raised when the process returned a non-zero status."""
        if self.returncode == 0:
            return
        raise SandboxError(
            f"command {self.args!r} returned non-zero exit status {self.returncode}"
        )


class SandboxProcess:
    """A running sandbox process with writable standard input."""

    _native_process: _native.StartedProcess
    _completed_process: CompletedProcess | None

    def __init__(self, native_process: _native.StartedProcess) -> None:
        """:param native_process: Native process handle."""
        self._native_process = native_process
        self._completed_process = None

    def __del__(self) -> None:
        """Release a running process if the handle is abandoned."""
        try:
            if self._completed_process is not None:
                return
            if not self._native_process.is_running():
                return
            self._native_process.cancel()
            self._native_process.wait_blocking()
        except Exception:
            LOGGER.debug("sandbox process destructor cleanup failed\n%s", traceback.format_exc())

    async def __aenter__(self) -> SandboxProcess:
        """:returns: This running process."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """:param exc_type: Exception type raised in the context.
        :param exc_value: Exception value raised in the context.
        :param traceback: Traceback raised in the context.
        """
        await self.aclose()

    @property
    def args(self) -> tuple[str, ...]:
        """:returns: Command arguments."""
        return tuple(self._native_process.args)

    @property
    def returncode(self) -> int | None:
        """:returns: Process return code when the process has finished."""
        if self._completed_process is not None:
            return self._completed_process.returncode
        return self._native_process.returncode

    @property
    def running(self) -> bool:
        """:returns: Whether the process is still running."""
        return self._native_process.is_running()

    @property
    def stdin_closed(self) -> bool:
        """:returns: Whether standard input has been closed."""
        try:
            return self._native_process.stdin_closed
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    @property
    def stdout(self) -> bytes:
        """:returns: Captured standard output produced so far."""
        try:
            return self._native_process.stdout
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    @property
    def stderr(self) -> bytes:
        """:returns: Captured standard error produced so far."""
        try:
            return self._native_process.stderr
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    @property
    def stdout_text(self) -> str:
        """:returns: Captured standard output produced so far decoded as UTF-8."""
        return self.stdout.decode()

    @property
    def stderr_text(self) -> str:
        """:returns: Captured standard error produced so far decoded as UTF-8."""
        return self.stderr.decode()

    def write_stdin_nowait(self, data: bytes | str, encoding: str = "utf-8") -> None:
        """:param data: Bytes or text to write to standard input.
        :param encoding: Encoding to use for text input.
        """
        data_bytes = data.encode(encoding) if isinstance(data, str) else data
        try:
            self._native_process.write_stdin(data_bytes)
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    async def write_stdin(self, data: bytes | str, encoding: str = "utf-8") -> None:
        """:param data: Bytes or text to write to standard input.
        :param encoding: Encoding to use for text input.
        """
        self.write_stdin_nowait(data, encoding)

    def close_stdin_nowait(self) -> None:
        """Close standard input."""
        try:
            self._native_process.close_stdin()
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    async def close_stdin(self) -> None:
        """Close standard input."""
        self.close_stdin_nowait()

    def cancel(self) -> None:
        """Cancel the running process."""
        self._native_process.cancel()

    def terminate(self) -> None:
        """Cancel the running process."""
        self.cancel()

    def kill(self) -> None:
        """Cancel the running process."""
        self.cancel()

    async def aclose(self) -> None:
        """Cancel the process and wait for native cleanup."""
        self.cancel()
        try:
            await self.wait()
        except asyncio.CancelledError:
            with contextlib.suppress(SandboxError):
                await self.wait()
            raise
        except SandboxError:
            return

    async def wait(self, *, check: bool = False) -> CompletedProcess:
        """:param check: Whether to raise on a non-zero return code.
        :returns: Completed process details.
        :raises SandboxError: Raised when the process fails or check is true and the command fails.
        """
        if self._completed_process is None:
            self._completed_process = await self._wait_native()
        if check:
            self._completed_process.check_returncode()
        return self._completed_process

    async def communicate(
        self,
        input: bytes | str | None = None,
        *,
        check: bool = False,
    ) -> CompletedProcess:
        """:param input: Bytes or text to write before closing standard input.
        :param check: Whether to raise on a non-zero return code.
        :returns: Completed process details.
        """
        if input is not None:
            await self.write_stdin(input)
        await self.close_stdin()
        return await self.wait(check=check)

    async def _wait_native(self) -> CompletedProcess:
        """:returns: Completed process details from the native handle."""
        try:
            native_result = await self._native_process.wait()
        except RuntimeError as error:
            raise SandboxError(str(error)) from error
        return CompletedProcess(
            args=tuple(native_result.args),
            returncode=native_result.returncode,
            stdout=native_result.stdout,
            stderr=native_result.stderr,
        )


class VirtualProcessOutput:
    """A bounded output stream for a virtual executable invocation."""

    _chunks: list[bytes]
    _limit: int
    _size: int

    def __init__(self, limit: int) -> None:
        """:param limit: Maximum number of bytes to capture."""
        self._chunks = []
        self._limit = limit
        self._size = 0

    async def write(self, data: bytes | str) -> None:
        """:param data: Bytes or text to append.
        :raises SandboxError: Raised when the output limit would be exceeded.
        """
        data_bytes = data.encode() if isinstance(data, str) else data
        self.write_nowait(data_bytes)

    def write_nowait(self, data: bytes | str) -> None:
        """:param data: Bytes or text to append.
        :raises SandboxError: Raised when the output limit would be exceeded.
        """
        data_bytes = data.encode() if isinstance(data, str) else data
        next_size = self._size + len(data_bytes)
        if next_size > self._limit:
            raise SandboxError(f"virtual executable output exceeded {self._limit} bytes")
        self._chunks.append(data_bytes)
        self._size = next_size

    def data(self) -> bytes:
        """:returns: Captured output bytes."""
        return b"".join(self._chunks)


@dataclass(frozen=True)
class CommandResult:
    """A result returned by a virtual executable handler.

    :ivar returncode: Process return code.
    :ivar stdout: Standard output bytes.
    :ivar stderr: Standard error bytes.
    """

    returncode: int = 0
    stdout: bytes = b""
    stderr: bytes = b""


@dataclass
class CommandInvocation:
    """A virtual executable invocation.

    :ivar sandbox: Sandbox that owns the executable.
    :ivar executable_path: Absolute executable path used for dispatch.
    :ivar argv: Process arguments.
    :ivar cwd: Current working directory.
    :ivar env: Environment variables.
    :ivar stdin: Captured standard input bytes.
    :ivar stdout: Standard output stream.
    :ivar stderr: Standard error stream.
    """

    sandbox: Sandbox
    executable_path: str
    argv: tuple[str, ...]
    cwd: str
    env: dict[str, str]
    stdin: bytes
    stdout: VirtualProcessOutput
    stderr: VirtualProcessOutput

    @property
    def stdin_text(self) -> str:
        """:returns: Standard input decoded as UTF-8."""
        return self.stdin.decode()

    async def run(
        self,
        args: list[str] | tuple[str, ...],
        *,
        input: bytes | str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        limits: Limits | None = None,
        wall_time_seconds: float | None = _DEFAULT_WALL_TIME_SECONDS_ARG,
        check: bool = False,
    ) -> CompletedProcess:
        """:param args: Command and arguments.
        :param input: Bytes or text to pass as stdin.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param limits: Complete per-process resource limits.
        :param wall_time_seconds: Per-process wall-time override.
        :param check: Whether to raise on a non-zero return code.
        :returns: Completed process details.
        """
        return await self.sandbox.run(
            args,
            input=input,
            env=env,
            cwd=self.cwd if cwd is None else cwd,
            limits=limits,
            wall_time_seconds=wall_time_seconds,
            check=check,
        )

    async def read_file(self, path: str) -> bytes:
        """:param path: Absolute sandbox path.
        :returns: File contents.
        """
        return await self.sandbox.read_file(path)

    async def write_file(self, path: str, data: bytes) -> None:
        """:param path: Absolute sandbox path.
        :param data: File contents.
        """
        await self.sandbox.write_file(path, data)

    async def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        """:param path: Absolute sandbox path.
        :param text: Text to write.
        :param encoding: Encoding to use.
        """
        await self.sandbox.write_text(path, text, encoding)


if TYPE_CHECKING:
    VirtualExecutableResult = int | CommandResult | CompletedProcess | None
    VirtualExecutableHandler = Callable[
        [CommandInvocation],
        Awaitable[VirtualExecutableResult] | VirtualExecutableResult,
    ]
else:
    VirtualExecutableResult = object
    VirtualExecutableHandler = Callable[[CommandInvocation], object]


@dataclass(frozen=True)
class VirtualExecutable:
    """A host-backed executable exposed inside the sandbox.

    :ivar path: Absolute executable path.
    :ivar handler: Function that implements the executable.
    :ivar aliases: Additional absolute executable paths handled by the same function.
    :ivar replace: Whether an existing file at a configured path may be replaced.
    """

    path: str
    handler: VirtualExecutableHandler
    aliases: tuple[str, ...] = ()
    replace: bool = False

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when a configured path is invalid."""
        _normalize_virtual_executable_paths(self.path, self.aliases)


class VirtualExecutableRegistration:
    """A handle for removing a virtual executable."""

    _sandbox: Sandbox
    _token: int
    _closed: bool

    def __init__(self, sandbox: Sandbox, token: int) -> None:
        """:param sandbox: Sandbox that owns the virtual executable.
        :param token: Handler token to remove.
        """
        self._sandbox = sandbox
        self._token = token
        self._closed = False

    @property
    def closed(self) -> bool:
        """:returns: Whether the registration has been closed."""
        return self._closed

    def close(self) -> None:
        """Remove the virtual executable from its sandbox."""
        if self._closed:
            return
        self._closed = True
        self._sandbox._remove_virtual_executable(self._token)

    async def aclose(self) -> None:
        """Remove the virtual executable from its sandbox."""
        self.close()


@dataclass(frozen=True)
class Limits:
    """Resource limits applied to sandbox process execution.

    :ivar output_bytes: Maximum captured bytes for each output stream.
    :ivar wall_time_seconds: Maximum wall-clock time for a process.
    """

    output_bytes: int = 16 * 1024 * 1024
    wall_time_seconds: float | None = DEFAULT_WALL_TIME_SECONDS

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when a limit value is invalid."""
        if self.output_bytes < 0:
            raise ValueError("output_bytes must be greater than or equal to zero")
        if self.wall_time_seconds is None:
            return
        if math.isfinite(self.wall_time_seconds) and self.wall_time_seconds > 0.0:
            return
        raise ValueError("wall_time_seconds must be a positive finite number")


def _resolve_process_limits(
    default_limits: Limits,
    limits: Limits | None,
    wall_time_seconds: float | None | _WallTimeSecondsUnset,
) -> Limits:
    """:param default_limits: Sandbox-level process limits.
    :param limits: Complete per-process limit override.
    :param wall_time_seconds: Per-process wall-time override.
    :returns: Limits to apply to a single process.
    :raises ValueError: Raised when both override forms are supplied.
    """
    if limits is not None and not isinstance(wall_time_seconds, _WallTimeSecondsUnset):
        raise ValueError("limits and wall_time_seconds cannot both be set")
    if limits is not None:
        return limits
    if isinstance(wall_time_seconds, _WallTimeSecondsUnset):
        return default_limits
    return Limits(
        output_bytes=default_limits.output_bytes,
        wall_time_seconds=wall_time_seconds,
    )


@dataclass(frozen=True)
class SandboxConfig:
    """Configuration for a sandbox instance.

    :ivar image: Package image that defines available Wasmer commands.
    :ivar files: Filesystem entries to create before commands run.
    :ivar host_mounts: Live host directory mounts to expose inside the sandbox.
    :ivar virtual_executables: Host-backed executables to expose inside the sandbox.
    :ivar cwd: Default working directory.
    :ivar env: Default environment variables.
    :ivar limits: Default resource limits.
    :ivar event_queue_size: Maximum queued filesystem events before overflow.
    """

    image: SandboxImage = field(default_factory=SandboxImage.standard)
    files: dict[str, File | Directory] = field(default_factory=dict)
    host_mounts: list[HostMount] = field(default_factory=list)
    virtual_executables: list[VirtualExecutable] = field(default_factory=list)
    cwd: str = "/work"
    env: dict[str, str] = field(default_factory=dict)
    limits: Limits = field(default_factory=Limits)
    event_queue_size: int = DEFAULT_EVENT_QUEUE_SIZE

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when an event setting is invalid."""
        if self.event_queue_size > 0:
            return
        raise ValueError("event_queue_size must be greater than zero")


class Sandbox:
    """An isolated UNIX-like Wasmer sandbox."""

    _config: SandboxConfig
    _native_sandbox: _native.Sandbox
    _event_handlers: dict[int, _EventHandlerRegistration]
    _event_dispatch_task: asyncio.Task[None] | None
    _event_dispatch_generation: int
    _next_event_handler_token: int
    _virtual_executable_handlers: dict[int, VirtualExecutableHandler]
    _virtual_executable_dispatch_task: asyncio.Task[None] | None
    _virtual_executable_request_tasks: dict[int, asyncio.Task[None]]
    _next_virtual_executable_token: int
    _next_process_token: int

    def __init__(self, config: SandboxConfig | None = None) -> None:
        """:param config: Sandbox configuration."""
        self._config = config if config is not None else SandboxConfig()
        self._event_handlers = {}
        self._event_dispatch_task = None
        self._event_dispatch_generation = 0
        self._next_event_handler_token = 0
        self._virtual_executable_handlers = {}
        self._virtual_executable_dispatch_task = None
        self._virtual_executable_request_tasks = {}
        self._next_virtual_executable_token = 0
        self._next_process_token = 0
        files: dict[str, bytes | None] = {}
        for path, entry in self._config.files.items():
            if isinstance(entry, File):
                files[path] = entry.data
                continue
            files[path] = None

        packages = _prepare_image_packages(self._config.image)
        try:
            self._native_sandbox = _native.Sandbox(
                files,
                [mount._native_tuple() for mount in self._config.host_mounts],
                packages,
                self._config.cwd,
                self._config.env,
                self._config.event_queue_size,
            )
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

        for executable in self._config.virtual_executables:
            self.register_executable(
                executable.path,
                executable.handler,
                aliases=executable.aliases,
                replace=executable.replace,
            )

    def __del__(self) -> None:
        """Release native registrations owned by this sandbox."""
        try:
            self._shutdown_event_handlers()
            self.close_virtual_executables()
        except Exception:
            LOGGER.debug("sandbox destructor cleanup failed\n%s", traceback.format_exc())

    async def __aenter__(self) -> Sandbox:
        """:returns: This sandbox."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """:param exc_type: Exception type raised in the context.
        :param exc_value: Exception value raised in the context.
        :param traceback: Traceback raised in the context.
        """
        await self.aclose()

    async def aclose(self) -> None:
        """Close event handlers and virtual executable dispatch tasks."""
        tasks = self._async_tasks()
        try:
            self._shutdown_event_handlers()
            self.close_virtual_executables()
        finally:
            await _drain_cancelled_tasks(tasks)

    def register_executable(
        self,
        path: str,
        handler: VirtualExecutableHandler,
        *,
        aliases: Iterable[str] = (),
        replace: bool = False,
    ) -> VirtualExecutableRegistration:
        """:param path: Absolute executable path.
        :param handler: Function that implements the executable.
        :param aliases: Additional absolute executable paths handled by the same function.
        :param replace: Whether an existing file at a configured path may be replaced.
        :returns: Registration that removes the executable when closed.
        :raises SandboxError: Raised when native registration fails.
        :raises ValueError: Raised when a configured path is invalid.
        """
        paths = _normalize_virtual_executable_paths(path, tuple(aliases))
        token = self._next_virtual_executable_token
        self._next_virtual_executable_token += 1
        try:
            self._native_sandbox.register_virtual_executable(token, paths, replace)
        except RuntimeError as error:
            raise SandboxError(str(error)) from error
        self._virtual_executable_handlers[token] = handler
        return VirtualExecutableRegistration(self, token)

    def close_virtual_executables(self) -> None:
        """Remove all virtual executables from the sandbox."""
        tokens = tuple(self._virtual_executable_handlers)
        for token in tokens:
            self._remove_virtual_executable(token)

        self._stop_virtual_executable_dispatcher()

    def _remove_virtual_executable(self, token: int) -> None:
        """:param token: Handler token to remove."""
        self._virtual_executable_handlers.pop(token, None)
        try:
            self._native_sandbox.unregister_virtual_executable(token)
        except RuntimeError as error:
            raise SandboxError(str(error)) from error
        if len(self._virtual_executable_handlers) > 0:
            return
        self._stop_virtual_executable_dispatcher()

    def _stop_virtual_executable_dispatcher(self) -> None:
        """Stop virtual executable dispatch and active request tasks."""
        self._cancel_virtual_executable_requests()
        task = self._virtual_executable_dispatch_task
        self._virtual_executable_dispatch_task = None
        if task is None or task.done():
            return
        _cancel_task(task)

    def _ensure_virtual_executable_dispatcher(self) -> None:
        """:raises RuntimeError: Raised when no asyncio loop is running."""
        if len(self._virtual_executable_handlers) == 0:
            return
        task = self._virtual_executable_dispatch_task
        if task is not None and not task.done():
            return
        loop = asyncio.get_running_loop()
        sandbox_reference = weakref.ref(self)
        self._virtual_executable_dispatch_task = loop.create_task(
            _dispatch_virtual_processes(sandbox_reference)
        )

    def _cancel_virtual_executable_requests(self) -> None:
        """Cancel active virtual executable request tasks."""
        for task in tuple(self._virtual_executable_request_tasks.values()):
            if task.done():
                continue
            _cancel_task(task)

    def _start_virtual_process_request(self, request_id: int, payload: bytes) -> None:
        """:param request_id: Native request identifier.
        :param payload: Encoded invocation request.
        """
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._complete_virtual_process_request(request_id, payload))
        self._virtual_executable_request_tasks[request_id] = task
        task.add_done_callback(
            lambda completed: self._finish_virtual_process_request(request_id, completed)
        )

    def _finish_virtual_process_request(
        self,
        request_id: int,
        task: asyncio.Task[None],
    ) -> None:
        """:param request_id: Native request identifier.
        :param task: Completed request task.
        """
        self._virtual_executable_request_tasks.pop(request_id, None)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            LOGGER.error(
                "virtual executable request supervisor failed\n%s",
                traceback.format_exc(),
            )

    async def _complete_virtual_process_request(self, request_id: int, payload: bytes) -> None:
        """:param request_id: Native request identifier.
        :param payload: Encoded invocation request.
        """
        handler_task = asyncio.create_task(self._handle_virtual_process(payload))
        cancellation_task = asyncio.ensure_future(
            self._native_sandbox.wait_virtual_process_cancelled(request_id)
        )
        try:
            done, _pending = await asyncio.wait(
                {handler_task, cancellation_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_task in done:
                handler_task.cancel()
                await _await_cancelled_handler(handler_task)
                self._complete_virtual_process(
                    request_id,
                    _encode_virtual_executable_response(_virtual_handler_cancelled_result()),
                )
                return

            response = await handler_task
            cancellation_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancellation_task
            self._complete_virtual_process(request_id, response)
        except asyncio.CancelledError:
            handler_task.cancel()
            cancellation_task.cancel()
            await _await_cancelled_handler(handler_task)
            with contextlib.suppress(asyncio.CancelledError):
                await cancellation_task
            self._complete_virtual_process(
                request_id,
                _encode_virtual_executable_response(_virtual_handler_cancelled_result()),
            )
            raise

    def _complete_virtual_process(self, request_id: int, response: bytes) -> None:
        """:param request_id: Native request identifier.
        :param response: Encoded invocation response.
        """
        try:
            self._native_sandbox.complete_virtual_process(request_id, response)
        except RuntimeError:
            LOGGER.debug(
                "virtual executable response delivery failed\n%s",
                traceback.format_exc(),
            )

    async def _handle_virtual_process(self, payload: bytes) -> bytes:
        """:param payload: Encoded invocation request.
        :returns: Encoded invocation response.
        """
        try:
            data = json.loads(payload.decode("utf-8"))
            token = int(data["handler_token"])
            handler = self._virtual_executable_handlers[token]
            invocation = CommandInvocation(
                sandbox=self,
                executable_path=str(data["executable_path"]),
                argv=tuple(str(item) for item in data["argv"]),
                cwd=str(data["cwd"]),
                env={str(key): str(value) for key, value in data["env"].items()},
                stdin=base64.b64decode(str(data["stdin"])),
                stdout=VirtualProcessOutput(self._config.limits.output_bytes),
                stderr=VirtualProcessOutput(self._config.limits.output_bytes),
            )
            result = handler(invocation)
            if isinstance(result, Awaitable):
                result = await result
            command_result = _normalize_virtual_executable_result(invocation, result)
        except Exception:
            formatted = traceback.format_exc()
            LOGGER.error("virtual executable handler failed\n%s", formatted)
            command_result = _virtual_handler_error_result(
                formatted,
                self._config.limits.output_bytes,
            )
        return _encode_virtual_executable_response(command_result)

    def on_event(
        self,
        handler: FilesystemEventHandler,
        *,
        event_types: Iterable[SandboxEventKind | str] | None = None,
        path_prefix: str | None = None,
    ) -> EventSubscription:
        """:param handler: Handler to invoke when a matching event occurs.
        :param event_types: Event kinds to deliver, or all kinds when omitted.
        :param path_prefix: Absolute sandbox path prefix to deliver.
        :returns: Subscription that removes the handler when closed.
        :raises RuntimeError: Raised when called outside a running asyncio loop.
        :raises ValueError: Raised when a filter value is invalid.
        """
        loop = asyncio.get_running_loop()
        normalized_event_types = _normalize_event_types(event_types)
        normalized_path_prefix = _normalize_event_path_prefix(path_prefix)
        token = self._next_event_handler_token
        self._next_event_handler_token += 1
        queue: asyncio.Queue[SandboxEvent] = asyncio.Queue(
            maxsize=self._config.event_queue_size,
        )
        worker_task = loop.create_task(self._deliver_events(handler, queue))
        self._event_handlers[token] = _EventHandlerRegistration(
            handler=handler,
            event_types=normalized_event_types,
            path_prefix=normalized_path_prefix,
            queue=queue,
            worker_task=worker_task,
        )
        worker_task.add_done_callback(
            lambda completed: self._finish_event_delivery(token, completed)
        )
        self._ensure_event_dispatcher(loop)
        return EventSubscription(self, token)

    def close_event_handlers(self) -> None:
        """Remove all event handlers from the sandbox."""
        for registration in tuple(self._event_handlers.values()):
            self._stop_event_delivery(registration)
        self._event_handlers.clear()
        self._native_sandbox.set_event_notifications_enabled(False)

    def _shutdown_event_handlers(self) -> None:
        """Remove event handlers and stop the dispatcher task."""
        self.close_event_handlers()
        self._event_dispatch_generation += 1
        task = self._event_dispatch_task
        self._event_dispatch_task = None
        if task is None or task.done():
            return
        _cancel_task(task)

    def _remove_event_subscription(self, token: int) -> None:
        """:param token: Handler token to remove."""
        registration = self._event_handlers.pop(token, None)
        if registration is not None:
            self._stop_event_delivery(registration)
        if len(self._event_handlers) > 0:
            return
        self.close_event_handlers()

    def _ensure_event_dispatcher(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """:raises RuntimeError: Raised when no asyncio loop is running."""
        task = self._event_dispatch_task
        if task is not None and not task.done():
            self._native_sandbox.set_event_notifications_enabled(True)
            return
        if loop is None:
            loop = asyncio.get_running_loop()
        self._event_dispatch_generation += 1
        generation = self._event_dispatch_generation
        self._native_sandbox.clear_events_now()
        self._native_sandbox.set_event_notifications_enabled(True)
        self._event_dispatch_task = loop.create_task(self._dispatch_events(generation))

    async def _dispatch_events(self, generation: int) -> None:
        """:param generation: Dispatcher generation owned by this task."""
        try:
            while len(self._event_handlers) > 0:
                native_event = await self._native_sandbox.next_event()
                event = SandboxEvent._from_native(native_event)
                for registration in tuple(self._event_handlers.values()):
                    if _event_matches(registration, event):
                        self._queue_event_delivery(registration, event)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.error("sandbox event dispatcher failed\n%s", traceback.format_exc())
        finally:
            if self._event_dispatch_generation == generation:
                self._native_sandbox.set_event_notifications_enabled(False)

    async def _deliver_events(
        self,
        handler: FilesystemEventHandler,
        queue: asyncio.Queue[SandboxEvent],
    ) -> None:
        """:param handler: Handler to invoke.
        :param queue: Event queue for the handler.
        """
        while True:
            event = await queue.get()
            await _call_event_handler(handler, event)

    def _finish_event_delivery(
        self,
        token: int,
        task: asyncio.Task[None],
    ) -> None:
        """:param token: Handler token.
        :param task: Completed delivery task.
        """
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            self._event_handlers.pop(token, None)
            if len(self._event_handlers) == 0:
                self.close_event_handlers()
            LOGGER.error(
                "sandbox event delivery task failed\n%s",
                traceback.format_exc(),
            )

    def _stop_event_delivery(self, registration: _EventHandlerRegistration) -> None:
        """:param registration: Handler registration to stop."""
        if registration.worker_task.done():
            return
        _cancel_task(registration.worker_task)

    def _async_tasks(self) -> tuple[asyncio.Task[None], ...]:
        """:returns: Async tasks owned by this sandbox."""
        tasks: list[asyncio.Task[None]] = []
        if self._event_dispatch_task is not None:
            tasks.append(self._event_dispatch_task)
        if self._virtual_executable_dispatch_task is not None:
            tasks.append(self._virtual_executable_dispatch_task)
        tasks.extend(registration.worker_task for registration in self._event_handlers.values())
        tasks.extend(self._virtual_executable_request_tasks.values())
        return tuple(tasks)

    def _queue_event_delivery(
        self,
        registration: _EventHandlerRegistration,
        event: SandboxEvent,
    ) -> None:
        """:param registration: Handler registration to receive the event.
        :param event: Event to enqueue.
        """
        if registration.dropped_count > 0:
            dropped_event = SandboxEvent(
                sequence=event.sequence,
                kind=SandboxEventKind.EVENTS_DROPPED,
                path="/",
                dropped_count=registration.dropped_count,
            )
            if not _queue_event_nowait(registration, dropped_event):
                registration.dropped_count += 1
                return
            registration.dropped_count = 0

        if _queue_event_nowait(registration, event):
            return
        registration.dropped_count += 1

    def start(
        self,
        args: list[str] | tuple[str, ...],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        limits: Limits | None = None,
        wall_time_seconds: float | None = _DEFAULT_WALL_TIME_SECONDS_ARG,
    ) -> SandboxProcess:
        """:param args: Command and arguments.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param limits: Complete per-process resource limits.
        :param wall_time_seconds: Per-process wall-time override.
        :returns: Running process handle.
        """
        process_limits = _resolve_process_limits(
            self._config.limits,
            limits,
            wall_time_seconds,
        )
        self._ensure_virtual_executable_dispatcher()
        process_token = self._next_process_token
        self._next_process_token += 1
        try:
            native_process = self._native_sandbox.start(
                process_token,
                list(args),
                env,
                cwd,
                process_limits.output_bytes,
                process_limits.wall_time_seconds,
            )
        except RuntimeError as error:
            raise SandboxError(str(error)) from error
        return SandboxProcess(native_process)

    def spawn(
        self,
        args: list[str] | tuple[str, ...],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        limits: Limits | None = None,
        wall_time_seconds: float | None = _DEFAULT_WALL_TIME_SECONDS_ARG,
    ) -> SandboxProcess:
        """:param args: Command and arguments.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param limits: Complete per-process resource limits.
        :param wall_time_seconds: Per-process wall-time override.
        :returns: Running process handle.
        """
        return self.start(
            args,
            env=env,
            cwd=cwd,
            limits=limits,
            wall_time_seconds=wall_time_seconds,
        )

    def popen(
        self,
        args: list[str] | tuple[str, ...],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        limits: Limits | None = None,
        wall_time_seconds: float | None = _DEFAULT_WALL_TIME_SECONDS_ARG,
    ) -> SandboxProcess:
        """:param args: Command and arguments.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param limits: Complete per-process resource limits.
        :param wall_time_seconds: Per-process wall-time override.
        :returns: Running process handle.
        """
        return self.start(
            args,
            env=env,
            cwd=cwd,
            limits=limits,
            wall_time_seconds=wall_time_seconds,
        )

    async def run(
        self,
        args: list[str] | tuple[str, ...],
        *,
        input: bytes | str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        limits: Limits | None = None,
        wall_time_seconds: float | None = _DEFAULT_WALL_TIME_SECONDS_ARG,
        check: bool = False,
    ) -> CompletedProcess:
        """:param args: Command and arguments.
        :param input: Bytes or text to pass as stdin.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param limits: Complete per-process resource limits.
        :param wall_time_seconds: Per-process wall-time override.
        :param check: Whether to raise on a non-zero return code.
        :returns: Completed process details.
        :raises SandboxError: Raised when check is true and the command fails.
        """
        input_bytes = input.encode() if isinstance(input, str) else input
        process_limits = _resolve_process_limits(
            self._config.limits,
            limits,
            wall_time_seconds,
        )
        self._ensure_virtual_executable_dispatcher()
        process_token = self._next_process_token
        self._next_process_token += 1
        native_task = asyncio.ensure_future(
            self._native_sandbox.run(
                process_token,
                list(args),
                input_bytes,
                env,
                cwd,
                process_limits.output_bytes,
                process_limits.wall_time_seconds,
            )
        )
        try:
            native_result = await asyncio.shield(native_task)
        except asyncio.CancelledError:
            self._native_sandbox.cancel_process(process_token)
            try:
                await native_task
            except Exception:
                LOGGER.debug(
                    "native process cancellation cleanup failed\n%s",
                    traceback.format_exc(),
                )
            raise
        except RuntimeError as error:
            raise SandboxError(str(error)) from error
        result = CompletedProcess(
            args=tuple(native_result.args),
            returncode=native_result.returncode,
            stdout=native_result.stdout,
            stderr=native_result.stderr,
        )
        if check:
            result.check_returncode()
        return result

    async def read_file(self, path: str) -> bytes:
        """:param path: Absolute sandbox path.
        :returns: File contents.
        """
        try:
            return await self._native_sandbox.read_file(path)
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    async def check_output(
        self,
        args: list[str] | tuple[str, ...],
        *,
        input: bytes | str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        limits: Limits | None = None,
        wall_time_seconds: float | None = _DEFAULT_WALL_TIME_SECONDS_ARG,
    ) -> bytes:
        """:param args: Command and arguments.
        :param input: Bytes or text to pass as stdin.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param limits: Complete per-process resource limits.
        :param wall_time_seconds: Per-process wall-time override.
        :returns: Captured stdout bytes.
        :raises SandboxError: Raised when the command fails.
        """
        result = await self.run(
            args,
            input=input,
            env=env,
            cwd=cwd,
            limits=limits,
            wall_time_seconds=wall_time_seconds,
            check=True,
        )
        return result.stdout

    async def check_output_text(
        self,
        args: list[str] | tuple[str, ...],
        *,
        input: bytes | str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        limits: Limits | None = None,
        wall_time_seconds: float | None = _DEFAULT_WALL_TIME_SECONDS_ARG,
        encoding: str = "utf-8",
    ) -> str:
        """:param args: Command and arguments.
        :param input: Bytes or text to pass as stdin.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param limits: Complete per-process resource limits.
        :param wall_time_seconds: Per-process wall-time override.
        :param encoding: Encoding to use.
        :returns: Captured stdout text.
        :raises SandboxError: Raised when the command fails.
        """
        data = await self.check_output(
            args,
            input=input,
            env=env,
            cwd=cwd,
            limits=limits,
            wall_time_seconds=wall_time_seconds,
        )
        return data.decode(encoding)

    async def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """:param path: Absolute sandbox path.
        :param encoding: Encoding to use.
        :returns: Decoded file contents.
        """
        data = await self.read_file(path)
        return data.decode(encoding)

    async def write_file(self, path: str, data: bytes) -> None:
        """:param path: Absolute sandbox path.
        :param data: File contents.
        """
        try:
            await self._native_sandbox.write_file(path, data)
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    async def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        """:param path: Absolute sandbox path.
        :param text: Text to write.
        :param encoding: Encoding to use.
        """
        await self.write_file(path, text.encode(encoding))

    async def exists(self, path: str) -> bool:
        """:param path: Absolute sandbox path.
        :returns: Whether the path exists.
        """
        try:
            return await self._native_sandbox.exists(path)
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    async def listdir(self, path: str) -> list[str]:
        """:param path: Absolute sandbox path.
        :returns: Directory entry names.
        """
        try:
            return await self._native_sandbox.listdir(path)
        except RuntimeError as error:
            raise SandboxError(str(error)) from error


async def _dispatch_virtual_processes(
    sandbox_reference: weakref.ReferenceType[Sandbox],
) -> None:
    """:param sandbox_reference: Sandbox that owns virtual executable handlers."""
    try:
        while True:
            sandbox = sandbox_reference()
            if sandbox is None:
                return
            if len(sandbox._virtual_executable_handlers) == 0:
                return
            native_sandbox = sandbox._native_sandbox
            del sandbox

            request_id, payload = await native_sandbox.next_virtual_process()

            sandbox = sandbox_reference()
            if sandbox is None:
                return
            sandbox._start_virtual_process_request(request_id, payload)
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.error(
            "virtual executable dispatcher failed\n%s",
            traceback.format_exc(),
        )
    finally:
        sandbox = sandbox_reference()
        if sandbox is not None:
            sandbox._virtual_executable_dispatch_task = None
            sandbox._cancel_virtual_executable_requests()


async def _await_cancelled_handler(task: asyncio.Task[bytes]) -> None:
    """:param task: Handler task to wait after requesting cancellation."""
    try:
        await task
    except asyncio.CancelledError:
        return
    except Exception:
        LOGGER.error(
            "virtual executable handler cancellation failed\n%s",
            traceback.format_exc(),
        )


def _cancel_task(task: asyncio.Task[object]) -> None:
    """:param task: Task to cancel on its owning event loop."""
    if task.done():
        return
    loop = task.get_loop()
    if loop.is_closed():
        return
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        loop.call_soon_threadsafe(task.cancel)
        return
    if running_loop is loop:
        task.cancel()
        return
    loop.call_soon_threadsafe(task.cancel)


async def _drain_cancelled_tasks(tasks: Iterable[asyncio.Task[object]]) -> None:
    """:param tasks: Tasks that have been asked to shut down."""
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    current_task = asyncio.current_task()
    local_tasks = [
        task
        for task in tasks
        if not task.done() and task.get_loop() is running_loop and task is not current_task
    ]
    if len(local_tasks) == 0:
        return
    try:
        results = await asyncio.gather(*local_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        await asyncio.gather(*local_tasks, return_exceptions=True)
        raise
    for result in results:
        if isinstance(result, asyncio.CancelledError):
            continue
        if isinstance(result, Exception):
            LOGGER.error("sandbox task shutdown failed: %r", result)


def _normalize_package_command_aliases(
    aliases: Iterable[PackageCommandAliasInput],
) -> tuple[PackageCommandAlias, ...]:
    """:param aliases: Alias values to normalize.
    :returns: Normalized package command aliases.
    :raises ValueError: Raised when alias values are invalid.
    """
    normalized: list[PackageCommandAlias] = []
    for alias in aliases:
        if isinstance(alias, PackageCommandAlias):
            normalized_alias = alias
        else:
            alias_name, command = alias
            normalized_alias = PackageCommandAlias(alias_name, command)
        if normalized_alias in normalized:
            continue
        normalized.append(normalized_alias)
    return tuple(normalized)


def _validate_package_command_name(value: str, label: str) -> None:
    """:param value: Command name to validate.
    :param label: Name used in validation errors.
    :raises ValueError: Raised when the command name is invalid.
    """
    if len(value) == 0:
        raise ValueError(f"package command {label} cannot be empty")
    if "\0" in value:
        raise ValueError(f"package command {label} cannot contain NUL bytes")
    if "/" in value:
        raise ValueError(f"package command {label} cannot contain path separators")


def _validate_sha256(value: str, label: str) -> None:
    """:param value: SHA-256 digest to validate.
    :param label: Name used in validation errors.
    :raises ValueError: Raised when the digest is invalid.
    """
    if len(value) != 64:
        raise ValueError(f"{label} must contain 64 hexadecimal characters")
    if all(character in "0123456789abcdefABCDEF" for character in value):
        return
    raise ValueError(f"{label} must contain 64 hexadecimal characters")


def _normalize_virtual_executable_paths(path: str, aliases: Iterable[str]) -> list[str]:
    """:param path: Primary executable path.
    :param aliases: Additional executable paths.
    :returns: Normalized executable paths.
    :raises ValueError: Raised when a configured path is invalid.
    """
    paths = [path, *aliases]
    normalized: list[str] = []
    for item in paths:
        if "\0" in item:
            raise ValueError("virtual executable path cannot contain NUL bytes")
        if not item.startswith("/"):
            raise ValueError("virtual executable path must be absolute")
        normalized_path = _normalize_absolute_path(item)
        if normalized_path == "/":
            raise ValueError("virtual executable path cannot be the sandbox root")
        if normalized_path in normalized:
            continue
        normalized.append(normalized_path)
    return normalized


def _normalize_absolute_path(path: str) -> str:
    """:param path: Absolute path to normalize.
    :returns: Normalized absolute path.
    :raises ValueError: Raised when the path escapes the sandbox root.
    """
    components: list[str] = []
    for component in path.split("/"):
        if len(component) == 0 or component == ".":
            continue
        if component == "..":
            if len(components) == 0:
                raise ValueError("path cannot escape the sandbox root")
            components.pop()
            continue
        components.append(component)
    return "/" + "/".join(components)


def _normalize_virtual_executable_result(
    invocation: CommandInvocation,
    result: VirtualExecutableResult,
) -> CommandResult:
    """:param invocation: Invocation whose output buffers were written.
    :param result: Handler return value.
    :returns: Normalized command result.
    :raises TypeError: Raised when the handler returns an unsupported result.
    """
    if result is None:
        return CommandResult(
            returncode=0,
            stdout=invocation.stdout.data(),
            stderr=invocation.stderr.data(),
        )
    if isinstance(result, int):
        return CommandResult(
            returncode=result,
            stdout=invocation.stdout.data(),
            stderr=invocation.stderr.data(),
        )
    if isinstance(result, CompletedProcess):
        return CommandResult(
            returncode=result.returncode,
            stdout=_combine_virtual_output(invocation.stdout, result.stdout),
            stderr=_combine_virtual_output(invocation.stderr, result.stderr),
        )
    if isinstance(result, CommandResult):
        return CommandResult(
            returncode=result.returncode,
            stdout=_combine_virtual_output(invocation.stdout, result.stdout),
            stderr=_combine_virtual_output(invocation.stderr, result.stderr),
        )
    raise TypeError("virtual executable handler returned an unsupported result")


def _virtual_handler_error_result(formatted: str, limit: int) -> CommandResult:
    """:param formatted: Formatted handler traceback.
    :param limit: Maximum number of stderr bytes to emit.
    :returns: Error result for a failed virtual executable handler.
    """
    stderr = formatted.encode()
    if len(stderr) <= limit:
        return CommandResult(returncode=1, stderr=stderr)
    return CommandResult(returncode=1, stderr=stderr[:limit])


def _virtual_handler_cancelled_result() -> CommandResult:
    """:returns: Error result for a cancelled virtual executable handler."""
    return CommandResult(returncode=126, stderr=b"virtual executable request cancelled\n")


def _combine_virtual_output(stream: VirtualProcessOutput, data: bytes) -> bytes:
    """:param stream: Output stream written by the handler.
    :param data: Additional result data.
    :returns: Combined output bytes.
    :raises SandboxError: Raised when the output limit would be exceeded.
    """
    stream.write_nowait(data)
    return stream.data()


def _encode_virtual_executable_response(result: CommandResult) -> bytes:
    """:param result: Command result to encode.
    :returns: Encoded native response.
    """
    return (
        b"UXR1"
        + struct.pack("<iII", result.returncode, len(result.stdout), len(result.stderr))
        + result.stdout
        + result.stderr
    )


def _normalize_event_types(
    event_types: Iterable[SandboxEventKind | str] | None,
) -> frozenset[SandboxEventKind] | None:
    """:param event_types: Event kinds to normalize.
    :returns: Normalized event kinds.
    :raises ValueError: Raised when no event kinds are provided.
    """
    if event_types is None:
        return None
    normalized = frozenset(SandboxEventKind(event_type) for event_type in event_types)
    if len(normalized) > 0:
        return normalized
    raise ValueError("event_types must contain at least one event kind")


def _normalize_event_path_prefix(path_prefix: str | None) -> str | None:
    """:param path_prefix: Path prefix to normalize.
    :returns: Normalized path prefix.
    :raises ValueError: Raised when the path prefix is invalid.
    """
    if path_prefix is None:
        return None
    if "\0" in path_prefix:
        raise ValueError("path_prefix cannot contain NUL bytes")
    if not path_prefix.startswith("/"):
        raise ValueError("path_prefix must be absolute")
    components: list[str] = []
    for component in path_prefix.split("/"):
        if len(component) == 0 or component == ".":
            continue
        if component == "..":
            if len(components) == 0:
                raise ValueError("path_prefix cannot escape the sandbox root")
            components.pop()
            continue
        components.append(component)
    normalized = "/" + "/".join(components)
    if normalized == "/":
        return normalized
    return normalized.rstrip("/")


def _event_matches(registration: _EventHandlerRegistration, event: SandboxEvent) -> bool:
    """:param registration: Handler registration.
    :param event: Event to match.
    :returns: Whether the handler should receive the event.
    """
    if registration.event_types is not None and event.kind not in registration.event_types:
        return False
    if registration.path_prefix is None:
        return True
    if _path_matches_prefix(event.path, registration.path_prefix):
        return True
    if event.target_path is None:
        return False
    return _path_matches_prefix(event.target_path, registration.path_prefix)


def _queue_event_nowait(
    registration: _EventHandlerRegistration,
    event: SandboxEvent,
) -> bool:
    """:param registration: Handler registration.
    :param event: Event to enqueue.
    :returns: Whether the event was queued.
    """
    try:
        registration.queue.put_nowait(event)
    except asyncio.QueueFull:
        return False
    return True


def _path_matches_prefix(path: str, prefix: str) -> bool:
    """:param path: Event path.
    :param prefix: Path prefix.
    :returns: Whether path is inside prefix.
    """
    if prefix == "/":
        return True
    return path == prefix or path.startswith(prefix + "/")


async def _call_event_handler(handler: FilesystemEventHandler, event: SandboxEvent) -> None:
    """:param handler: Handler to invoke.
    :param event: Event to deliver.
    """
    try:
        result = handler(event)
        if result is None:
            return
        await result
    except Exception:
        LOGGER.error("sandbox event handler failed\n%s", traceback.format_exc())


NativePackageSpec = tuple[str, str, str, list[tuple[str, str]]]


def _prepare_image_packages(image: SandboxImage) -> list[NativePackageSpec]:
    """:param image: Sandbox package image.
    :returns: Native package specifications.
    """
    source_dir = resources.files("unix_sandbox").joinpath("assets")
    manifest = _load_asset_manifest(source_dir)
    cache_dir = _asset_cache_dir(manifest)
    cache_dir.mkdir(parents=True, exist_ok=True)

    packages: list[NativePackageSpec] = []
    for package in image.packages:
        if package.source is PackageSource.BUNDLED:
            packages.append(_prepare_bundled_package(package, source_dir, manifest, cache_dir))
            continue
        if package.source is PackageSource.LOCAL:
            packages.append(_prepare_local_package(package))
            continue
        packages.append(_prepare_url_package(package))

    return packages


def _prepare_bundled_package(
    package: WasmerPackage,
    source_dir: Traversable,
    manifest: dict[str, dict[str, str]],
    cache_dir: Path,
) -> NativePackageSpec:
    """:param package: Bundled package to prepare.
    :param source_dir: Package asset directory.
    :param manifest: Bundled asset manifest.
    :param cache_dir: Cache directory for expanded assets.
    :returns: Native package specification.
    :raises SandboxError: Raised when the bundled package does not exist.
    """
    spec = manifest.get(package.name)
    if spec is None:
        raise SandboxError(f"bundled package not found: {package.name}")

    sha256 = spec["sha256"]
    _expand_asset(source_dir, cache_dir, package.name, sha256)
    return _native_package_spec(
        package,
        cache_dir / f"{package.name}.webc",
        sha256,
    )


def _prepare_local_package(package: WasmerPackage) -> NativePackageSpec:
    """:param package: Local package to prepare.
    :returns: Native package specification.
    :raises SandboxError: Raised when the local package cannot be used.
    """
    if package.path is None:
        raise SandboxError(f"local package {package.name} requires a path")

    path = Path(package.path).expanduser()
    if not path.exists():
        raise SandboxError(f"package {package.name} path does not exist: {path}")
    if not path.is_file():
        raise SandboxError(f"package {package.name} path is not a file: {path}")

    sha256 = _hash_file(path)
    if package.sha256 is not None and sha256 != package.sha256:
        raise SandboxError(
            f"{package.name} package hash mismatch: expected {package.sha256}, got {sha256}"
        )

    return _native_package_spec(package, path.resolve(), sha256)


def _prepare_url_package(package: WasmerPackage) -> NativePackageSpec:
    """:param package: URL-backed package to prepare.
    :returns: Native package specification.
    :raises SandboxError: Raised when the downloaded package hash does not match.
    """
    if package.url is None:
        raise SandboxError(f"url package {package.name} requires a url")
    if package.sha256 is None:
        raise SandboxError(f"url package {package.name} requires sha256")

    cache_dir = _package_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{package.sha256}.webc"
    marker = cache_dir / f"{package.sha256}.webc.sha256"
    if (
        path.exists()
        and marker.exists()
        and marker.read_bytes().strip() == package.sha256.encode()
    ):
        return _native_package_spec(package, path, package.sha256)

    temporary: Path | None = None
    completed = False
    digest = hashlib.sha256()
    try:
        request = urllib.request.Request(
            package.url,
            headers={"User-Agent": "unix-wasm-sandbox package fetcher"},
        )
        with (
            urllib.request.urlopen(request, timeout=120) as response,
            tempfile.NamedTemporaryFile(
                "wb",
                dir=cache_dir,
                prefix=f"{package.name}.",
                suffix=".webc.tmp",
                delete=False,
            ) as output,
        ):
            temporary = Path(output.name)
            while True:
                chunk = response.read(1024 * 1024)
                if len(chunk) == 0:
                    break
                digest.update(chunk)
                output.write(chunk)

        actual_sha256 = digest.hexdigest()
        if actual_sha256 != package.sha256:
            raise SandboxError(
                f"{package.name} package hash mismatch: "
                f"expected {package.sha256}, got {actual_sha256}"
            )

        temporary.replace(path)
        marker.write_text(package.sha256 + "\n", encoding="utf-8")
        completed = True
    finally:
        if not completed and temporary is not None:
            temporary.unlink(missing_ok=True)

    return _native_package_spec(package, path, package.sha256)


def _native_package_spec(
    package: WasmerPackage,
    path: Path,
    sha256: str,
) -> NativePackageSpec:
    """:param package: Package configuration.
    :param path: Expanded WEBC path.
    :param sha256: Expanded WEBC SHA-256 digest.
    :returns: Native package specification.
    """
    return (
        package.name,
        str(path),
        sha256,
        [(alias.alias, alias.command) for alias in package.command_aliases],
    )


def _package_cache_dir() -> Path:
    """:returns: Cache directory for URL-backed WEBC packages."""
    cache_home = os.environ.get("XDG_CACHE_HOME")
    if cache_home is not None and len(cache_home) > 0:
        root = Path(cache_home)
    else:
        root = Path.home() / ".cache"
    return root / "unix-wasm-sandbox" / "packages"


def _hash_file(path: Path) -> str:
    """:param path: File to hash.
    :returns: SHA-256 digest for the file contents.
    """
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if len(chunk) == 0:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _load_asset_manifest(source_dir: Traversable) -> dict[str, dict[str, str]]:
    """:param source_dir: Package asset directory.
    :returns: Asset manifest.
    """
    data = json.loads(source_dir.joinpath("manifest.json").read_text())
    return {
        name: {key: str(value) for key, value in spec.items()}
        for name, spec in data.items()
    }


def _asset_cache_dir(manifest: dict[str, dict[str, str]]) -> Path:
    """:param manifest: Asset manifest.
    :returns: Cache directory for the manifest.
    """
    cache_home = os.environ.get("XDG_CACHE_HOME")
    if cache_home is not None and len(cache_home) > 0:
        root = Path(cache_home)
    else:
        root = Path.home() / ".cache"

    manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
    cache_key = hashlib.sha256(manifest_bytes).hexdigest()[:16]
    return root / "unix-wasm-sandbox" / "assets" / cache_key


def _expand_asset(
    source_dir: Traversable,
    cache_dir: Path,
    name: str,
    expected_sha256: str,
) -> None:
    """:param source_dir: Package asset directory.
    :param cache_dir: Cache directory for expanded assets.
    :param name: Asset name.
    :param expected_sha256: Expected SHA-256 digest of the expanded asset.
    :raises SandboxError: Raised when the packaged asset hash does not match.
    """
    destination = cache_dir / f"{name}.webc"
    marker = cache_dir / f"{name}.webc.sha256"
    if (
        destination.exists()
        and marker.exists()
        and marker.read_bytes().strip() == expected_sha256.encode()
    ):
        return

    digest = hashlib.sha256()
    temporary: Path | None = None
    completed = False
    try:
        with (
            source_dir.joinpath(f"{name}.webc.gz").open("rb") as compressed,
            gzip.GzipFile(fileobj=compressed) as expanded,
            tempfile.NamedTemporaryFile(
                "wb",
                dir=cache_dir,
                prefix=f"{name}.",
                suffix=".webc.tmp",
                delete=False,
            ) as output,
        ):
            temporary = Path(output.name)
            while True:
                chunk = expanded.read(1024 * 1024)
                if len(chunk) == 0:
                    break
                digest.update(chunk)
                output.write(chunk)

        actual_sha256 = digest.hexdigest()
        if actual_sha256 != expected_sha256:
            raise SandboxError(
                f"{name} asset hash mismatch: expected {expected_sha256}, got {actual_sha256}"
            )

        temporary.replace(destination)
        marker.write_text(expected_sha256 + "\n", encoding="utf-8")
        completed = True
    finally:
        if not completed and temporary is not None:
            temporary.unlink(missing_ok=True)
