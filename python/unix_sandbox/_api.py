"""Typed Python facade for the Rust sandbox runtime."""

import asyncio
import gzip
import hashlib
import json
import logging
import math
import os
import tempfile
import traceback
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from types import TracebackType
from typing import Self

from . import _native

DEFAULT_WALL_TIME_SECONDS = 30.0
DEFAULT_EVENT_QUEUE_SIZE = 4096
LOGGER = logging.getLogger(__name__)


class SandboxError(RuntimeError):
    """Error raised when a sandbox operation cannot be completed."""


@dataclass(frozen=True, slots=True)
class File:
    """A file to place in the sandbox filesystem.

    :ivar data: File contents as bytes.
    """

    data: bytes

    @classmethod
    def text(cls, text: str, encoding: str = "utf-8") -> Self:
        """:param text: Text to encode into the file.
        :param encoding: Encoding to use.
        :returns: File instance containing encoded text.
        """
        return cls(text.encode(encoding))

    @classmethod
    def bytes(cls, data: bytes) -> Self:
        """:param data: File bytes.
        :returns: File instance containing the supplied bytes.
        """
        return cls(data)


@dataclass(frozen=True, slots=True)
class Directory:
    """A directory to create in the sandbox filesystem."""


@dataclass(frozen=True, slots=True)
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


class SandboxEventKind(StrEnum):
    """Filesystem event kinds emitted by a sandbox."""

    FILE_CREATED = "file_created"
    FILE_MODIFIED = "file_modified"
    FILE_METADATA_MODIFIED = "file_metadata_modified"
    FILE_REMOVED = "file_removed"
    DIRECTORY_CREATED = "directory_created"
    DIRECTORY_REMOVED = "directory_removed"
    PATH_RENAMED = "path_renamed"
    EVENTS_DROPPED = "events_dropped"


@dataclass(frozen=True, slots=True)
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
    def _from_native(cls, event: tuple[int, str, str, str | None, int]) -> Self:
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


FilesystemEventHandler = Callable[[SandboxEvent], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class _EventHandlerRegistration:
    """A registered event handler and its delivery filter.

    :ivar handler: Handler to invoke for matching events.
    :ivar event_types: Event kinds to deliver.
    :ivar path_prefix: Sandbox path prefix to deliver.
    """

    handler: FilesystemEventHandler
    event_types: frozenset[SandboxEventKind] | None
    path_prefix: str | None


class EventSubscription:
    """A handle for removing a sandbox event handler."""

    _sandbox: "Sandbox"
    _token: int
    _closed: bool

    def __init__(self, sandbox: "Sandbox", token: int) -> None:
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


@dataclass(frozen=True, slots=True)
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


@dataclass(frozen=True, slots=True)
class SandboxConfig:
    """Configuration for a sandbox instance.

    :ivar files: Filesystem entries to create before commands run.
    :ivar host_mounts: Live host directory mounts to expose inside the sandbox.
    :ivar cwd: Default working directory.
    :ivar env: Default environment variables.
    :ivar limits: Default resource limits.
    :ivar event_queue_size: Maximum queued filesystem events before overflow.
    """

    files: dict[str, File | Directory] = field(default_factory=dict)
    host_mounts: list[HostMount] = field(default_factory=list)
    cwd: str = "/work"
    env: dict[str, str] = field(default_factory=dict)
    limits: Limits = field(default_factory=Limits)
    event_queue_size: int = DEFAULT_EVENT_QUEUE_SIZE

    def __post_init__(self) -> None:
        """:raises ValueError: Raised when an event setting is invalid."""
        if self.event_queue_size > 0:
            return
        raise ValueError("event_queue_size must be greater than zero")


@dataclass(frozen=True, slots=True)
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


class Sandbox:
    """An isolated UNIX-like Wasmer sandbox."""

    _config: SandboxConfig
    _native_sandbox: _native.Sandbox
    _event_handlers: dict[int, _EventHandlerRegistration]
    _event_dispatch_task: asyncio.Task[None] | None
    _next_event_handler_token: int

    def __init__(self, config: SandboxConfig | None = None) -> None:
        """:param config: Sandbox configuration."""
        self._config = config if config is not None else SandboxConfig()
        self._event_handlers = {}
        self._event_dispatch_task = None
        self._next_event_handler_token = 0
        files: dict[str, bytes | None] = {}
        for path, entry in self._config.files.items():
            if isinstance(entry, File):
                files[path] = entry.data
                continue
            files[path] = None

        asset_dir = _prepare_asset_dir()
        try:
            self._native_sandbox = _native.Sandbox(
                files,
                [mount._native_tuple() for mount in self._config.host_mounts],
                self._config.cwd,
                self._config.env,
                str(asset_dir),
                self._config.limits.output_bytes,
                self._config.limits.wall_time_seconds,
                self._config.event_queue_size,
            )
        except RuntimeError as error:
            raise SandboxError(str(error)) from error

    async def __aenter__(self) -> Self:
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
        self.close_event_handlers()

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
        normalized_event_types = _normalize_event_types(event_types)
        normalized_path_prefix = _normalize_event_path_prefix(path_prefix)
        token = self._next_event_handler_token
        self._next_event_handler_token += 1
        self._event_handlers[token] = _EventHandlerRegistration(
            handler=handler,
            event_types=normalized_event_types,
            path_prefix=normalized_path_prefix,
        )
        self._ensure_event_dispatcher()
        return EventSubscription(self, token)

    def close_event_handlers(self) -> None:
        """Remove all event handlers from the sandbox."""
        self._event_handlers.clear()
        self._native_sandbox.set_event_notifications_enabled(False)
        task = self._event_dispatch_task
        self._event_dispatch_task = None
        if task is None:
            return
        if task.done():
            return
        task.cancel()

    def _remove_event_subscription(self, token: int) -> None:
        """:param token: Handler token to remove."""
        self._event_handlers.pop(token, None)
        if len(self._event_handlers) > 0:
            return
        self.close_event_handlers()

    def _ensure_event_dispatcher(self) -> None:
        """:raises RuntimeError: Raised when no asyncio loop is running."""
        task = self._event_dispatch_task
        if task is not None and not task.done():
            return
        loop = asyncio.get_running_loop()
        self._native_sandbox.clear_events_now()
        self._native_sandbox.set_event_notifications_enabled(True)
        self._event_dispatch_task = loop.create_task(self._dispatch_events())

    async def _dispatch_events(self) -> None:
        """Deliver native filesystem events to registered handlers."""
        try:
            while len(self._event_handlers) > 0:
                native_event = await self._native_sandbox.next_event()
                event = SandboxEvent._from_native(native_event)
                for registration in tuple(self._event_handlers.values()):
                    if _event_matches(registration, event):
                        await _call_event_handler(registration.handler, event)
        finally:
            self._native_sandbox.set_event_notifications_enabled(False)

    async def run(
        self,
        args: list[str] | tuple[str, ...],
        *,
        input: bytes | str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        check: bool = False,
    ) -> CompletedProcess:
        """:param args: Command and arguments.
        :param input: Bytes or text to pass as stdin.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param check: Whether to raise on a non-zero return code.
        :returns: Completed process details.
        :raises SandboxError: Raised when check is true and the command fails.
        """
        input_bytes = input.encode() if isinstance(input, str) else input
        try:
            native_result = await self._native_sandbox.run(list(args), input_bytes, env, cwd)
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
    ) -> bytes:
        """:param args: Command and arguments.
        :param input: Bytes or text to pass as stdin.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :returns: Captured stdout bytes.
        :raises SandboxError: Raised when the command fails.
        """
        result = await self.run(args, input=input, env=env, cwd=cwd, check=True)
        return result.stdout

    async def check_output_text(
        self,
        args: list[str] | tuple[str, ...],
        *,
        input: bytes | str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        encoding: str = "utf-8",
    ) -> str:
        """:param args: Command and arguments.
        :param input: Bytes or text to pass as stdin.
        :param env: Environment variable overrides.
        :param cwd: Working directory override.
        :param encoding: Encoding to use.
        :returns: Captured stdout text.
        :raises SandboxError: Raised when the command fails.
        """
        data = await self.check_output(args, input=input, env=env, cwd=cwd)
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


def _prepare_asset_dir() -> Path:
    """:returns: Directory containing expanded WEBC assets."""
    source_dir = resources.files("unix_sandbox").joinpath("assets")
    manifest = _load_asset_manifest(source_dir)
    cache_dir = _asset_cache_dir(manifest)
    cache_dir.mkdir(parents=True, exist_ok=True)

    for name in manifest:
        spec = manifest[name]
        _expand_asset(source_dir, cache_dir, name, spec["sha256"])

    return cache_dir


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
