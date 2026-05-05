"""Typed Python facade for the Rust sandbox runtime."""

from dataclasses import dataclass, field
from importlib import resources
from types import TracebackType
from typing import Self

from . import _native


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
class Limits:
    """Resource limits applied to sandbox process execution.

    :ivar output_bytes: Maximum captured bytes for each output stream.
    :ivar wall_time_seconds: Maximum wall-clock time for a process.
    """

    output_bytes: int = 16 * 1024 * 1024
    wall_time_seconds: float | None = 10.0


@dataclass(frozen=True, slots=True)
class SandboxConfig:
    """Configuration for a sandbox instance.

    :ivar files: Filesystem entries to create before commands run.
    :ivar cwd: Default working directory.
    :ivar env: Default environment variables.
    :ivar limits: Default resource limits.
    """

    files: dict[str, File | Directory] = field(default_factory=dict)
    cwd: str = "/work"
    env: dict[str, str] = field(default_factory=dict)
    limits: Limits = field(default_factory=Limits)


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

    def __init__(self, config: SandboxConfig | None = None) -> None:
        """:param config: Sandbox configuration."""
        self._config = config if config is not None else SandboxConfig()
        files: dict[str, bytes | None] = {}
        for path, entry in self._config.files.items():
            if isinstance(entry, File):
                files[path] = entry.data
                continue
            files[path] = None

        asset_dir = resources.files("unix_sandbox").joinpath("assets")
        self._native_sandbox = _native.Sandbox(
            files,
            self._config.cwd,
            self._config.env,
            str(asset_dir),
            self._config.limits.output_bytes,
            self._config.limits.wall_time_seconds,
        )

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
        native_result = await self._native_sandbox.run(list(args), input_bytes, env, cwd)
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
        return await self._native_sandbox.read_file(path)

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
        await self._native_sandbox.write_file(path, data)

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
        return await self._native_sandbox.exists(path)

    async def listdir(self, path: str) -> list[str]:
        """:param path: Absolute sandbox path.
        :returns: Directory entry names.
        """
        return await self._native_sandbox.listdir(path)
