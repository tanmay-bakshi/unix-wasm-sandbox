"""Typed Python facade for the Rust sandbox runtime."""

import gzip
import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass, field
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
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

        asset_dir = _prepare_asset_dir()
        try:
            self._native_sandbox = _native.Sandbox(
                files,
                self._config.cwd,
                self._config.env,
                str(asset_dir),
                self._config.limits.output_bytes,
                self._config.limits.wall_time_seconds,
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
