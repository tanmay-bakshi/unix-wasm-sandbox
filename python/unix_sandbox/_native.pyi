from collections.abc import Awaitable

class CompletedProcess:
    args: list[str]
    returncode: int
    stdout: bytes
    stderr: bytes


class Sandbox:
    def __init__(
        self,
        files: dict[str, bytes | None],
        host_mounts: list[tuple[str, str, bool]],
        cwd: str,
        env: dict[str, str],
        asset_dir: str,
        output_limit: int,
        wall_time_seconds: float | None,
    ) -> None: ...

    def exists(self, path: str) -> Awaitable[bool]: ...

    def read_file(self, path: str) -> Awaitable[bytes]: ...

    def write_file(self, path: str, data: bytes) -> Awaitable[None]: ...

    def listdir(self, path: str) -> Awaitable[list[str]]: ...

    def run(
        self,
        args: list[str],
        input: bytes | None,
        env: dict[str, str] | None,
        cwd: str | None,
    ) -> Awaitable[CompletedProcess]: ...
