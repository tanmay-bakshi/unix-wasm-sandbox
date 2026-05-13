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
        event_queue_size: int,
    ) -> None: ...

    def set_event_notifications_enabled(self, enabled: bool) -> None: ...

    def clear_events_now(self) -> None: ...

    def clear_events(self) -> Awaitable[None]: ...

    def next_event(self) -> Awaitable[tuple[int, str, str, str | None, int]]: ...

    def register_virtual_executable(
        self,
        token: int,
        paths: list[str],
        replace: bool,
    ) -> None: ...

    def unregister_virtual_executable(self, token: int) -> None: ...

    def next_virtual_process(self) -> Awaitable[tuple[int, bytes]]: ...

    def complete_virtual_process(self, id: int, response: bytes) -> None: ...

    def wait_virtual_process_cancelled(self, id: int) -> Awaitable[None]: ...

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
