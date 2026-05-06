import asyncio
from pathlib import Path

import pytest
from unix_sandbox import (
    Directory,
    File,
    HostMount,
    Limits,
    Sandbox,
    SandboxConfig,
    SandboxError,
    SandboxEvent,
    SandboxEventKind,
)


async def wait_for_events(events: list[SandboxEvent], count: int) -> None:
    """Wait until the expected number of events is delivered.

    :param events: Event list populated by a handler.
    :param count: Expected minimum event count.
    """
    for _ in range(100):
        if len(events) >= count:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"expected at least {count} events, got {events!r}")


@pytest.mark.asyncio
async def test_sandbox_context_manager() -> None:
    """Verify that the sandbox can be created from the Python facade."""
    async with Sandbox() as sandbox:
        assert await sandbox.exists("/work") is True


@pytest.mark.asyncio
async def test_coreutils_process_reads_sandbox_files() -> None:
    """Verify that coreutils commands run against the sandbox filesystem."""
    sandbox = Sandbox(SandboxConfig(files={"/work/input.txt": File.text("abc")}))
    result = await sandbox.run(["cat", "/work/input.txt"], check=True)
    assert result.stdout == b"abc"
    assert result.stderr == b""


@pytest.mark.asyncio
async def test_python_process_runs_in_standard_image() -> None:
    """Verify that the standard image includes a usable Python interpreter."""
    sandbox = Sandbox()
    result = await sandbox.run(["python", "-c", "print(6 * 7)"], check=True)
    assert result.stdout_text == "42\n"


@pytest.mark.asyncio
async def test_standard_utility_processes_run() -> None:
    """Verify that the standard image includes common UNIX utilities."""
    sandbox = Sandbox(
        SandboxConfig(
            files={
                "/work/data/first.txt": File.text("alpha\nneedle\n"),
                "/work/data/second.txt": File.text("beta\n"),
            },
        ),
    )

    grep = await sandbox.run(["grep", "needle", "/work/data/first.txt"], check=True)
    assert grep.stdout_text == "needle\n"

    sed = await sandbox.run(["sed", "s/needle/thread/", "/work/data/first.txt"], check=True)
    assert sed.stdout_text == "alpha\nthread\n"

    find = await sandbox.run(["find", "/work/data", "-name", "*.txt"], check=True)
    assert find.stdout_text.splitlines() == ["/work/data/first.txt", "/work/data/second.txt"]

    await sandbox.run(["tar", "-cf", "/work/archive.tar", "-C", "/work", "data"], check=True)
    tar = await sandbox.run(["tar", "-tf", "/work/archive.tar"], check=True)
    assert "data/first.txt" in tar.stdout_text

    gzip = await sandbox.run(["gzip", "-c", "/work/data/first.txt"], check=True)
    assert len(gzip.stdout) > 0


@pytest.mark.asyncio
async def test_shell_can_run_standard_pipeline() -> None:
    """Verify that the shell can discover injected utilities through PATH."""
    sandbox = Sandbox()
    result = await sandbox.run(
        ["bash", "-lc", "printf 'alpha\\nbeta\\n' | grep beta | sed 's/beta/BETA/'"],
        check=True,
    )
    assert result.stdout_text == "BETA\n"


@pytest.mark.asyncio
async def test_dev_null_behaves_like_standard_null_device() -> None:
    """Verify that /dev/null exists and discards redirected output."""
    sandbox = Sandbox()
    result = await sandbox.run(
        ["bash", "-lc", "{ printf hidden >&2; printf visible; } 2>/dev/null; cat /dev/null"],
        check=True,
    )
    assert result.stdout_text == "visible"
    assert result.stderr == b""


@pytest.mark.asyncio
async def test_executable_paths_do_not_collapse_to_basenames() -> None:
    """Verify that path-like commands require a mapped executable path."""
    sandbox = Sandbox(SandboxConfig(files={"/work/input.txt": File.text("abc")}))

    with pytest.raises(SandboxError, match="command not found"):
        await sandbox.run(["/no/such/path/cat", "/work/input.txt"])

    result = await sandbox.run(["/bin/cat", "/work/input.txt"], check=True)
    assert result.stdout_text == "abc"


@pytest.mark.asyncio
async def test_direct_command_lookup_respects_path() -> None:
    """Verify that bare command names are resolved through PATH entries."""
    sandbox = Sandbox()

    with pytest.raises(SandboxError, match="command not found"):
        await sandbox.run(["cat"], input="abc", env={"PATH": "/not/bin"})

    result = await sandbox.run(["cat"], input="abc", env={"PATH": "/usr/bin"}, check=True)
    assert result.stdout_text == "abc"


@pytest.mark.asyncio
async def test_process_stdin_stdout_and_returncode() -> None:
    """Verify stdin capture and non-zero return code handling."""
    sandbox = Sandbox()
    cat = await sandbox.run(["cat"], input="hello", check=True)
    assert cat.stdout_text == "hello"
    assert await sandbox.check_output_text(["cat"], input="hello") == "hello"

    failed = await sandbox.run(["false"])
    assert failed.returncode == 1
    with pytest.raises(SandboxError):
        failed.check_returncode()


@pytest.mark.asyncio
async def test_python_process_receives_environment_and_cwd() -> None:
    """Verify per-process environment and working directory settings."""
    sandbox = Sandbox()
    result = await sandbox.run(
        [
            "python",
            "-c",
            "import os; print(os.getcwd()); print(os.environ['SANDBOX_VALUE'])",
        ],
        cwd="/tmp",
        env={"SANDBOX_VALUE": "visible"},
        check=True,
    )
    assert result.stdout_text == "/tmp\nvisible\n"


@pytest.mark.asyncio
async def test_process_cwd_must_exist_and_be_directory() -> None:
    """Verify that process launch rejects missing and non-directory cwd values."""
    sandbox = Sandbox(SandboxConfig(files={"/work/file.txt": File.text("x")}))

    with pytest.raises(SandboxError, match="cwd does not exist"):
        await sandbox.run(["pwd"], cwd="/work/missing")
    assert await sandbox.exists("/work/missing") is False

    with pytest.raises(SandboxError, match="cwd is not a directory"):
        await sandbox.run(["pwd"], cwd="/work/file.txt")

    with pytest.raises(SandboxError, match="cwd is not a directory"):
        await sandbox.run(["pwd"], cwd="/work/file.txt/child")

    default_missing = Sandbox(SandboxConfig(cwd="/work/missing"))
    with pytest.raises(SandboxError, match="cwd does not exist"):
        await default_missing.run(["pwd"])

    default_file = Sandbox(
        SandboxConfig(files={"/work/file.txt": File.text("x")}, cwd="/work/file.txt"),
    )
    with pytest.raises(SandboxError, match="cwd is not a directory"):
        await default_file.run(["pwd"])

    configured = Sandbox(SandboxConfig(files={"/work/subdir": Directory()}, cwd="/work/subdir"))
    result = await configured.run(["pwd"], check=True)
    assert result.stdout_text == "/work/subdir\n"


@pytest.mark.asyncio
async def test_wall_time_limit_raises_sandbox_error() -> None:
    """Verify that long-running commands are stopped by the wall-time limit."""
    sandbox = Sandbox(SandboxConfig(limits=Limits(wall_time_seconds=0.05)))
    with pytest.raises(SandboxError, match="wall time limit"):
        await sandbox.run(["python", "-c", "import time; time.sleep(1)"])


@pytest.mark.asyncio
async def test_output_limit_raises_sandbox_error() -> None:
    """Verify that oversized captured output raises a sandbox error."""
    sandbox = Sandbox(SandboxConfig(limits=Limits(output_bytes=4)))
    with pytest.raises(SandboxError, match="output exceeded"):
        await sandbox.run(["python", "-c", "print('too long')"])


@pytest.mark.asyncio
async def test_output_limit_stops_unbounded_writers() -> None:
    """Verify that unbounded writers stop at the configured output limit."""
    sandbox = Sandbox(SandboxConfig(limits=Limits(output_bytes=1024, wall_time_seconds=5.0)))
    with pytest.raises(SandboxError, match="output exceeded"):
        await sandbox.run(["yes"])


@pytest.mark.asyncio
async def test_filesystem_api_round_trip() -> None:
    """Verify direct filesystem API writes are visible to process execution."""
    sandbox = Sandbox()
    await sandbox.write_text("/work/generated/output.txt", "written")

    assert await sandbox.read_text("/work/generated/output.txt") == "written"
    assert await sandbox.listdir("/work") == ["generated"]

    result = await sandbox.run(["cat", "/work/generated/output.txt"], check=True)
    assert result.stdout_text == "written"


@pytest.mark.asyncio
async def test_direct_filesystem_writes_emit_events() -> None:
    """Verify that direct filesystem writes emit filtered event notifications."""
    sandbox = Sandbox()
    events: list[SandboxEvent] = []
    subscription = sandbox.on_event(
        events.append,
        event_types=[
            SandboxEventKind.DIRECTORY_CREATED,
            SandboxEventKind.FILE_CREATED,
            SandboxEventKind.FILE_MODIFIED,
        ],
        path_prefix="/work/events",
    )

    await sandbox.write_text("/work/events/output.txt", "created")
    await sandbox.write_text("/work/events/output.txt", "modified")
    await wait_for_events(events, 3)
    subscription.close()

    assert [event.kind for event in events] == [
        SandboxEventKind.DIRECTORY_CREATED,
        SandboxEventKind.FILE_CREATED,
        SandboxEventKind.FILE_MODIFIED,
    ]
    assert [event.path for event in events] == [
        "/work/events",
        "/work/events/output.txt",
        "/work/events/output.txt",
    ]
    assert subscription.closed is True


@pytest.mark.asyncio
async def test_process_filesystem_writes_emit_events() -> None:
    """Verify that Wasmer process writes emit filesystem notifications."""
    sandbox = Sandbox()
    events: list[SandboxEvent] = []
    sandbox.on_event(
        events.append,
        event_types=[SandboxEventKind.FILE_CREATED, SandboxEventKind.FILE_MODIFIED],
        path_prefix="/work/process.txt",
    )

    await sandbox.run(
        ["bash", "-lc", "printf created > /work/process.txt; printf modified >> /work/process.txt"],
        check=True,
    )
    await wait_for_events(events, 2)

    assert events[0].kind == SandboxEventKind.FILE_CREATED
    assert events[0].path == "/work/process.txt"
    assert SandboxEventKind.FILE_MODIFIED in {event.kind for event in events}
    assert await sandbox.read_text("/work/process.txt") == "createdmodified"


@pytest.mark.asyncio
async def test_event_subscription_close_stops_delivery() -> None:
    """Verify that closed subscriptions stop receiving filesystem events."""
    sandbox = Sandbox()
    events: list[SandboxEvent] = []
    subscription = sandbox.on_event(events.append, path_prefix="/work/closed.txt")
    subscription.close()

    await sandbox.write_text("/work/closed.txt", "hidden")
    await asyncio.sleep(0.1)

    assert events == []


@pytest.mark.asyncio
async def test_read_only_host_mount_exposes_live_host_files(tmp_path: Path) -> None:
    """Verify that read-only host mounts expose current host file contents."""
    host_file = tmp_path / "input.txt"
    host_file.write_text("alpha", encoding="utf-8")
    sandbox = Sandbox(SandboxConfig(host_mounts=[HostMount(tmp_path, "/host")]))

    assert await sandbox.read_text("/host/input.txt") == "alpha"

    host_file.write_text("beta", encoding="utf-8")
    result = await sandbox.run(["cat", "/host/input.txt"], check=True)
    assert result.stdout_text == "beta"

    with pytest.raises(SandboxError, match="permission denied"):
        await sandbox.write_text("/host/input.txt", "blocked")

    failed = await sandbox.run(["bash", "-lc", "printf blocked > /host/input.txt"])
    assert failed.returncode != 0
    assert host_file.read_text(encoding="utf-8") == "beta"


@pytest.mark.asyncio
async def test_writable_host_mount_persists_host_changes(tmp_path: Path) -> None:
    """Verify that writable host mounts persist direct and process writes."""
    sandbox = Sandbox(
        SandboxConfig(host_mounts=[HostMount(tmp_path, "/host", read_only=False)]),
    )

    await sandbox.write_text("/host/generated.txt", "created")
    assert (tmp_path / "generated.txt").read_text(encoding="utf-8") == "created"

    result = await sandbox.run(
        ["bash", "-lc", "printf updated > /host/generated.txt"],
        check=True,
    )
    assert result.stderr == b""
    assert (tmp_path / "generated.txt").read_text(encoding="utf-8") == "updated"


def test_host_mount_rejects_relative_targets(tmp_path: Path) -> None:
    """Verify that host mount targets must be absolute sandbox paths."""
    with pytest.raises(ValueError, match="absolute"):
        HostMount(tmp_path, "relative")


def test_host_mount_source_must_be_directory(tmp_path: Path) -> None:
    """Verify that sandbox construction validates host mount sources."""
    with pytest.raises(SandboxError, match="host mount source does not exist"):
        Sandbox(SandboxConfig(host_mounts=[HostMount(tmp_path / "missing", "/host")]))

    host_file = tmp_path / "file.txt"
    host_file.write_text("x", encoding="utf-8")
    with pytest.raises(SandboxError, match="host mount source is not a directory"):
        Sandbox(SandboxConfig(host_mounts=[HostMount(host_file, "/host")]))


def test_file_text_constructor() -> None:
    """Verify that text files encode predictably."""
    assert File.text("hello").data == b"hello"


def test_config_accepts_directories() -> None:
    """Verify that filesystem entries accept directory markers."""
    config = SandboxConfig(files={"/work": Directory()})
    assert isinstance(config.files["/work"], Directory)


def test_limits_reject_invalid_values() -> None:
    """Verify that resource limits fail at configuration time."""
    with pytest.raises(ValueError, match="output_bytes"):
        Limits(output_bytes=-1)

    with pytest.raises(ValueError, match="wall_time_seconds"):
        Limits(wall_time_seconds=float("inf"))


def test_config_rejects_relative_cwd() -> None:
    """Verify that sandbox construction validates the default working directory."""
    with pytest.raises(SandboxError, match="absolute"):
        Sandbox(SandboxConfig(cwd="relative"))
