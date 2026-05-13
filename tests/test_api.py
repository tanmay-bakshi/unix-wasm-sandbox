import asyncio
from pathlib import Path

import pytest
from unix_sandbox import (
    CommandInvocation,
    CommandResult,
    Directory,
    File,
    HostMount,
    Limits,
    Sandbox,
    SandboxConfig,
    SandboxError,
    SandboxEvent,
    SandboxEventKind,
    VirtualExecutable,
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
async def test_event_dispatcher_restart_keeps_notifications_enabled() -> None:
    """Verify that stale event dispatchers cannot disable newer subscriptions."""
    sandbox = Sandbox()
    first_events: list[SandboxEvent] = []
    second_events: list[SandboxEvent] = []
    subscription = sandbox.on_event(first_events.append, path_prefix="/work/first.txt")
    await asyncio.sleep(0)

    subscription.close()
    sandbox.on_event(second_events.append, path_prefix="/work/second.txt")
    await sandbox.write_text("/work/second.txt", "visible")
    await wait_for_events(second_events, 1)

    assert first_events == []
    assert [event.path for event in second_events] == ["/work/second.txt"]


@pytest.mark.asyncio
async def test_slow_event_handler_does_not_block_other_handlers() -> None:
    """Verify that event handlers are delivered independently."""
    sandbox = Sandbox()
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    fast_events: list[SandboxEvent] = []

    async def slow_handler(event: SandboxEvent) -> None:
        slow_started.set()
        await release_slow.wait()

    sandbox.on_event(slow_handler, path_prefix="/work")
    sandbox.on_event(fast_events.append, path_prefix="/work")

    await sandbox.write_text("/work/first.txt", "first")
    await asyncio.wait_for(slow_started.wait(), timeout=2.0)
    await sandbox.write_text("/work/second.txt", "second")
    await wait_for_events(fast_events, 2)
    release_slow.set()

    assert [event.path for event in fast_events] == [
        "/work/first.txt",
        "/work/second.txt",
    ]


def test_failed_event_registration_does_not_leak_handler() -> None:
    """Verify that event registration mutates state only after a loop is available."""
    sandbox = Sandbox()
    leaked_events: list[SandboxEvent] = []

    with pytest.raises(RuntimeError, match="no running event loop"):
        sandbox.on_event(leaked_events.append, path_prefix="/work/leaked.txt")

    assert len(sandbox._event_handlers) == 0


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


@pytest.mark.asyncio
async def test_direct_virtual_executable_invokes_handler() -> None:
    """Verify that direct command execution can be backed by a Python handler."""
    sandbox = Sandbox()
    calls: list[CommandInvocation] = []

    async def handler(invocation: CommandInvocation) -> CommandResult:
        calls.append(invocation)
        assert invocation.argv == ("host-tool", "arg")
        assert invocation.cwd == "/tmp"
        assert invocation.env["VALUE"] == "visible"
        assert invocation.stdin_text == "payload"
        await invocation.stdout.write("streamed stdout\n")
        await invocation.stderr.write("streamed stderr\n")
        await invocation.write_text("/work/virtual.txt", "created by handler")
        return CommandResult(returncode=7, stdout=b"returned stdout\n", stderr=b"returned stderr\n")

    sandbox.register_executable(
        "/usr/bin/host-tool",
        handler,
        aliases=("/bin/host-tool",),
    )

    result = await sandbox.run(
        ["host-tool", "arg"],
        input="payload",
        env={"VALUE": "visible"},
        cwd="/tmp",
    )

    assert result.returncode == 7
    assert result.stdout_text == "streamed stdout\nreturned stdout\n"
    assert result.stderr_text == "streamed stderr\nreturned stderr\n"
    assert await sandbox.read_text("/work/virtual.txt") == "created by handler"
    assert len(calls) == 1
    assert calls[0].executable_path in {"/bin/host-tool", "/usr/bin/host-tool"}


@pytest.mark.asyncio
async def test_virtual_executable_is_visible_to_shell_and_executes_as_wasm() -> None:
    """Verify that a virtual executable behaves like an executable file in the sandbox."""
    sandbox = Sandbox()
    seen: list[tuple[str, tuple[str, ...]]] = []

    async def handler(invocation: CommandInvocation) -> CommandResult:
        seen.append((invocation.executable_path, invocation.argv))
        await invocation.write_text("/work/from-host-tool.txt", "side effect")
        return CommandResult(returncode=9, stdout=b"handler stdout\n", stderr=b"handler stderr\n")

    sandbox.register_executable(
        "/usr/bin/host-tool",
        handler,
        aliases=("/bin/host-tool",),
    )

    result = await sandbox.run(
        [
            "bash",
            "-lc",
            (
                "command -v host-tool\n"
                "test -x /usr/bin/host-tool\n"
                "head -c 4 /usr/bin/host-tool | od -An -tx1\n"
                "host-tool shell\n"
                "printf 'rc:%s\\n' \"$?\"\n"
                "cat /work/from-host-tool.txt\n"
            ),
        ],
    )

    assert result.returncode == 0
    assert "handler stderr\n" in result.stderr_text
    lines = result.stdout_text.splitlines()
    assert lines[0] in {"/bin/host-tool", "/usr/bin/host-tool"}
    assert lines[1].strip() == "00 61 73 6d"
    assert lines[2] == "handler stdout"
    assert lines[3] == "rc:9"
    assert lines[4] == "side effect"
    assert seen == [(lines[0], ("host-tool", "shell"))]


@pytest.mark.asyncio
async def test_virtual_executable_can_be_configured_on_sandbox_config() -> None:
    """Verify that virtual executables can be part of sandbox construction."""

    async def handler(invocation: CommandInvocation) -> CommandResult:
        return CommandResult(stdout=f"configured:{invocation.argv[1]}\n".encode())

    sandbox = Sandbox(
        SandboxConfig(
            virtual_executables=[
                VirtualExecutable(
                    "/usr/bin/config-tool",
                    handler,
                    aliases=("/bin/config-tool",),
                ),
            ],
        ),
    )

    result = await sandbox.run(["config-tool", "ok"], check=True)
    assert result.stdout_text == "configured:ok\n"


@pytest.mark.asyncio
async def test_virtual_executable_registration_close_removes_command() -> None:
    """Verify that closing a registration removes its executable paths."""
    sandbox = Sandbox()

    async def handler(invocation: CommandInvocation) -> CommandResult:
        return CommandResult(stdout=b"active\n")

    registration = sandbox.register_executable(
        "/usr/bin/closed-tool",
        handler,
        aliases=("/bin/closed-tool",),
    )

    active = await sandbox.run(["closed-tool"], check=True)
    assert active.stdout_text == "active\n"

    registration.close()
    assert registration.closed is True

    with pytest.raises(SandboxError, match="command not found"):
        await sandbox.run(["closed-tool"])


@pytest.mark.asyncio
async def test_sandbox_python_subprocess_can_spawn_virtual_executable() -> None:
    """Verify that sandboxed Python subprocesses can invoke host-backed executables."""
    sandbox = Sandbox()
    invocations: list[CommandInvocation] = []

    async def handler(invocation: CommandInvocation) -> CommandResult:
        invocations.append(invocation)
        return CommandResult(returncode=13, stdout=b"from handler\n", stderr=b"handler err\n")

    sandbox.register_executable(
        "/usr/bin/python-tool",
        handler,
        aliases=("/bin/python-tool",),
    )

    await sandbox.run(
        [
            "python",
            "-c",
            'import subprocess; subprocess.run(["echo", "warm"], capture_output=True)',
        ],
        check=True,
    )
    result = await sandbox.run(
        [
            "python",
            "-c",
            (
                "import subprocess, sys\n"
                "process = subprocess.run(['python-tool', 'child'], capture_output=True)\n"
                "print(process.returncode)\n"
                "sys.stdout.buffer.write(process.stdout)\n"
                "sys.stderr.buffer.write(process.stderr)\n"
            ),
        ],
        check=True,
    )

    assert result.stdout_text == "13\nfrom handler\n"
    assert result.stderr_text == "handler err\n"
    assert len(invocations) == 1
    assert invocations[0].argv == ("python-tool", "child")


@pytest.mark.asyncio
async def test_nested_virtual_executable_invocations_are_dispatched() -> None:
    """Verify that virtual executable handlers can invoke other virtual executables."""
    sandbox = Sandbox()

    async def inner(invocation: CommandInvocation) -> CommandResult:
        return CommandResult(stdout=f"inner:{invocation.argv[1]}\n".encode())

    async def outer(invocation: CommandInvocation) -> CommandResult:
        result = await invocation.run(["inner-tool", invocation.argv[1]], check=True)
        return CommandResult(stdout=b"outer:" + result.stdout)

    sandbox.register_executable(
        "/usr/bin/inner-tool",
        inner,
        aliases=("/bin/inner-tool",),
    )
    sandbox.register_executable(
        "/usr/bin/outer-tool",
        outer,
        aliases=("/bin/outer-tool",),
    )

    result = await sandbox.run(["outer-tool", "value"], check=True)

    assert result.stdout_text == "outer:inner:value\n"


@pytest.mark.asyncio
async def test_virtual_executable_timeout_cancels_handler_side_effects() -> None:
    """Verify that timed-out virtual executable handlers are cancelled."""
    sandbox = Sandbox(SandboxConfig(limits=Limits(wall_time_seconds=0.1)))
    started = asyncio.Event()

    async def handler(invocation: CommandInvocation) -> CommandResult:
        started.set()
        await asyncio.sleep(1.0)
        await invocation.write_text("/work/late.txt", "late")
        return CommandResult(stdout=b"late\n")

    sandbox.register_executable(
        "/usr/bin/slow-tool",
        handler,
        aliases=("/bin/slow-tool",),
    )

    with pytest.raises(SandboxError, match="wall time limit"):
        await sandbox.run(["slow-tool"])

    assert started.is_set() is True
    await asyncio.sleep(0.2)
    assert await sandbox.exists("/work/late.txt") is False


@pytest.mark.asyncio
async def test_closing_virtual_executables_completes_active_request() -> None:
    """Verify that closing virtual executables unblocks active requests."""
    sandbox = Sandbox(SandboxConfig(limits=Limits(wall_time_seconds=None)))
    started = asyncio.Event()

    async def handler(invocation: CommandInvocation) -> CommandResult:
        started.set()
        await asyncio.sleep(10.0)
        return CommandResult(stdout=b"late\n")

    sandbox.register_executable(
        "/usr/bin/slow-tool",
        handler,
        aliases=("/bin/slow-tool",),
    )
    task = asyncio.create_task(sandbox.run(["slow-tool"]))
    await asyncio.wait_for(started.wait(), timeout=2.0)

    sandbox.close_virtual_executables()
    result = await asyncio.wait_for(task, timeout=2.0)

    assert result.returncode == 126
    assert result.stderr_text == "virtual executable request cancelled\n"


@pytest.mark.asyncio
async def test_cancelled_run_stops_guest_process() -> None:
    """Verify that cancelling a run task stops the underlying guest process."""
    sandbox = Sandbox(SandboxConfig(limits=Limits(wall_time_seconds=None)))
    started = asyncio.Event()

    async def marker(invocation: CommandInvocation) -> CommandResult:
        started.set()
        return CommandResult()

    sandbox.register_executable(
        "/usr/bin/start-tool",
        marker,
        aliases=("/bin/start-tool",),
    )
    task = asyncio.create_task(
        sandbox.run(
            [
                "python",
                "-c",
                (
                    "from pathlib import Path\n"
                    "import subprocess\n"
                    "import time\n"
                    "subprocess.run(['start-tool'], check=True)\n"
                    "time.sleep(2)\n"
                    "Path('/work/late.txt').write_text('late')\n"
                ),
            ],
        ),
    )

    await asyncio.wait_for(started.wait(), timeout=60.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(2.5)

    assert await sandbox.exists("/work/late.txt") is False


@pytest.mark.asyncio
async def test_virtual_executable_handler_failures_respect_output_limit() -> None:
    """Verify that handler exceptions are reported without bypassing output limits."""
    sandbox = Sandbox(SandboxConfig(limits=Limits(output_bytes=32)))

    async def handler(invocation: CommandInvocation) -> CommandResult:
        raise RuntimeError("handler failure")

    sandbox.register_executable(
        "/usr/bin/failing-tool",
        handler,
        aliases=("/bin/failing-tool",),
    )

    result = await sandbox.run(["failing-tool"])

    assert result.returncode == 1
    assert len(result.stderr) == 32
    assert result.stderr_text.startswith("Traceback")


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
