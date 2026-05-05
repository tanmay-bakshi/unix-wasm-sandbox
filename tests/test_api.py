import pytest
from unix_sandbox import Directory, File, Limits, Sandbox, SandboxConfig, SandboxError


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
async def test_filesystem_api_round_trip() -> None:
    """Verify direct filesystem API writes are visible to process execution."""
    sandbox = Sandbox()
    await sandbox.write_text("/work/generated/output.txt", "written")

    assert await sandbox.read_text("/work/generated/output.txt") == "written"
    assert await sandbox.listdir("/work") == ["generated"]

    result = await sandbox.run(["cat", "/work/generated/output.txt"], check=True)
    assert result.stdout_text == "written"


def test_file_text_constructor() -> None:
    """Verify that text files encode predictably."""
    assert File.text("hello").data == b"hello"


def test_config_accepts_directories() -> None:
    """Verify that filesystem entries accept directory markers."""
    config = SandboxConfig(files={"/work": Directory()})
    assert isinstance(config.files["/work"], Directory)
