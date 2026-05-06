# unix-wasm-sandbox

`unix-wasm-sandbox` is a Python package, implemented in Rust with PyO3, for
running isolated UNIX-like Wasmer environments from async Python code.

The default sandbox starts with an in-memory filesystem, a working directory at
`/work`, common coreutils, Bash, text/archive utilities, gzip, and CPython
3.12.0. Commands run inside Wasmer WASIX and expose captured stdin, stdout,
stderr, return codes, working directory, and environment overrides through a
small Python API.

## Install

From this repository:

```console
uv venv
uv pip install -e ".[dev]"
```

The package contains compressed WEBC assets. On first use, those assets are
expanded into `XDG_CACHE_HOME/unix-wasm-sandbox` or `~/.cache/unix-wasm-sandbox`.

## Quickstart

```python
import asyncio

from unix_sandbox import File, Sandbox, SandboxConfig


async def main() -> None:
    sandbox = Sandbox(
        SandboxConfig(
            files={
                "/work/input.txt": File.text("hello from the sandbox\n"),
            },
        ),
    )

    result = await sandbox.run(["cat", "/work/input.txt"], check=True)
    print(result.stdout_text)

    python = await sandbox.run(
        ["python", "-c", "import os; print(os.getcwd()); print(6 * 7)"],
        check=True,
    )
    print(python.stdout_text)


asyncio.run(main())
```

## API Shape

`SandboxConfig` controls the initial filesystem, default working directory,
default environment, and resource limits.

```python
from unix_sandbox import Directory, File, Limits, SandboxConfig

config = SandboxConfig(
    files={
        "/work/src": Directory(),
        "/work/src/main.py": File.text("print('ok')\n"),
    },
    cwd="/work",
    env={"LANG": "C.UTF-8"},
    limits=Limits(
        output_bytes=16 * 1024 * 1024,
        wall_time_seconds=10.0,
    ),
)
```

`Sandbox.run()` is async and does not block the Python event loop while the
Wasmer process executes.

```python
sandbox = Sandbox(config)
result = await sandbox.run(
    ["python", "/work/src/main.py"],
    input=b"",
    env={"EXTRA": "1"},
    cwd="/work",
    check=True,
)
```

Captured stdout and stderr are capped while the process writes, so a process
cannot fill host memory before `Limits.output_bytes` is enforced.

`CompletedProcess` mirrors the useful parts of `subprocess.CompletedProcess`:

- `args`
- `returncode`
- `stdout` and `stderr`
- `stdout_text` and `stderr_text`
- `check_returncode()`

Direct filesystem helpers are also async:

```python
await sandbox.write_text("/work/generated.txt", "data")
text = await sandbox.read_text("/work/generated.txt")
names = await sandbox.listdir("/work")
exists = await sandbox.exists("/work/generated.txt")
```

For the common "raise on failure and return stdout" shape:

```python
stdout = await sandbox.check_output(["cat", "/work/generated.txt"])
text = await sandbox.check_output_text(["python", "-c", "print('ok')"])
```

Commands can be invoked by name through the sandbox `PATH`, or through the
standard `/bin/<name>` and `/usr/bin/<name>` mappings. Path-like executable
arguments are not reduced to basenames, so `/no/such/path/cat` is treated as a
missing executable instead of resolving to `cat`.

## Standard Image

The bundled standard image is pinned and hash-verified:

- `wasmer/coreutils@1.0.19`
- `wasmer/bash@1.0.25`
- `wasmer/grep@3.12.0`
- `wasmer/sed@4.9.0`
- `wasmer/find@4.10.0`
- `wasmer/tar@1.35.0`
- `wasmer/gzip@1.14.0`
- `python/python@0.2.0`, CPython 3.12.0

Regenerate the compressed assets with:

```console
uv run python scripts/fetch_assets.py
```

Asset regeneration requires the Wasmer CLI because the bundled Bash package is
rebuilt after removing registry dependency metadata; the runtime injects the
standard utility packages itself.

## Development

This repository pins the Rust toolchain in `rust-toolchain.toml`.

Run the local checks:

```console
cargo fmt --check
cargo check
cargo test
uv run ruff check .
uv run mypy
uv run pytest
```

## License

Apache-2.0
