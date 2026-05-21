import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

DARWIN_ALLOWED_PREFIXES: tuple[str, ...] = (
    "/System/Library/",
    "/usr/lib/",
    "@executable_path/",
    "@loader_path/",
    "@rpath/",
)
LINUX_ALLOWED_LIBRARIES: set[str] = {
    "ld-linux-aarch64.so.1",
    "ld-linux-x86-64.so.2",
    "libc.so.6",
    "libdl.so.2",
    "libgcc_s.so.1",
    "libm.so.6",
    "libpthread.so.0",
    "libresolv.so.2",
    "librt.so.1",
    "libstdc++.so.6",
    "libutil.so.1",
    "libz.so.1",
}


def main() -> None:
    """Verify that built wheels do not require unbundled native dependencies."""
    parser = argparse.ArgumentParser()
    parser.add_argument("wheels", nargs="+", type=Path)
    args = parser.parse_args()

    failures: list[str] = []
    for wheel in args.wheels:
        failures.extend(verify_wheel(wheel))

    if len(failures) == 0:
        return

    for failure in failures:
        print(failure, file=sys.stderr)
    raise SystemExit(1)


def verify_wheel(wheel: Path) -> list[str]:
    """:param wheel: Wheel to inspect.
    :returns: Human-readable dependency failures.
    """
    if wheel.exists() is False:
        return [f"{wheel}: file does not exist"]

    with tempfile.TemporaryDirectory() as temporary_directory:
        extract_root = Path(temporary_directory)
        with zipfile.ZipFile(wheel) as archive:
            archive.extractall(extract_root)

        libraries = find_native_libraries(extract_root)
        if len(libraries) == 0:
            return []

        if sys.platform == "darwin":
            return verify_darwin_libraries(wheel, libraries)
        if sys.platform.startswith("linux"):
            return verify_linux_libraries(wheel, extract_root, libraries)

    return [f"{wheel}: native dependency verification is unsupported on {sys.platform}"]


def find_native_libraries(root: Path) -> list[Path]:
    """:param root: Extracted wheel root.
    :returns: Native libraries found inside the wheel.
    """
    libraries: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix in {".so", ".dylib"}:
            libraries.append(path)
            continue
        if ".so." in path.name or ".dylib" in path.name:
            libraries.append(path)
    return libraries


def verify_darwin_libraries(wheel: Path, libraries: list[Path]) -> list[str]:
    """:param wheel: Wheel being inspected.
    :param libraries: Native libraries extracted from the wheel.
    :returns: Human-readable dependency failures.
    """
    failures: list[str] = []
    for library in libraries:
        install_name = read_darwin_install_name(library)
        output = subprocess.check_output(["otool", "-L", str(library)], text=True)
        dependencies = parse_otool_dependencies(output)
        for dependency in dependencies:
            if dependency == install_name:
                continue
            if dependency.startswith(DARWIN_ALLOWED_PREFIXES):
                continue
            failures.append(f"{wheel}: {library.name} requires unbundled {dependency}")
    return failures


def read_darwin_install_name(library: Path) -> str | None:
    """:param library: Native library to inspect.
    :returns: The library's own install name, if one is present.
    """
    output = subprocess.check_output(["otool", "-D", str(library)], text=True)
    lines = output.splitlines()
    if len(lines) < 2:
        return None

    install_name = lines[1].strip()
    if len(install_name) == 0:
        return None
    return install_name


def parse_otool_dependencies(output: str) -> list[str]:
    """:param output: ``otool -L`` output.
    :returns: Dependency install names reported by ``otool``.
    """
    dependencies: list[str] = []
    for line in output.splitlines()[1:]:
        stripped = line.strip()
        if len(stripped) == 0:
            continue
        dependencies.append(stripped.split(" ", 1)[0])
    return dependencies


def verify_linux_libraries(wheel: Path, root: Path, libraries: list[Path]) -> list[str]:
    """:param wheel: Wheel being inspected.
    :param root: Extracted wheel root.
    :param libraries: Native libraries extracted from the wheel.
    :returns: Human-readable dependency failures.
    """
    if shutil.which("readelf") is None:
        return [f"{wheel}: readelf is required for Linux dependency verification"]

    bundled_libraries = {library.name for library in libraries}
    failures: list[str] = []
    for library in libraries:
        output = subprocess.check_output(["readelf", "-d", str(library)], text=True)
        for dependency in parse_readelf_needed(output):
            if dependency in LINUX_ALLOWED_LIBRARIES:
                continue
            if dependency in bundled_libraries:
                continue
            failures.append(
                f"{wheel}: {library.relative_to(root)} requires unbundled {dependency}"
            )
    return failures


def parse_readelf_needed(output: str) -> list[str]:
    """:param output: ``readelf -d`` output.
    :returns: Dynamic library names from ``DT_NEEDED`` entries.
    """
    needed: list[str] = []
    pattern = re.compile(r"Shared library: \[(?P<library>[^\]]+)\]")
    for line in output.splitlines():
        match = pattern.search(line)
        if match is None:
            continue
        needed.append(match.group("library"))
    return needed


if __name__ == "__main__":
    main()
