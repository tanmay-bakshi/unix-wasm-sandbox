"""Fetch pinned Wasmer WEBC assets for the standard sandbox image."""

import gzip
import hashlib
import json
import subprocess
import tempfile
import urllib.request
from pathlib import Path

ASSETS: dict[str, dict[str, str]] = {
    "coreutils": {
        "package": "wasmer/coreutils",
        "version": "1.0.19",
        "sha256": "36ea48f185ca15fe8454b1defb6a11754659dbed6330549662b62874d509f95f",
        "url": "https://cdn.wasmer.io/webcimages/36ea48f185ca15fe8454b1defb6a11754659dbed6330549662b62874d509f95f.webc",
    },
    "bash": {
        "package": "wasmer/bash",
        "version": "1.0.25",
        "source_sha256": "059606d132e2e6bc1afe3b432ee64dcb1b1b059815c8bb213cf3b24798ef21e1",
        "sha256": "830f33d4ac880934d42348f2161a04d376ad0c237208e2ca53a73ad51b269970",
        "url": "https://cdn.wasmer.io/webcimages/059606d132e2e6bc1afe3b432ee64dcb1b1b059815c8bb213cf3b24798ef21e1.webc",
        "patch": "remove-dependencies",
    },
    "grep": {
        "package": "wasmer/grep",
        "version": "3.12.0",
        "sha256": "42a2dd5452990c94a51036cfb5eb9574899beccb5ce8f83f75995f7ac5e0e1ca",
        "url": "https://cdn.wasmer.io/webcimages/42a2dd5452990c94a51036cfb5eb9574899beccb5ce8f83f75995f7ac5e0e1ca.webc",
    },
    "sed": {
        "package": "wasmer/sed",
        "version": "4.9.0",
        "sha256": "3fc12256be87f6b8b7810d68d642359a6220f63b39a2ea6ef7a2bb6d79ec1393",
        "url": "https://cdn.wasmer.io/webcimages/3fc12256be87f6b8b7810d68d642359a6220f63b39a2ea6ef7a2bb6d79ec1393.webc",
    },
    "find": {
        "package": "wasmer/find",
        "version": "4.10.0",
        "sha256": "89ce490149de27351f18f467f990d3dd8e6ba70be83a819b10361c7b39e4f2fa",
        "url": "https://cdn.wasmer.io/webcimages/89ce490149de27351f18f467f990d3dd8e6ba70be83a819b10361c7b39e4f2fa.webc",
    },
    "tar": {
        "package": "wasmer/tar",
        "version": "1.35.0",
        "sha256": "8e53221cd089dfd90e95ed96d2604db01d8d5fba9a9bda1752db27a4b65f5cc7",
        "url": "https://cdn.wasmer.io/webcimages/8e53221cd089dfd90e95ed96d2604db01d8d5fba9a9bda1752db27a4b65f5cc7.webc",
    },
    "gzip": {
        "package": "wasmer/gzip",
        "version": "1.14.0",
        "sha256": "ebf0160b02f9ef7f586f6c06fc6d36824c9da6467d3e2fa29dbc84df7bbaf5d5",
        "url": "https://cdn.wasmer.io/webcimages/ebf0160b02f9ef7f586f6c06fc6d36824c9da6467d3e2fa29dbc84df7bbaf5d5.webc",
    },
    "python": {
        "package": "python/python",
        "version": "0.2.0",
        "sha256": "47ff83d2d205df14e7f057a1f0a1c1da70c565d2e32c052f2970a150f5a9b407",
        "url": "https://cdn.wasmer.io/webcimages/47ff83d2d205df14e7f057a1f0a1c1da70c565d2e32c052f2970a150f5a9b407.webc",
    },
}


def fetch_asset(name: str, spec: dict[str, str], asset_dir: Path) -> None:
    """:param name: Asset name.
    :param spec: Asset metadata.
    :param asset_dir: Directory where the asset should be written.
    :raises RuntimeError: Raised when the downloaded asset hash does not match.
    """
    destination = asset_dir / f"{name}.webc.gz"
    if destination.exists():
        digest = hashlib.sha256(gzip.decompress(destination.read_bytes())).hexdigest()
        if digest == spec["sha256"]:
            return

    data = fetch_raw_asset(name, spec, asset_dir)
    digest = hashlib.sha256(data).hexdigest()
    if digest != spec["sha256"] and spec.get("patch") == "remove-dependencies":
        data = remove_package_dependencies(data)

    digest = hashlib.sha256(data).hexdigest()
    if digest != spec["sha256"]:
        raise RuntimeError(f"{name} hash mismatch: expected {spec['sha256']}, got {digest}")

    destination.write_bytes(gzip.compress(data, compresslevel=9, mtime=0))


def fetch_raw_asset(name: str, spec: dict[str, str], asset_dir: Path) -> bytes:
    """:param name: Asset name.
    :param spec: Asset metadata.
    :param asset_dir: Directory where raw local assets may be found.
    :returns: Raw source WEBC bytes.
    :raises RuntimeError: Raised when the source asset hash does not match.
    """
    expected_sha256 = spec.get("source_sha256", spec["sha256"])
    raw_destination = asset_dir / f"{name}.webc"
    if raw_destination.exists():
        data = raw_destination.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if digest == expected_sha256:
            return data
        if digest == spec["sha256"]:
            return data

    request = urllib.request.Request(
        spec["url"],
        headers={"User-Agent": "unix-wasm-sandbox asset fetcher"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        data = response.read()

    digest = hashlib.sha256(data).hexdigest()
    if digest != expected_sha256:
        raise RuntimeError(
            f"{name} source hash mismatch: expected {expected_sha256}, got {digest}"
        )
    return data


def remove_package_dependencies(data: bytes) -> bytes:
    """:param data: Source WEBC bytes.
    :returns: WEBC bytes rebuilt without registry dependency metadata.
    """
    with tempfile.TemporaryDirectory() as temporary_name:
        temporary = Path(temporary_name)
        source = temporary / "source.webc"
        unpacked = temporary / "unpacked"
        patched = temporary / "patched.webc"
        source.write_bytes(data)

        subprocess.run(
            ["wasmer", "package", "unpack", str(source), "--out-dir", str(unpacked)],
            check=True,
            capture_output=True,
            text=True,
        )
        manifest = unpacked / "wasmer.toml"
        lines = manifest.read_text(encoding="utf-8").splitlines()
        filtered_lines: list[str] = []
        skip_dependencies = False
        for line in lines:
            if line == "[dependencies]":
                skip_dependencies = True
                continue
            if skip_dependencies and line.startswith("[") and line != "[dependencies]":
                skip_dependencies = False
            if skip_dependencies:
                continue
            filtered_lines.append(line)
        manifest.write_text("\n".join(filtered_lines).strip() + "\n", encoding="utf-8")

        subprocess.run(
            ["wasmer", "package", "build", str(unpacked), "--out", str(patched)],
            check=True,
            capture_output=True,
            text=True,
        )
        return patched.read_bytes()


def main() -> None:
    """:returns: Nothing."""
    root = Path(__file__).resolve().parents[1]
    asset_dir = root / "python" / "unix_sandbox" / "assets"
    asset_dir.mkdir(parents=True, exist_ok=True)

    for name, spec in ASSETS.items():
        fetch_asset(name, spec, asset_dir)

    manifest = {
        name: {
            key: value
            for key, value in spec.items()
            if key in {"package", "version", "sha256"}
        }
        for name, spec in ASSETS.items()
    }
    (asset_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
