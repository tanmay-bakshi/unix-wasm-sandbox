"""Fetch pinned Wasmer WEBC assets for the standard sandbox image."""

import gzip
import hashlib
import json
import urllib.request
from pathlib import Path

ASSETS: dict[str, dict[str, str]] = {
    "coreutils": {
        "package": "sharrattj/coreutils",
        "version": "1.0.16",
        "sha256": "59b01ca057218b8ab51cab83546d22b729e015d6cf519b2383cc68bce67ef750",
        "url": "https://cdn.wasmer.io/webcimages/59b01ca057218b8ab51cab83546d22b729e015d6cf519b2383cc68bce67ef750.webc",
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

    raw_destination = asset_dir / f"{name}.webc"
    if raw_destination.exists():
        data = raw_destination.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if digest == spec["sha256"]:
            destination.write_bytes(gzip.compress(data, compresslevel=9, mtime=0))
            return

    request = urllib.request.Request(
        spec["url"],
        headers={"User-Agent": "unix-wasm-sandbox asset fetcher"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        data = response.read()

    digest = hashlib.sha256(data).hexdigest()
    if digest != spec["sha256"]:
        raise RuntimeError(f"{name} hash mismatch: expected {spec['sha256']}, got {digest}")

    destination.write_bytes(gzip.compress(data, compresslevel=9, mtime=0))


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
