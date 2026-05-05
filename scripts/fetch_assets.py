"""Fetch pinned Wasmer WEBC assets for the standard sandbox image."""

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
        "version": "3.13.5",
        "sha256": "c03ebe0946e66edf598fd7a1f192101f60e4e9c0095aecd04e049989692bdcab",
        "url": "https://cdn.wasmer.io/webcimages/c03ebe0946e66edf598fd7a1f192101f60e4e9c0095aecd04e049989692bdcab.webc",
    },
}


def fetch_asset(name: str, spec: dict[str, str], asset_dir: Path) -> None:
    """:param name: Asset name.
    :param spec: Asset metadata.
    :param asset_dir: Directory where the asset should be written.
    :raises RuntimeError: Raised when the downloaded asset hash does not match.
    """
    destination = asset_dir / f"{name}.webc"
    if destination.exists():
        digest = hashlib.sha256(destination.read_bytes()).hexdigest()
        if digest == spec["sha256"]:
            return

    with urllib.request.urlopen(spec["url"], timeout=120) as response:
        data = response.read()

    digest = hashlib.sha256(data).hexdigest()
    if digest != spec["sha256"]:
        raise RuntimeError(f"{name} hash mismatch: expected {spec['sha256']}, got {digest}")

    destination.write_bytes(data)


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
