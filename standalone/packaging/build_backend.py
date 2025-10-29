"""Build the Taiko Web backend binary for desktop distribution."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT.parent / "taiko-web-backend.spec"
DIST = ROOT / "dist" / "backend"
BUILD = ROOT / "dist" / "build-backend"
APP_NAME = "taiko-web-backend"


def build_backend() -> None:
    DIST.mkdir(parents=True, exist_ok=True)
    BUILD.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        str(SPEC),
        "--noconfirm",
        "--clean",
        "--distpath",
        str(DIST),
        "--workpath",
        str(BUILD),
        "--specpath",
        str(BUILD),
    ]
    print("[build_backend]", " ".join(cmd))
    subprocess.check_call(cmd)

    app_dir = DIST / APP_NAME
    if not app_dir.exists():
        raise FileNotFoundError(f"PyInstaller did not create bundle at {app_dir}")

    binary_name = "taiko-web-backend.exe" if sys.platform.startswith("win") else "taiko-web-backend"
    binary_path = app_dir / binary_name
    if not binary_path.exists():
        raise FileNotFoundError(f"Expected backend binary not found at {binary_path}")

    songs_dir = app_dir / "songs"
    songs_dir.mkdir(parents=True, exist_ok=True)

    print(f"[build_backend] Built at: {app_dir}")


def main() -> None:  # pragma: no cover - exercised in CI
    build_backend()


if __name__ == "__main__":
    main()
