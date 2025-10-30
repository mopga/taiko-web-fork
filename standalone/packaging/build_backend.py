"""Build the Taiko Web backend binary for desktop distribution."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


APP_NAME = "taiko-web-backend"


def build_backend() -> None:
    repo_root = Path(__file__).resolve().parents[1].parent
    spec = repo_root / "taiko-web-backend.spec"
    dist = repo_root / "standalone" / "dist" / "backend"
    build = repo_root / "standalone" / "dist" / "build-backend"

    dist.mkdir(parents=True, exist_ok=True)
    build.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        str(spec),
        "--noconfirm",
        "--clean",
        "--distpath",
        str(dist),
        "--workpath",
        str(build),
    ]
    print("[build_backend]", " ".join(cmd))
    subprocess.check_call(cmd, cwd=str(repo_root))

    app_dir = dist / APP_NAME
    if not app_dir.exists():
        raise FileNotFoundError(f"PyInstaller did not create bundle at {app_dir}")

    binary_name = "taiko-web-backend.exe" if sys.platform.startswith("win") else "taiko-web-backend"
    binary_path = app_dir / binary_name
    if not binary_path.exists():
        raise FileNotFoundError(f"Expected backend binary not found at {binary_path}")

    songs_dir = app_dir / "songs"
    songs_dir.mkdir(parents=True, exist_ok=True)

    internal_dir = app_dir / "_internal"
    for directory_name in ("public", "templates"):
        source = internal_dir / directory_name
        if not source.exists():
            continue
        destination = app_dir / directory_name
        shutil.copytree(source, destination, dirs_exist_ok=True)

    print(f"[build_backend] Built at: {app_dir}")


def main() -> None:  # pragma: no cover - exercised in CI
    build_backend()


if __name__ == "__main__":
    main()
