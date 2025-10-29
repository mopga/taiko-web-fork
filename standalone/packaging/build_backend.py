"""Build the Taiko Web backend binary for desktop distribution."""

from __future__ import annotations

import os
import platform
import shutil
import sys
from pathlib import Path

import PyInstaller.__main__

ROOT = Path(__file__).resolve().parents[2]
ENTRY = ROOT / "standalone" / "run_desktop.py"
DIST_DIR = ROOT / "standalone" / "dist" / "backend"
BUILD_DIR = ROOT / "standalone" / "dist" / "build-backend"
NAME = "taiko-web-backend"


def _add_data_arg(src: Path, target: str) -> str:
    separator = ";" if platform.system() == "Windows" else ":"
    return f"{src}{separator}{target}"


def _collect_additional_data() -> list[str]:
    data_args: list[str] = []
    public_dir = ROOT / "public"
    if not public_dir.exists():
        raise RuntimeError(f"Frontend assets are missing at {public_dir}")
    data_args.append(_add_data_arg(public_dir, "public"))

    templates_dir = ROOT / "templates"
    if templates_dir.exists():
        data_args.append(_add_data_arg(templates_dir, "templates"))

    return data_args


def _pyinstaller_args() -> list[str]:
    args: list[str] = [
        str(ENTRY),
        "--name",
        NAME,
        "--onedir",
        "--clean",
        "--noconfirm",
        "--distpath",
        str(DIST_DIR),
        "--workpath",
        str(BUILD_DIR),
        "--specpath",
        str(BUILD_DIR),
        "--hidden-import",
        "bcrypt",
        "--hidden-import",
        "cffi",
        "--hidden-import",
        "_cffi_backend",
        "--collect-all",
        "bcrypt",
        "--collect-all",
        "cffi",
    ]

    if platform.system() == "Windows":
        args.append("--noconsole")

    for add_data in _collect_additional_data():
        args.extend(["--add-data", add_data])

    return args


def _ensure_clean_dirs() -> None:
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_songs_dir(bundle_root: Path) -> None:
    songs = bundle_root / "songs"
    songs.mkdir(parents=True, exist_ok=True)


def _validate_binaries(bundle_root: Path) -> None:
    binary = bundle_root / ("taiko-web-backend.exe" if os.name == "nt" else "taiko-web-backend")
    if not binary.exists():
        tree = "\n".join(str(p) for p in bundle_root.rglob("*"))
        raise FileNotFoundError(
            f"Expected backend binary not found at {binary}. Current bundle contents:\n{tree}"
        )


def build_backend() -> None:
    _ensure_clean_dirs()

    pyinstaller_args = _pyinstaller_args()
    print("[build_backend] Invoking PyInstaller with:")
    for part in pyinstaller_args:
        print("  ", part)

    PyInstaller.__main__.run(pyinstaller_args)

    bundle_root = DIST_DIR / NAME
    if not bundle_root.exists():
        raise RuntimeError(f"PyInstaller did not create bundle at {bundle_root}")

    _ensure_songs_dir(bundle_root)
    _validate_binaries(bundle_root)
    print(f"[build_backend] Done. Binaries at: {bundle_root}")


def main() -> None:  # pragma: no cover - exercised in CI
    build_backend()


if __name__ == "__main__":
    main()
