"""Build the Taiko Web backend binary for desktop distribution."""
from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRY = ROOT / "standalone" / "run_desktop.py"
DEFAULT_OUTDIR = ROOT / "standalone" / "dist" / "backend"
NAME = "taiko-web-backend"

DATA_DIRS = [
    ("web/templates", "web/templates"),
    ("web/static", "web/static"),
    ("public", "public"),
    ("songs", "songs"),
]


def add_data_arg(src_rel: str, dst_rel: str) -> str:
    separator = ";" if platform.system() == "Windows" else ":"
    return f"--add-data={src_rel}{separator}{dst_rel}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the desktop backend distribution")
    parser.add_argument("--profile", default="desktop", help="Runtime profile to embed (default: desktop)")
    parser.add_argument(
        "--outdir",
        type=Path,
        default=DEFAULT_OUTDIR,
        help="Output directory for the staged backend",
    )
    return parser.parse_args(argv)


def ensure_support_directories() -> None:
    songs_dir = ROOT / "songs"
    songs_dir.mkdir(parents=True, exist_ok=True)
    public_dir = ROOT / "public"
    if not public_dir.exists():
        raise RuntimeError(f"Frontend assets are missing at {public_dir}")


def build_backend(*, profile: str, outdir: Path) -> None:
    ensure_support_directories()
    os.chdir(ROOT)

    args = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        NAME,
        "--onedir",
        "--hidden-import=bcrypt",
        "--hidden-import=cffi",
        "--hidden-import=_cffi_backend",
        "--collect-all=bcrypt",
        "--collect-all=cffi",
        str(ENTRY),
    ]

    for src_rel, dst_rel in DATA_DIRS:
        src_path = ROOT / src_rel
        if src_path.exists():
            args.append(add_data_arg(src_rel, dst_rel))

    if platform.system() == "Windows":
        args.append("--noconsole")

    print("Running:", " ".join(args))
    subprocess.check_call(args)

    source_dir = ROOT / "dist" / NAME
    if not source_dir.is_dir():
        raise RuntimeError(f"PyInstaller output missing at {source_dir}")

    target_dir = outdir / NAME
    if target_dir.exists():
        shutil.rmtree(target_dir)
    else:
        target_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, target_dir)
    songs_target = target_dir / "songs"
    songs_target.mkdir(parents=True, exist_ok=True)
    print(f"Backend staged at {target_dir}")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    build_backend(profile=args.profile, outdir=args.outdir)


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
