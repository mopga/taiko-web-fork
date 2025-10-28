import os
import sys
import subprocess
import shutil
import platform
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENTRY = ROOT / "standalone" / "run_desktop.py"
DIST = ROOT / "standalone" / "dist" / "backend"
NAME = "taiko-web-backend"
FRONTEND_BUILD = ROOT / "client" / "build"

DATA_DIRS = [
    ("web/templates", "web/templates"),
    ("web/static", "web/static"),
    ("client/build", "taiko_web_backend/_internal/public"),
]


def add_data_arg(src_rel, dst_rel):
    sep = ";" if platform.system() == "Windows" else ":"
    return f"--add-data={src_rel}{sep}{dst_rel}"


def main():
    os.chdir(ROOT)
    if not FRONTEND_BUILD.is_dir() or not (FRONTEND_BUILD / "index.html").is_file():
        raise RuntimeError(
            f"Frontend build not found at {FRONTEND_BUILD}. Did you run the build step?"
        )
    args = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        NAME,
        "--onefile",
        "--hidden-import=bcrypt",
        "--hidden-import=cffi",
        "--hidden-import=_cffi_backend",
        "--collect-all=bcrypt",
        "--collect-all=cffi",
        str(ENTRY),
    ]
    if platform.system() == "Windows":
        args.append("--noconsole")

    for s, d in DATA_DIRS:
        if (ROOT / s).exists():
            args.append(add_data_arg(s, d))

    print("Running:", " ".join(args))
    subprocess.check_call(args)

    if DIST.exists():
        shutil.rmtree(DIST)
    DIST.mkdir(parents=True, exist_ok=True)
    if platform.system() == "Windows":
        shutil.copyfile(ROOT / "dist" / f"{NAME}.exe", DIST / f"{NAME}.exe")
    else:
        dst = DIST / NAME
        shutil.copyfile(ROOT / "dist" / NAME, dst)
        dst.chmod(0o755)


if __name__ == "__main__":
    main()
