# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules

project_root = Path(os.getcwd()).resolve()
public_dir = project_root / "public"
templates_dir = project_root / "templates"

hiddenimports = [
    "bcrypt",
    "cffi",
    "_cffi_backend",
    "desktop_config",
]
hiddenimports += collect_submodules("storage")
hiddenimports += collect_submodules("tools")
hiddenimports += collect_submodules("lock")

datas = []  # если надо, добавляй точечные пары (src, dest), НО не Tree и не dist/пути


def _collect_datas(root: Path, dest_root: str) -> None:
    if not root.is_dir():
        return
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        relative_parent = path.relative_to(root).parent
        destination = Path(dest_root) / relative_parent
        datas.append((str(path), str(destination)))


_collect_datas(public_dir, "public")
_collect_datas(templates_dir, "templates")

a = Analysis(
    [str(project_root / "standalone" / "run_desktop.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="taiko-web-backend",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="taiko-web-backend",
)
