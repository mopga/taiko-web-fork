# -*- mode: python ; coding: utf-8 -*-

import os
from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules

# Корень репозитория
project_root  = Path(os.getcwd()).resolve()
public_dir    = project_root / "public"
templates_dir = project_root / "templates"

# Если у тебя есть точные hiddenimports — добавь сюда
hiddenimports = [
    "bcrypt",
    "cffi",
    "_cffi_backend",
    "desktop_config",
]
# На всякий случай подхватываем подпакеты, если нужны
hiddenimports += collect_submodules("storage")
hiddenimports += collect_submodules("tools")
hiddenimports += collect_submodules("lock")

# ВАЖНО: Analysis(datas=...) — только пары (src, dst), никакого Tree, никаких абсолютных путей в dest
datas = [(str(public_dir), "public")]
if templates_dir.is_dir():
    datas.append((str(templates_dir), "templates"))

# Жёсткая валидация формата datas — сорвёмся раньше, чем дойдём до COLLECT
for item in datas:
    assert isinstance(item, (list, tuple)) and len(item) == 2, f"bad datas item: {item}"
    assert not os.path.isabs(item[1]), f"dest must be relative, got: {item}"

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
    []
    exclude_binaries=True, 
    name="taiko-web-backend",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

from PyInstaller.building.datastruct import Tree
extra = [Tree(str(public_dir), prefix="public")]
if templates_dir.is_dir():
    extra.append(Tree(str(templates_dir), prefix="templates"))

# COLLECT — ТОЛЬКО стандартные a.*; НИКАКИХ extra путей, НИКАКОГО distpath в spec
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    a.zipfiles,
    *extra,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="taiko-web-backend",
)
