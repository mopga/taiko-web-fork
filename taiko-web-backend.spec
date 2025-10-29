# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path

from PyInstaller.building.datastruct import Tree
from PyInstaller.utils.hooks import collect_all, collect_submodules

project_root = Path(os.getcwd()).resolve()
public_dir = project_root / "public"
templates_dir = project_root / "templates"

datas: list = []
binaries = []
hiddenimports = ['bcrypt', 'cffi', '_cffi_backend', 'desktop_config']

if not public_dir.is_dir():
    raise FileNotFoundError(f"Frontend assets are missing at {public_dir}")

tmp_ret = collect_all('bcrypt')
binaries += tmp_ret[1]
hiddenimports += tmp_ret[2]
tmp_ret = collect_all('cffi')
binaries += tmp_ret[1]
hiddenimports += tmp_ret[2]
hiddenimports += collect_submodules('storage')
hiddenimports += collect_submodules('tools')
hiddenimports += collect_submodules('lock')

datas += Tree(str(public_dir), prefix='public')
if templates_dir.is_dir():
    datas += Tree(str(templates_dir), prefix='templates')

a = Analysis(
    [str(project_root / 'standalone' / 'run_desktop.py')],
    pathex=[str(project_root)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='taiko-web-backend',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='taiko-web-backend',
)
