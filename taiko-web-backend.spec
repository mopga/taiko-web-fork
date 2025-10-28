# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from PyInstaller.building.datastruct import Tree
from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules

project_root = Path(__file__).resolve().parent
frontend_build = project_root / 'client' / 'build'

datas = []
binaries = []
hiddenimports = ['bcrypt', 'cffi', '_cffi_backend', 'desktop_config']
tmp_ret = collect_all('bcrypt')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('cffi')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
hiddenimports += collect_submodules('storage')
hiddenimports += collect_submodules('tools')
hiddenimports += collect_submodules('lock')

datas += collect_data_files('config', include_py_files=False)

if frontend_build.is_dir():
    datas += Tree(str(frontend_build), prefix='taiko_web_backend/_internal/public').toc

extra_data = [
    (Path('templates'), 'web/templates'),
    (Path('public'), 'web/static'),
    (Path('taiko_web_backend') / '_internal' / 'public', 'taiko_web_backend/_internal/public'),
    (Path('taiko-web-backend') / '_internal' / 'public', 'taiko-web-backend/_internal/public'),
]
for source_path, target in extra_data:
    if source_path.exists():
        datas.append((str(source_path), target.replace('\\', '/')))


a = Analysis(
    ['standalone\\run_desktop.py'],
    pathex=[],
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
