# -*- mode: python ; coding: utf-8 -*-

import inspect
import os
from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

_spec_file = os.path.abspath(inspect.getfile(inspect.currentframe()))
project_root = os.path.abspath(os.path.join(os.path.dirname(_spec_file), os.pardir))

def _data_tuple(relative_path):
    source_path = os.path.join(project_root, relative_path)
    if os.path.exists(source_path):
        return [(source_path, relative_path)]
    return []


datas = []
for folder in ("templates", "assets", "public", "standalone/static", "standalone/templates"):
    datas.extend(_data_tuple(folder))

datas.extend(collect_data_files("jinja2", includes=["**/*.py", "**/*.json", "**/*.data"]))

a = Analysis(
    [os.path.join(project_root, "standalone", "run_desktop.py")],
    pathex=[project_root],
    binaries=[],
    datas=datas,
    hiddenimports=[
        "flask",
        "jinja2",
        "websockets",
        "uvicorn",
        "waitress",
        "werkzeug",
        "markdown",
        "importlib.metadata",
        "pkg_resources",
        "cffi",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
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
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
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
    name="taiko-web-backend",
)
