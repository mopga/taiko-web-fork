"""Path utilities shared between web and desktop runtimes."""
from __future__ import annotations

import os
import logging
import os
import platform
import sys
from pathlib import Path
from typing import Optional

_LOGGER = logging.getLogger(__name__)
_SONGS_DIR_CACHE: Optional[Path] = None
_SONGS_DIR_LOGGED = False


def is_desktop() -> bool:
    """Return ``True`` when the desktop runtime profile is active."""

    return os.getenv("RUN_PROFILE", "").strip().lower() == "desktop"


def app_dir() -> Path:
    """Return the effective application directory for the backend."""

    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    module_path = Path(__file__).resolve()
    candidate = module_path.parents[2]
    if (candidate / "public").exists():
        return candidate
    return module_path.parents[1]


def public_dir() -> Path:
    """Return the directory that serves static frontend assets."""

    return app_dir() / "public"


def _expand_existing(path_value: Optional[str]) -> Optional[Path]:
    if not path_value:
        return None
    try:
        candidate = Path(path_value).expanduser()
    except Exception:
        return None
    try:
        if candidate.exists():
            return candidate.resolve()
    except Exception:
        return None
    return None


def songs_dir() -> Path:
    """Return the songs directory bundled with the application."""

    global _SONGS_DIR_CACHE, _SONGS_DIR_LOGGED

    if _SONGS_DIR_CACHE is not None:
        return _SONGS_DIR_CACHE

    override = _expand_existing(os.getenv("TAIKO_SONGS_DIR"))
    if override is not None:
        target = override
    else:
        app_root_env = os.getenv("TAIKO_APP_ROOT")
        app_root_path: Optional[Path] = None
        if app_root_env:
            try:
                app_root_path = Path(app_root_env).expanduser().resolve()
            except Exception:
                app_root_path = None
        candidate = app_root_path.joinpath("songs") if app_root_path else None
        if candidate is not None and candidate.exists():
            target = candidate.resolve()
        else:
            target = (app_dir() / "songs").resolve()

    _SONGS_DIR_CACHE = target
    if not _SONGS_DIR_LOGGED:
        try:
            _LOGGER.info("songs_dir=%s", target)
        except Exception:
            pass
        _SONGS_DIR_LOGGED = True
    return target


def data_dir() -> Path:
    """Return the directory used for persistent desktop data."""

    env = os.getenv("DATA_DIR")
    if env:
        return Path(env).expanduser().resolve()
    if is_desktop():
        system = platform.system()
        if system == "Windows":
            base = Path(os.getenv("APPDATA", app_dir()))
            return (base / "taiko-web-backend").resolve()
        if system == "Darwin":
            return Path.home().joinpath("Library", "Application Support", "taiko-web-backend").resolve()
        return Path.home().joinpath(".local", "share", "taiko-web-backend").resolve()
    return Path(__file__).resolve().parents[1]


def is_desktop_profile() -> bool:  # pragma: no cover - compatibility shim
    return is_desktop()


def get_app_dir() -> Path:  # pragma: no cover - compatibility shim
    return app_dir()


def get_public_dir() -> Path:  # pragma: no cover - compatibility shim
    return public_dir()


def get_songs_dir() -> Path:  # pragma: no cover - compatibility shim
    return songs_dir()


def get_songs_dir_desktop() -> Path:  # pragma: no cover - compatibility shim
    target = songs_dir()
    target.mkdir(parents=True, exist_ok=True)
    return target


__all__ = [
    "app_dir",
    "public_dir",
    "songs_dir",
    "data_dir",
    "is_desktop",
    "get_app_dir",
    "get_public_dir",
    "get_songs_dir",
    "get_songs_dir_desktop",
    "is_desktop_profile",
]
