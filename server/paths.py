"""Path utilities shared between web and desktop runtimes."""
from __future__ import annotations

import os
import sys
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def is_desktop_profile() -> bool:
    """Return ``True`` when the desktop runtime profile is active."""

    return os.getenv("RUN_PROFILE", "").strip().lower() == "desktop"


def get_app_dir() -> Path:
    """Return the effective application directory for the backend."""

    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return _PROJECT_ROOT


def get_public_dir() -> Path:
    """Return the directory that serves static frontend assets."""

    return get_app_dir() / "public"


def get_songs_dir_desktop() -> Path:
    """Return the desktop songs directory, ensuring it exists."""

    songs_dir = get_app_dir() / "songs"
    songs_dir.mkdir(parents=True, exist_ok=True)
    return songs_dir


def get_songs_dir() -> Path:
    """Backward-compatible alias for the desktop songs directory."""

    return get_songs_dir_desktop()


__all__ = [
    "get_app_dir",
    "get_public_dir",
    "get_songs_dir",
    "get_songs_dir_desktop",
    "is_desktop_profile",
]
