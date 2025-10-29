"""Path utilities shared between web and desktop runtimes."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def is_desktop_profile() -> bool:
    """Return ``True`` when the desktop runtime profile is active."""

    return os.getenv("RUN_PROFILE", "").strip().lower() == "desktop"


def _project_root() -> Path:
    """Resolve the repository root for the current module."""

    return Path(__file__).resolve().parents[1]


def get_app_dir() -> Path:
    """Return the effective application directory for the backend."""

    if is_desktop_profile():
        executable = Path(sys.executable).resolve()
        if getattr(sys, "frozen", False):
            return executable.parent
        # When running from sources (e.g. tests) fall back to the project root.
        return _project_root()
    return _project_root()


def get_public_dir() -> Path:
    """Return the directory that serves static frontend assets."""

    return get_app_dir() / "public"


def get_songs_dir() -> Path:
    """Return the root directory containing bundled songs."""

    return get_app_dir() / "songs"


__all__ = [
    "get_app_dir",
    "get_public_dir",
    "get_songs_dir",
    "is_desktop_profile",
]
