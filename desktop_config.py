"""Desktop configuration helpers shared between backend targets."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Iterable, Optional, Tuple

LOGGER = logging.getLogger("taiko.desktop.config")

DESKTOP_CONFIG_ENV = "TAIKO_DESKTOP_CONFIG_PATH"
DESKTOP_CONFIG_DIRNAME = "Taiko Web Desktop"
DESKTOP_CONFIG_FILENAME = "config.json"


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[str] = set()
    result: list[Path] = []
    for path in paths:
        candidate = Path(path).expanduser()
        try:
            key = str(candidate.resolve())
        except FileNotFoundError:
            key = str(candidate)
        except Exception:
            key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result


def iter_desktop_config_paths() -> Iterable[Path]:
    """Yield potential desktop config paths in priority order."""

    candidates: list[Path] = []
    env_path = os.environ.get(DESKTOP_CONFIG_ENV)
    if env_path:
        candidates.append(Path(env_path).expanduser())

    data_dir = os.environ.get("TAIKO_DESKTOP_DATA_DIR") or os.environ.get("DATA_DIR")
    if data_dir:
        candidates.append(Path(data_dir).expanduser() / DESKTOP_CONFIG_FILENAME)

    if sys.platform == "win32":
        appdata = os.environ.get("APPDATA")
        if appdata:
            candidates.append(Path(appdata) / DESKTOP_CONFIG_DIRNAME / DESKTOP_CONFIG_FILENAME)
    else:
        xdg_home = os.environ.get("XDG_CONFIG_HOME")
        if xdg_home:
            config_root = Path(xdg_home).expanduser()
        else:
            config_root = Path.home() / ".config"
        candidates.append(config_root / DESKTOP_CONFIG_DIRNAME / DESKTOP_CONFIG_FILENAME)

    default_data_dir = Path.home() / ".taiko-web-data" / DESKTOP_CONFIG_FILENAME
    candidates.append(default_data_dir)

    return _dedupe_paths(candidates)


def load_desktop_config() -> Tuple[Optional[dict], Optional[Path]]:
    """Return the first desktop config payload and the path it was read from."""

    for candidate in iter_desktop_config_paths():
        try:
            if not candidate.is_file():
                continue
        except Exception as exc:
            LOGGER.debug("desktop.config skip path=%s error=%s", candidate, exc)
            continue
        try:
            text = candidate.read_text(encoding="utf-8")
        except Exception as exc:
            LOGGER.warning("Failed to read desktop config from %s: %s", candidate, exc)
            continue
        try:
            payload = json.loads(text)
        except Exception as exc:
            LOGGER.warning("Failed to parse desktop config %s: %s", candidate, exc)
            continue
        if isinstance(payload, dict):
            return payload, candidate
        LOGGER.warning("Desktop config %s does not contain an object payload", candidate)
    return None, None


def resolve_songs_dir_from_config(
    *,
    logger: Optional[logging.Logger] = None,
) -> Tuple[Optional[Path], Optional[Path]]:
    """Resolve the songs directory from the desktop config if present."""

    payload, path = load_desktop_config()
    if not payload:
        return None, path

    songs_value = payload.get("songs_dir")
    if isinstance(songs_value, str) and songs_value.strip():
        songs_path = Path(songs_value).expanduser()
        if logger:
            logger.info("Desktop config songs_dir=%s source=%s", songs_path, path)
        return songs_path, path

    if logger and path:
        logger.warning("Desktop config %s missing songs_dir; ignoring", path)
    return None, path


__all__ = [
    "DESKTOP_CONFIG_ENV",
    "DESKTOP_CONFIG_DIRNAME",
    "DESKTOP_CONFIG_FILENAME",
    "iter_desktop_config_paths",
    "load_desktop_config",
    "resolve_songs_dir_from_config",
]
