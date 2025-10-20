"""Utilities for building the gameplay modes manifest."""

from __future__ import annotations

from typing import Iterable, Mapping, Sequence


DEFAULT_CACHE_TTL = 300


MODE_DEFINITIONS = {
    "standard": {
        "key": "standard",
        "label": "Standard",
        "notes_source": {"type": "builtin", "format": "engine-v1"},
    },
    "tower": {
        "key": "tower",
        "label": "Taiko Towers",
        "notes_source": {
            "type": "rest",
            "endpoint": "/api/tower/chart",
            "params": ["title", "course", "mode"],
            "format": "measures-v1",
        },
    },
    "dandojo": {
        "key": "dandojo",
        "label": "Dan Dojo",
        "notes_source": {
            "type": "rest",
            "endpoint": "/api/dan/chart",
            "params": ["title", "rank", "mode"],
            "format": "measures-v1",
        },
    },
}


REST_MODE_HINTS = {
    "tower": {"tower", "towers"},
    "dandojo": {"dandojo", "dan", "dojo"},
}


def _extract_title(category: Mapping[str, object]) -> str:
    title = category.get("title")
    if isinstance(title, str) and title.strip():
        return title.strip()
    name = category.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    raw = category.get("id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return ""


def _normalise_token(value: object) -> str:
    if isinstance(value, str):
        return value.strip().casefold()
    return ""


def determine_category_mode(category: Mapping[str, object]) -> str:
    """Infer the gameplay mode for a category document."""

    mode_hint = _normalise_token(category.get("mode"))
    if not mode_hint and isinstance(category.get("mode_key"), str):
        mode_hint = _normalise_token(category.get("mode_key"))

    if not mode_hint:
        modes_value = category.get("modes")
        if isinstance(modes_value, Sequence):
            for candidate in modes_value:
                token = _normalise_token(candidate)
                if token:
                    mode_hint = token
                    break

    if mode_hint:
        if mode_hint in MODE_DEFINITIONS:
            return mode_hint
        if mode_hint in {"dan", "dojo"}:
            return "dandojo"

    title = _normalise_token(_extract_title(category))
    if title:
        for key, hints in REST_MODE_HINTS.items():
            if any(hint in title for hint in hints):
                return key

    return "standard"


def build_modes_manifest(categories: Iterable[Mapping[str, object]], *, cache_ttl: int = DEFAULT_CACHE_TTL) -> dict:
    """Build the manifest payload for the supplied categories."""

    category_lists: dict[str, list[str]] = {key: [] for key in MODE_DEFINITIONS}

    for category in categories:
        if not isinstance(category, Mapping):
            continue
        title = _extract_title(category)
        if not title:
            continue
        mode_key = determine_category_mode(category)
        if mode_key not in MODE_DEFINITIONS:
            mode_key = "standard"
        bucket = category_lists[mode_key]
        if title not in bucket:
            bucket.append(title)

    modes_payload = []
    for key in ("standard", "tower", "dandojo"):
        base = MODE_DEFINITIONS[key]
        entry = {
            "key": base["key"],
            "label": base["label"],
            "categories": category_lists.get(key, []),
            "notes_source": base["notes_source"],
        }
        modes_payload.append(entry)

    return {
        "status": "ok",
        "modes": modes_payload,
        "cache_ttl": cache_ttl,
    }


__all__ = [
    "DEFAULT_CACHE_TTL",
    "MODE_DEFINITIONS",
    "build_modes_manifest",
    "determine_category_mode",
]

