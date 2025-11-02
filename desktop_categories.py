"""Canonical category definitions and helpers for the desktop profile."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional


_CATEGORY_PREFIX_PATTERN = re.compile(r"^\d+\s*[-_.]?\s*")
_ALIAS_TOKEN_PATTERN = re.compile(r"[^0-9a-z]+")
_SLUG_TOKEN_PATTERN = re.compile(r"[^0-9a-z]+")
_DEFAULT_DYNAMIC_ID_OFFSET = 100
_DEFAULT_DYNAMIC_ID_RANGE = 900


@dataclass(frozen=True)
class DesktopCategory:
    """Representation of a canonical desktop category."""

    id: int
    slug: str
    title: str
    aliases: tuple[str, ...]
    title_lang: Mapping[str, object]
    song_skin: Optional[Mapping[str, object]]


_CANON_DESKTOP: tuple[DesktopCategory, ...] = (
    DesktopCategory(
        id=1,
        slug="pop",
        title="Pop",
        aliases=("pop", "pops", "j pop", "j-pop", "jpop"),
        title_lang={
            "ja": "J-POP",
            "en": "Pop",
        },
        song_skin=None,
    ),
    DesktopCategory(
        id=2,
        slug="anime",
        title="Anime",
        aliases=("anime",),
        title_lang={
            "ja": "アニメ",
            "en": "Anime",
        },
        song_skin=None,
    ),
    DesktopCategory(
        id=3,
        slug="vocaloid",
        title="VOCALOID",
        aliases=("vocaloid", "vocaloidtm", "vocaloid™"),
        title_lang={
            "ja": "ボーカロイド",
            "en": "VOCALOID",
        },
        song_skin=None,
    ),
    DesktopCategory(
        id=4,
        slug="variety",
        title="Variety",
        aliases=("variety", "variety music"),
        title_lang={
            "ja": "バラエティ",
            "en": "Variety",
        },
        song_skin=None,
    ),
    DesktopCategory(
        id=5,
        slug="classical",
        title="Classical",
        aliases=("classical", "classic"),
        title_lang={
            "ja": "クラシック",
            "en": "Classical",
        },
        song_skin=None,
    ),
    DesktopCategory(
        id=6,
        slug="game",
        title="Game Music",
        aliases=("game music", "game-music", "game"),
        title_lang={
            "ja": "ゲームミュージック",
            "en": "Game Music",
        },
        song_skin=None,
    ),
    DesktopCategory(
        id=7,
        slug="namco",
        title="NAMCO Original",
        aliases=("namco", "namco original", "namco-original"),
        title_lang={
            "ja": "ナムコオリジナル",
            "en": "NAMCO Original",
        },
        song_skin=None,
    ),
)


CANON_DESKTOP: tuple[dict[str, object], ...] = tuple(
    {
        "id": category.id,
        "slug": category.slug,
        "title": category.title,
        "aliases": list(category.aliases),
        "title_lang": dict(category.title_lang),
        "song_skin": dict(category.song_skin) if isinstance(category.song_skin, Mapping) else None,
    }
    for category in _CANON_DESKTOP
)


CANON_DESKTOP_BY_SLUG: Dict[str, DesktopCategory] = {
    category.slug: category for category in _CANON_DESKTOP
}
CANON_DESKTOP_BY_ID: Dict[int, DesktopCategory] = {
    category.id: category for category in _CANON_DESKTOP
}


def _normalize_alias(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "")
    normalized = normalized.replace("＆", "&")
    normalized = normalized.replace("™", "tm")
    normalized = normalized.lower()
    tokens = _ALIAS_TOKEN_PATTERN.split(normalized)
    token = " ".join(part for part in tokens if part)
    return token.strip()


def normalize_topdir(name: str) -> str:
    token = unicodedata.normalize("NFKC", name or "")
    token = token.replace("＆", "&")
    token = token.strip()
    token = _CATEGORY_PREFIX_PATTERN.sub("", token)
    token = re.sub(r"\s+", " ", token)
    return token.strip()


def _slugify(value: str) -> str:
    if not value:
        return ""
    ascii_candidate = unicodedata.normalize("NFKD", value)
    ascii_candidate = ascii_candidate.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_candidate.lower()
    tokens = [token for token in _SLUG_TOKEN_PATTERN.split(lowered) if token]
    slug = "-".join(tokens)
    return slug


def normalize_category_slug(value: Optional[str]) -> str:
    if not isinstance(value, str):
        return ""
    token = value.strip()
    if not token:
        return ""
    slug = _slugify(token)
    if slug:
        return slug
    return token.casefold()


_ALIAS_TO_SLUG: Dict[str, str] = {}
for category in _CANON_DESKTOP:
    for alias in (*category.aliases, category.title, category.slug):
        token = _normalize_alias(alias)
        if token:
            _ALIAS_TO_SLUG[token] = category.slug


def slug_from_alias(value: str) -> Optional[str]:
    token = _normalize_alias(value)
    if not token:
        return None
    canonical_slug = _ALIAS_TO_SLUG.get(token)
    if canonical_slug:
        return canonical_slug
    topdir = normalize_topdir(value)
    slug = _slugify(topdir)
    return slug or None


def slug_from_topdir(name: str) -> str:
    normalized = normalize_topdir(name)
    lowered = normalized.casefold()
    if "vocaloid" in lowered:
        return "vocaloid"
    if "anime" in lowered:
        return "anime"
    if "classical" in lowered or "classic" in lowered:
        return "classical"
    if "variety" in lowered:
        return "variety"
    if "game" in lowered:
        return "game"
    if "namco" in lowered:
        return "namco"
    if "pop" in lowered or "j-pop" in lowered or "j pop" in lowered:
        return "pop"
    slug = _slugify(normalized)
    return slug or "unsorted"


def derive_category_from_path(path: Path, songs_root: Path) -> Optional[str]:
    try:
        relative = path.relative_to(songs_root)
    except ValueError:
        return None
    parts = relative.parts
    if not parts:
        return None
    top_segment = parts[0]
    slug = slug_from_topdir(top_segment)
    normalized_slug = normalize_category_slug(slug)
    return normalized_slug or None


def resolve_category(category_id: Optional[int], title: Optional[str]) -> Optional[DesktopCategory]:
    if category_id is not None and category_id in CANON_DESKTOP_BY_ID:
        return CANON_DESKTOP_BY_ID[category_id]
    if title:
        slug = slug_from_alias(title)
        if slug:
            slug = normalize_category_slug(slug)
            return CANON_DESKTOP_BY_SLUG.get(slug)
    return None


def _stable_dynamic_category_id(slug: str, *, attempt: int = 0) -> int:
    seed = f"{slug}|{attempt}".encode("utf-8")
    digest = hashlib.sha1(seed).hexdigest()
    value = int(digest[:6], 16) % _DEFAULT_DYNAMIC_ID_RANGE
    return _DEFAULT_DYNAMIC_ID_OFFSET + value


def category_id_from_slug(slug: str, *, used_ids: Optional[Iterable[int]] = None) -> int:
    normalized = normalize_category_slug(slug)
    if not normalized:
        return 0
    canonical = CANON_DESKTOP_BY_SLUG.get(normalized)
    if canonical:
        return canonical.id
    used_set = set(used_ids or ())
    attempt = 0
    while True:
        candidate = _stable_dynamic_category_id(normalized, attempt=attempt)
        if candidate not in used_set:
            return candidate
        attempt += 1


def category_title_from_slug(slug: str, fallback: Optional[str] = None) -> str:
    normalized = normalize_category_slug(slug)
    canonical = CANON_DESKTOP_BY_SLUG.get(normalized)
    if canonical:
        return canonical.title
    if fallback:
        candidate = normalize_topdir(fallback)
        if candidate:
            return candidate
    candidate = slug.replace("-", " ").replace("_", " ")
    candidate = re.sub(r"\s+", " ", candidate).strip()
    if candidate:
        return candidate.title()
    return normalized.title() if normalized else "Unsorted"


def canonical_categories_with_counts(counts: Mapping[str, int]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for category in _CANON_DESKTOP:
        count_value = int(counts.get(category.slug, 0)) if counts else 0
        entry = {
            "id": category.id,
            "title": category.title,
            "aliases": list(category.aliases),
            "title_lang": dict(category.title_lang),
            "song_skin": dict(category.song_skin) if isinstance(category.song_skin, Mapping) else None,
            "count": count_value,
        }
        payload.append(entry)
    return payload


def build_categories_payload(
    counts: Mapping[str, int],
    titles: Mapping[str, str],
    *,
    include_unsorted: bool = False,
) -> list[dict[str, object]]:
    ordered: list[dict[str, object]] = []
    used_ids: set[int] = set()
    for canonical in _CANON_DESKTOP:
        used_ids.add(canonical.id)
        count_value = int(counts.get(canonical.slug, 0)) if counts else 0
        entry = {
            "id": canonical.id,
            "title": canonical.title,
            "aliases": list(canonical.aliases),
            "title_lang": dict(canonical.title_lang),
            "song_skin": dict(canonical.song_skin) if isinstance(canonical.song_skin, Mapping) else None,
            "count": count_value,
        }
        ordered.append(entry)

    extra_slugs = [
        slug
        for slug in counts.keys()
        if slug not in CANON_DESKTOP_BY_SLUG
    ]
    extra_slugs.sort()
    for slug in extra_slugs:
        if slug == "unsorted" and not include_unsorted:
            continue
        count_value = int(counts.get(slug, 0))
        if count_value <= 0:
            continue
        category_id = category_id_from_slug(slug, used_ids=used_ids)
        used_ids.add(category_id)
        title = titles.get(slug) or category_title_from_slug(slug)
        entry = {
            "id": category_id,
            "title": title,
            "aliases": [],
            "title_lang": {},
            "song_skin": None,
            "count": count_value,
        }
        ordered.append(entry)
    return ordered


def empty_category_counts() -> Dict[str, int]:
    return {category.slug: 0 for category in _CANON_DESKTOP}


__all__ = [
    "CANON_DESKTOP",
    "CANON_DESKTOP_BY_ID",
    "CANON_DESKTOP_BY_SLUG",
    "build_categories_payload",
    "canonical_categories_with_counts",
    "category_id_from_slug",
    "category_title_from_slug",
    "derive_category_from_path",
    "empty_category_counts",
    "normalize_category_slug",
    "normalize_topdir",
    "resolve_category",
    "slug_from_alias",
    "slug_from_topdir",
]
