"""Canonical category definitions and helpers for the desktop profile."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional


_CATEGORY_PREFIX_PATTERN = re.compile(r"^\d+\s*[-_.]?\s*")
_ALIAS_TOKEN_PATTERN = re.compile(r"[^0-9a-z]+")


@dataclass(frozen=True)
class DesktopCategory:
    """Representation of a canonical desktop category."""

    id: int
    slug: str
    title: str
    aliases: tuple[str, ...]
    title_lang: Mapping[str, object]
    song_skin: Mapping[str, object]


_CANON_DESKTOP: tuple[DesktopCategory, ...] = (
    DesktopCategory(
        id=1,
        slug="pop",
        title="Pop",
        aliases=("pop", "pops", "j pop", "j-pop", "jpop"),
        title_lang={
            "ja": "J-POP",
            "en": "Pop",
            "cn": "流行音乐",
            "tw": "流行音樂",
            "ko": "POP",
        },
        song_skin={
            "sort": 1,
            "background": "#219fbb",
            "border": ["#7ec3d3", "#0b6773"],
            "outline": "#005058",
            "info_fill": "#004d68",
            "bg_img": "bg_genre_0.png",
        },
    ),
    DesktopCategory(
        id=2,
        slug="anime",
        title="Anime",
        aliases=("anime",),
        title_lang={
            "ja": "アニメ",
            "en": "Anime",
            "cn": "卡通动画音乐",
            "tw": "卡通動畫音樂",
            "ko": "애니메이션",
        },
        song_skin={
            "sort": 2,
            "background": "#ff9700",
            "border": ["#ffdb8c", "#e75500"],
            "outline": "#9c4100",
            "info_fill": "#9c4002",
            "bg_img": "bg_genre_1.png",
        },
    ),
    DesktopCategory(
        id=3,
        slug="vocaloid",
        title="VOCALOID™ Music",
        aliases=(
            "vocaloid music",
            "vocaloidtm music",
            "vocaloid",
            "vocaloid™ music",
        ),
        title_lang={
            "ja": "ボーカロイド™曲",
            "en": "VOCALOID™ Music",
        },
        song_skin={
            "sort": 3,
            "background": "#def2ef",
            "border": ["#f7fbff", "#79919f"],
            "outline": "#5a6584",
            "info_fill": "#546184",
            "bg_img": "bg_genre_2.png",
        },
    ),
    DesktopCategory(
        id=4,
        slug="children",
        title="Children & Folk",
        aliases=(
            "children",
            "children folk",
            "children and folk",
            "children & folk",
            "children-folk",
            "children/folk",
            "folk",
        ),
        title_lang={
            "ja": "童謡・民謡",
            "en": "Children & Folk",
            "cn": "儿童民谣",
            "tw": "兒童民謠",
            "ko": "동요・민요",
        },
        song_skin={
            "sort": 4,
            "background": "#8fd321",
            "border": ["#f7fbff", "#587d0b"],
            "outline": "#374c00",
            "info_fill": "#3c6800",
            "bg_img": "bg_genre_3.png",
        },
    ),
    DesktopCategory(
        id=5,
        slug="variety",
        title="Variety",
        aliases=("variety", "variety music"),
        title_lang={
            "ja": "バラエティ",
            "en": "Variety",
            "cn": "综合音乐",
            "tw": "綜合音樂",
            "ko": "버라이어티",
        },
        song_skin={
            "sort": 5,
            "background": "#9c72c0",
            "border": ["#bda2ce", "#63407e"],
            "outline": "#4b1c74",
            "info_fill": "#4f2886",
            "bg_img": "bg_genre_5.png",
        },
    ),
    DesktopCategory(
        id=6,
        slug="classical",
        title="Classical",
        aliases=("classical", "classic"),
        title_lang={
            "ja": "クラシック",
            "en": "Classical",
            "cn": "古典音乐",
            "tw": "古典音樂",
            "ko": "클래식",
        },
        song_skin={
            "sort": 6,
            "background": "#d1a016",
            "border": ["#e7cf6b", "#9a6b00"],
            "outline": "#734d00",
            "info_fill": "#865800",
            "bg_img": "bg_genre_4.png",
        },
    ),
)


CANON_DESKTOP: tuple[dict[str, object], ...] = tuple(
    {
        "id": category.id,
        "slug": category.slug,
        "title": category.title,
        "aliases": list(category.aliases),
        "title_lang": dict(category.title_lang),
        "song_skin": dict(category.song_skin),
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
    return _ALIAS_TO_SLUG.get(token)


def derive_category_from_path(path: Path, songs_root: Path) -> Optional[str]:
    try:
        relative = path.relative_to(songs_root)
    except ValueError:
        return None
    parts = relative.parts
    if not parts:
        return None
    top_segment = unicodedata.normalize("NFKC", parts[0])
    top_segment = _CATEGORY_PREFIX_PATTERN.sub("", top_segment)
    slug = slug_from_alias(top_segment)
    return slug


def resolve_category(category_id: Optional[int], title: Optional[str]) -> Optional[DesktopCategory]:
    if category_id is not None and category_id in CANON_DESKTOP_BY_ID:
        return CANON_DESKTOP_BY_ID[category_id]
    if title:
        slug = slug_from_alias(title)
        if slug:
            return CANON_DESKTOP_BY_SLUG.get(slug)
    return None


def canonical_categories_with_counts(counts: Mapping[str, int]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for category in _CANON_DESKTOP:
        count_value = int(counts.get(category.slug, 0)) if counts else 0
        entry = {
            "id": category.id,
            "title": category.title,
            "aliases": list(category.aliases),
            "title_lang": dict(category.title_lang),
            "song_skin": dict(category.song_skin),
            "count": count_value,
        }
        payload.append(entry)
    return payload


def empty_category_counts() -> Dict[str, int]:
    return {category.slug: 0 for category in _CANON_DESKTOP}


__all__ = [
    "CANON_DESKTOP",
    "CANON_DESKTOP_BY_ID",
    "CANON_DESKTOP_BY_SLUG",
    "canonical_categories_with_counts",
    "derive_category_from_path",
    "empty_category_counts",
    "resolve_category",
    "slug_from_alias",
]
