"""Song scanning and parsing utilities for Taiko Web."""
from __future__ import annotations

import fnmatch
import hashlib
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import unicodedata

from pymongo.database import Database


LOGGER = logging.getLogger(__name__)


SUPPORTED_AUDIO_EXTS = [
    ".ogg",
    ".mp3",
    ".wav",
    ".m4a",
    ".aac",
    ".flac",
    ".opus",
]

COURSE_NAMES = {
    "easy": "easy",
    "kantan": "easy",
    "normal": "normal",
    "futsuu": "normal",
    "hard": "hard",
    "muzukashii": "hard",
    "oni": "oni",
    "ura": "ura",
    "edit": "ura",
}

COURSE_ORDER = ["easy", "normal", "hard", "oni", "ura"]

DEFAULT_CATEGORY_TITLE = "Unsorted"

ENCODINGS = ["utf-8-sig", "utf-16", "utf-8", "shift_jis", "cp932", "latin-1"]


@dataclass
class CourseInfo:
    stars: Optional[int] = None
    branch: bool = False


@dataclass
class ParsedTJA:
    title: str = ""
    subtitle: str = ""
    offset: float = 0.0
    preview: float = 0.0
    wave: Optional[str] = None
    courses: Dict[str, CourseInfo] = field(default_factory=dict)
    raw_text: str = ""
    fingerprint: str = ""


def _normalise_newlines(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines)


def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def read_tja(path: Path) -> Tuple[str, str]:
    raw_bytes = path.read_bytes()
    for encoding in ENCODINGS:
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw_bytes.decode("utf-8", errors="replace")
    text = unicodedata.normalize("NFC", text)
    normalised = _normalise_newlines(text)
    return text, normalised


def parse_tja(path: Path) -> ParsedTJA:
    original_text, normalised_text = read_tja(path)
    parsed = ParsedTJA(raw_text=original_text, fingerprint=md5_text(normalised_text))

    active_course: Optional[str] = None
    branch_courses: Dict[str, bool] = {}

    for raw_line in normalised_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("//"):
            continue
        if line.startswith("#"):
            upper_line = line.upper()
            if active_course and upper_line.startswith("#BRANCH"):
                branch_courses[active_course] = True
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key_upper = key.strip().upper()
        value_stripped = value.strip()

        if key_upper == "TITLE":
            parsed.title = value_stripped
        elif key_upper == "SUBTITLE":
            parsed.subtitle = value_stripped
        elif key_upper == "OFFSET":
            try:
                parsed.offset = float(value_stripped)
            except ValueError:
                LOGGER.debug("Invalid OFFSET value '%s' in %s", value_stripped, path)
        elif key_upper in {"DEMOSTART", "PREVIEW"}:
            try:
                parsed.preview = float(value_stripped)
            except ValueError:
                LOGGER.debug("Invalid PREVIEW value '%s' in %s", value_stripped, path)
        elif key_upper == "WAVE":
            parsed.wave = value_stripped
        elif key_upper == "COURSE":
            value_lower = value_stripped.lower()
            course = COURSE_NAMES.get(value_lower)
            if not course:
                LOGGER.debug("Unknown COURSE '%s' in %s", value_stripped, path)
                active_course = None
            else:
                active_course = course
                parsed.courses.setdefault(course, CourseInfo())
        elif key_upper == "LEVEL" and active_course:
            try:
                parsed.courses.setdefault(active_course, CourseInfo()).stars = int(value_stripped)
            except ValueError:
                LOGGER.debug("Invalid LEVEL value '%s' in %s", value_stripped, path)

    for course, info in parsed.courses.items():
        info.branch = branch_courses.get(course, False)

    return parsed


def _match_any(path: Path, patterns: Iterable[str]) -> bool:
    if not patterns:
        return False
    as_posix = path.as_posix()
    return any(fnmatch.fnmatch(as_posix, pattern) for pattern in patterns)


class SongScanner:
    def __init__(
        self,
        db: Database,
        songs_dir: Path,
        songs_baseurl: str,
        ignore_globs: Optional[Iterable[str]] = None,
    ) -> None:
        self.db = db
        self.songs_dir = songs_dir
        self._songs_root = songs_dir.resolve()
        self.songs_baseurl = songs_baseurl
        self.ignore_globs = list(ignore_globs or [])
        self._next_song_id: Optional[int] = None
        self._max_song_id: int = 0

    def _ensure_sequence(self) -> None:
        if self._next_song_id is not None:
            return
        seq = self.db.seq.find_one({'name': 'songs'})
        max_song = self.db.songs.find_one(sort=[('id', -1)])
        current = 0
        if seq and isinstance(seq.get('value'), int):
            current = max(current, seq['value'])
        if max_song:
            current = max(current, max_song.get('id', 0))
        self._max_song_id = current
        self._next_song_id = current + 1

    def _get_next_song_id(self) -> int:
        self._ensure_sequence()
        assert self._next_song_id is not None
        result = self._next_song_id
        self._next_song_id += 1
        self._max_song_id = max(self._max_song_id, result)
        return result

    def _update_sequence(self) -> None:
        if self._max_song_id > 0:
            self.db.seq.update_one(
                {'name': 'songs'},
                {'$set': {'value': self._max_song_id}},
                upsert=True,
            )

    def _iter_tja_files(self) -> Iterable[Path]:
        if not self.songs_dir.exists():
            return []
        for path in sorted(self.songs_dir.rglob('*.tja')):
            try:
                resolved = path.resolve()
            except FileNotFoundError:
                continue
            if path.is_symlink():
                LOGGER.debug("Skipping symlinked chart %s", path)
                continue
            try:
                relative = resolved.relative_to(self._songs_root)
            except ValueError:
                LOGGER.warning("Skipping chart outside songs dir: %s", path)
                continue
            if _match_any(relative, self.ignore_globs):
                continue
            yield resolved

    def _build_url(self, relative_path: Path) -> str:
        rel_posix = relative_path.as_posix()
        if rel_posix == '.':
            rel_posix = ''
        base = self.songs_baseurl
        if not base.endswith('/'):
            base += '/'
        return base + rel_posix

    def _detect_audio(self, tja_path: Path, parsed: ParsedTJA) -> Tuple[Optional[Path], List[str]]:
        diagnostics: List[str] = []
        if parsed.wave:
            candidate = (tja_path.parent / parsed.wave).resolve()
            try:
                candidate.relative_to(self._songs_root)
            except ValueError:
                diagnostics.append('wave-outside-root')
            else:
                if candidate.is_file():
                    return candidate, diagnostics
                diagnostics.append('wave-missing')
        candidates = sorted(
            [p for p in tja_path.parent.iterdir() if p.is_file()],
            key=lambda p: p.name.lower(),
        )
        for audio_path in candidates:
            resolved_audio = audio_path.resolve()
            try:
                resolved_audio.relative_to(self._songs_root)
            except ValueError:
                continue
            if resolved_audio.suffix.lower() in SUPPORTED_AUDIO_EXTS:
                return resolved_audio, diagnostics
        diagnostics.append('no-audio')
        return None, diagnostics

    def _determine_category(self, tja_path: Path) -> Tuple[int, str]:
        try:
            relative = tja_path.relative_to(self._songs_root)
        except ValueError:
            return 0, DEFAULT_CATEGORY_TITLE
        parts = relative.parts
        if not parts:
            return 0, DEFAULT_CATEGORY_TITLE
        top_folder = parts[0]
        match = re.match(r'^(\d{2})\s+(.+)$', top_folder)
        if match:
            number = int(match.group(1))
            title = match.group(2).strip() or DEFAULT_CATEGORY_TITLE
            return number, title
        return 0, DEFAULT_CATEGORY_TITLE

    def scan(self) -> Dict[str, int]:
        summary = {
            'found': 0,
            'inserted': 0,
            'updated': 0,
            'disabled': 0,
            'errors': 0,
        }
        categories: Dict[int, str] = {0: DEFAULT_CATEGORY_TITLE}

        if not self.songs_dir.exists():
            LOGGER.warning("Songs directory %s does not exist", self.songs_dir)
            return summary

        for tja_path in self._iter_tja_files():
            summary['found'] += 1
            try:
                parsed = parse_tja(tja_path)
                audio_path, diagnostics = self._detect_audio(tja_path, parsed)
            except Exception:  # pragma: no cover - defensive
                LOGGER.exception("Failed to parse %s", tja_path)
                summary['errors'] += 1
                continue

            tja_bytes = tja_path.read_bytes()
            file_hash = md5_bytes(tja_bytes)
            fingerprint = parsed.fingerprint

            relative_tja = tja_path.relative_to(self._songs_root)
            tja_url = self._build_url(relative_tja)
            dir_url = self._build_url(relative_tja.parent)
            if not dir_url.endswith('/'):
                dir_url += '/'

            audio_url = None
            music_type = None
            if audio_path:
                relative_audio = audio_path.resolve().relative_to(self._songs_root)
                audio_url = self._build_url(relative_audio)
                music_type = audio_path.suffix.lower().lstrip('.')

            category_id, category_title = self._determine_category(tja_path)
            if category_id and category_title:
                categories[category_id] = category_title

            courses_doc: Dict[str, Optional[Dict[str, object]]] = {course: None for course in COURSE_ORDER}
            for course in COURSE_ORDER:
                course_info = parsed.courses.get(course)
                if course_info and course_info.stars is not None:
                    courses_doc[course] = {
                        'stars': course_info.stars,
                        'branch': bool(course_info.branch),
                    }
                elif course_info:
                    courses_doc[course] = {
                        'stars': 0,
                        'branch': bool(course_info.branch),
                    }

            enabled = bool(audio_url)
            if not enabled:
                summary['disabled'] += 1

            document = {
                'title': parsed.title or tja_path.stem,
                'title_lang': {
                    'ja': parsed.title or tja_path.stem,
                    'en': None,
                    'cn': None,
                    'tw': None,
                    'ko': None,
                },
                'subtitle': parsed.subtitle,
                'subtitle_lang': {
                    'ja': parsed.subtitle,
                    'en': None,
                    'cn': None,
                    'tw': None,
                    'ko': None,
                },
                'courses': courses_doc,
                'enabled': enabled,
                'category_id': category_id,
                'type': 'tja',
                'offset': parsed.offset,
                'skin_id': 0,
                'preview': parsed.preview if parsed.preview else 0.0,
                'volume': 1.0,
                'maker_id': 0,
                'hash': file_hash,
                'fingerprint': fingerprint,
                'order': None,
                'paths': {
                    'tja_url': tja_url,
                    'audio_url': audio_url,
                    'dir_url': dir_url,
                },
                'music_type': music_type,
                'diagnostics': diagnostics if diagnostics else [],
            }

            existing = self.db.songs.find_one({'hash': file_hash})
            if not existing:
                existing = self.db.songs.find_one({'paths.tja_url': tja_url})

            if existing:
                document['id'] = existing['id']
                document['order'] = existing.get('order', existing['id'])
                self.db.songs.update_one({'id': existing['id']}, {'$set': document})
                summary['updated'] += 1
            else:
                new_id = self._get_next_song_id()
                document['id'] = new_id
                document['order'] = new_id
                self.db.songs.insert_one(document)
                summary['inserted'] += 1

        self._update_sequence()

        # Update categories collection
        for cat_id, title in categories.items():
            update = {
                'id': cat_id,
                'title': title,
            }
            existing_cat = self.db.categories.find_one({'id': cat_id})
            if existing_cat:
                self.db.categories.update_one({'id': cat_id}, {'$set': {'title': title}})
            else:
                update.setdefault('song_skin', None)
                self.db.categories.insert_one(update)

        return summary
