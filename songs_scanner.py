"""Song scanning and parsing utilities for Taiko Web."""
from __future__ import annotations

import fnmatch
import hashlib
import logging
import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple, Callable
import unicodedata

from pymongo.database import Database

try:  # pragma: no cover - pymongo always available in production
    from pymongo.errors import DuplicateKeyError, PyMongoError
except Exception:  # pragma: no cover - fallback when pymongo unavailable
    DuplicateKeyError = None  # type: ignore[assignment]
    PyMongoError = None  # type: ignore[assignment]


try:  # pragma: no cover - watchdog is optional during tests
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except Exception:  # pragma: no cover - watchdog optional dependency
    FileSystemEventHandler = None  # type: ignore[assignment]
    Observer = None  # type: ignore[assignment]


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

COURSE_ALIASES = {
    "EASY": "Easy",
    "KANTAN": "Easy",
    "AMAKUCHI": "Easy",
    "NORMAL": "Normal",
    "FUTSUU": "Normal",
    "FUTSU": "Normal",
    "KARAKUCHI": "Normal",
    "HARD": "Hard",
    "MUZUKASHII": "Hard",
    "ONI": "Oni",
    "EDIT": "Oni",
    "TOWER": "Oni",
    "URAONI": "UraOni",
    "URA": "UraOni",
}

COURSE_ORDER = ["Easy", "Normal", "Hard", "Oni", "UraOni"]

COURSE_LEGACY_MAP = {
    "Easy": "easy",
    "Normal": "normal",
    "Hard": "hard",
    "Oni": "oni",
    "UraOni": "ura",
}

DEFAULT_CATEGORY_TITLE = "Unsorted"
UNKNOWN_VALUE = "Unknown"

ENCODINGS = ["utf-8-sig", "utf-16", "utf-8", "shift_jis", "cp932", "latin-1"]

ZERO_WIDTH_CHARACTERS = {
    "\u200b",  # zero width space
    "\u200c",  # zero width non-joiner
    "\u200d",  # zero width joiner
    "\ufeff",  # zero width no-break space / BOM
    "\u2060",  # word joiner
    "\u180e",  # mongolian vowel separator
}


@dataclass
class CourseInfo:
    canonical: str
    raw_name: str
    normalised: str
    stars: Optional[int] = None
    branch: bool = False
    branch_sections: Set[str] = field(default_factory=set)
    start_blocks: int = 0
    end_blocks: int = 0
    issues: List[str] = field(default_factory=list)

    def add_issue(self, issue: str) -> None:
        if issue not in self.issues:
            self.issues.append(issue)


@dataclass
class ParsedTJA:
    title: str = ""
    title_ja: str = ""
    subtitle: str = ""
    subtitle_ja: str = ""
    offset: float = 0.0
    preview: float = 0.0
    wave: Optional[str] = None
    genre: Optional[str] = None
    song_id: Optional[str] = None
    courses: List[CourseInfo] = field(default_factory=list)
    raw_text: str = ""
    fingerprint: str = ""


@dataclass
class ChartRecord:
    course: str
    raw_course: str
    normalised: str
    level: Optional[int]
    branch: bool
    valid: bool
    issues: List[str]
    coerced: bool = False


@dataclass
class TjaImportRecord:
    relative_path: str
    relative_dir: str
    tja_url: str
    dir_url: str
    audio_url: Optional[str]
    audio_path: Optional[str]
    audio_hash: Optional[str]
    audio_mtime_ns: Optional[int]
    audio_size: Optional[int]
    music_type: Optional[str]
    diagnostics: List[str]
    title: str
    title_ja: Optional[str]
    subtitle: str
    subtitle_ja: Optional[str]
    locale: Dict[str, Dict[str, Optional[str]]]
    offset: float
    preview: float
    fingerprint: str
    tja_hash: str
    wave: Optional[str]
    song_id: Optional[str]
    genre: Optional[str]
    category_id: int
    category_title: str
    charts: List[ChartRecord]
    import_issues: List[str]
    normalized_title: str


def _normalise_newlines(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines)


def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def read_tja(path: Path) -> Tuple[str, str]:
    raw_bytes = path.read_bytes()
    encoding_used: Optional[str] = None
    for encoding in ENCODINGS:
        try:
            text = raw_bytes.decode(encoding)
            encoding_used = encoding
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw_bytes.decode("utf-8", errors="replace")
        encoding_used = "utf-8"
    if encoding_used and not encoding_used.lower().startswith("utf"):
        LOGGER.warning("Decoded %s using non-UTF encoding %s", path, encoding_used)
    text = unicodedata.normalize("NFC", text)
    normalised = _normalise_newlines(text)
    return text, normalised


def _normalise_invisible_whitespace(value: str) -> str:
    """Replace non-breaking whitespace and strip zero-width characters."""

    normalised_chars: List[str] = []
    for char in value:
        if char in ZERO_WIDTH_CHARACTERS:
            continue
        category = unicodedata.category(char)
        if category == "Cf":
            # Other format characters such as directional marks should not affect search.
            continue
        if category == "Zs" and char != " ":
            normalised_chars.append(" ")
            continue
        if char == "\xa0":  # NBSP
            normalised_chars.append(" ")
            continue
        normalised_chars.append(char)
    normalised = "".join(normalised_chars)
    # Collapse runs of ASCII whitespace to a single space to stabilise search tokens.
    normalised = re.sub(r"[\t\f\v ]+", " ", normalised)
    return normalised


def _clean_metadata_value(value: str) -> str:
    """Remove characters that cannot be stored in MongoDB documents."""

    # MongoDB rejects strings containing the null character, which can appear
    # when UTF-16 encoded TJAs include trailing nulls in metadata fields.
    cleaned = value.replace("\x00", "")
    cleaned = _normalise_invisible_whitespace(cleaned)
    return cleaned


def _normalise_course_token(value: str) -> str:
    token = re.sub(r"[\s\-_]", "", value.upper())
    return token


def _resolve_course(value: str) -> Tuple[str, str]:
    token = _normalise_course_token(value)
    canonical = COURSE_ALIASES.get(token)
    return (canonical or "Unknown", token)


def _normalise_title_key(value: str) -> str:
    value = value.strip().casefold()
    value = re.sub(r"\s+", " ", value)
    return value


def _derive_genre_from_path(relative_tja: Path, category_title: str) -> str:
    parts = list(relative_tja.parts)
    if len(parts) > 1:
        parent_name = _clean_metadata_value(parts[-2])
        if parent_name:
            return parent_name
    cleaned_category = _clean_metadata_value(category_title) if category_title else None
    return cleaned_category or DEFAULT_CATEGORY_TITLE


def parse_tja(path: Path) -> ParsedTJA:
    original_text, normalised_text = read_tja(path)
    parsed = ParsedTJA(raw_text=original_text, fingerprint=md5_text(normalised_text))

    active_course: Optional[CourseInfo] = None
    known_courses: Dict[str, CourseInfo] = {}

    for raw_line in normalised_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("//"):
            continue
        if line.startswith("#"):
            upper_line = line.upper()
            if active_course:
                if upper_line == "#START":
                    active_course.start_blocks += 1
                elif upper_line == "#END":
                    active_course.end_blocks += 1
                elif upper_line.startswith("#BRANCH"):
                    active_course.branch = True
                    if upper_line.startswith("#BRANCHSTART"):
                        active_course.branch_sections.add("START")
                elif upper_line in {"#N", "#E", "#M"}:
                    active_course.branch_sections.add(upper_line[1:])
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key_upper = key.strip().upper()
        value_stripped = value.strip()

        clean_value = _clean_metadata_value(value_stripped)

        if key_upper == "TITLE":
            parsed.title = clean_value
        elif key_upper == "TITLEJA":
            parsed.title_ja = clean_value
        elif key_upper == "SUBTITLE":
            parsed.subtitle = clean_value
        elif key_upper == "SUBTITLEJA":
            parsed.subtitle_ja = clean_value
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
            parsed.wave = clean_value or None
        elif key_upper == "GENRE":
            parsed.genre = clean_value or None
        elif key_upper == "SONGID":
            parsed.song_id = clean_value or None
        elif key_upper == "COURSE":
            canonical, token = _resolve_course(value_stripped)
            if canonical == "Unknown":
                LOGGER.warning("Unknown COURSE '%s' in %s", value_stripped, path)
                active_course = CourseInfo(
                    canonical="Unknown",
                    raw_name=value_stripped,
                    normalised=token,
                )
                parsed.courses.append(active_course)
            else:
                existing = known_courses.get(canonical)
                if existing:
                    active_course = existing
                    active_course.raw_name = value_stripped
                    active_course.normalised = token
                else:
                    active_course = CourseInfo(
                        canonical=canonical,
                        raw_name=value_stripped,
                        normalised=token,
                    )
                    known_courses[canonical] = active_course
                    parsed.courses.append(active_course)
        elif key_upper == "LEVEL" and active_course:
            try:
                level_value = float(value_stripped)
            except ValueError:
                LOGGER.warning("Invalid LEVEL value '%s' in %s", value_stripped, path)
                active_course.add_issue("invalid-level")
                continue
            level_int = int(round(level_value))
            clamped = max(1, min(10, level_int))
            if level_int != level_value:
                active_course.add_issue("level-non-integer")
            if clamped != level_int:
                LOGGER.warning(
                    "LEVEL value %s for course '%s' in %s out of range; clamped to %s",
                    value_stripped,
                    active_course.raw_name,
                    path,
                    clamped,
                )
                active_course.add_issue("level-out-of-range")
            active_course.stars = clamped

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
        coerce_unknown_course: Optional[str] = None,
    ) -> None:
        self.db = db
        self.songs_dir = songs_dir
        self._songs_root = songs_dir.resolve()
        self.songs_baseurl = songs_baseurl
        self.ignore_globs = list(ignore_globs or [])
        self._coerce_unknown_course: Optional[str] = None
        if coerce_unknown_course:
            token = coerce_unknown_course.strip()
            if token:
                lowered = token.casefold()
                for canonical in COURSE_ORDER:
                    if canonical.casefold() == lowered or COURSE_LEGACY_MAP[canonical] == lowered:
                        self._coerce_unknown_course = canonical
                        break
        self._next_song_id: Optional[int] = None
        self._max_song_id: int = 0
        self._scan_lock = threading.Lock()
        self._state_collection = getattr(self.db, 'song_scanner_state', None)
        if self._state_collection is not None:
            try:
                self._state_collection.create_index('tja_path', unique=True)
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure unique index for song_scanner_state collection')
        self._watchdog_supported = Observer is not None and FileSystemEventHandler is not None

    def _build_chart_records(self, parsed: ParsedTJA, tja_path: Path) -> Tuple[List[ChartRecord], List[str]]:
        records: List[ChartRecord] = []
        import_issues: List[str] = []
        for course in parsed.courses:
            course_name = course.canonical
            coerced = False
            issues = list(course.issues)

            if course_name == "Unknown":
                if self._coerce_unknown_course:
                    LOGGER.warning(
                        "Coercing unknown course '%s' in %s to %s",
                        course.raw_name,
                        tja_path,
                        self._coerce_unknown_course,
                    )
                    course_name = self._coerce_unknown_course
                    coerced = True
                else:
                    issues.append("unknown-course")

            if course.start_blocks == 0 or course.end_blocks == 0 or course.end_blocks < course.start_blocks:
                issues.append("missing-chart-content")
            if course.branch:
                required_sections = {"N", "E", "M"}
                if not required_sections.issubset(course.branch_sections):
                    issues.append("invalid-branch-sections")

            level_value = course.stars if course.stars is not None else 0
            if course.stars is None:
                issues.append("missing-level")

            valid = course_name in COURSE_ORDER and "missing-chart-content" not in issues and "unknown-course" not in issues

            if course.branch and "invalid-branch-sections" in issues:
                valid = False

            record = ChartRecord(
                course=course_name,
                raw_course=course.raw_name,
                normalised=course.normalised,
                level=level_value,
                branch=course.branch,
                valid=valid,
                issues=sorted(set(issues)),
                coerced=coerced,
            )
            records.append(record)

            if record.issues:
                import_issues.extend(record.issues)

        return records, sorted(set(import_issues))

    def _build_import_record(
        self,
        *,
        tja_path: Path,
        relative_tja: Path,
        parsed: ParsedTJA,
        fingerprint: str,
        file_hash: str,
        audio_path: Optional[Path],
        audio_url: Optional[str],
        audio_hash: Optional[str],
        audio_mtime_ns: Optional[int],
        audio_size: Optional[int],
        music_type: Optional[str],
        diagnostics: List[str],
        category_id: int,
        category_title: str,
    ) -> TjaImportRecord:
        charts, chart_issues = self._build_chart_records(parsed, tja_path)
        import_issues = list(chart_issues)

        fallback_title = _clean_metadata_value(tja_path.stem)
        if not fallback_title:
            fallback_title = UNKNOWN_VALUE

        title_value = (parsed.title or "").strip() or fallback_title or UNKNOWN_VALUE
        if not (parsed.title or "").strip():
            import_issues.append('missing-title')
        subtitle_value = (parsed.subtitle or "").strip() or UNKNOWN_VALUE
        title_ja_value = (parsed.title_ja or "").strip() or None
        subtitle_ja_value = (parsed.subtitle_ja or "").strip() or None

        locale_doc: Dict[str, Dict[str, Optional[str]]] = {
            'en': {
                'title': title_value,
                'subtitle': subtitle_value,
            }
        }
        if title_ja_value or subtitle_ja_value:
            locale_doc['ja'] = {
                'title': title_ja_value or title_value,
                'subtitle': subtitle_ja_value or subtitle_value,
            }

        relative_audio = None
        if audio_path:
            try:
                relative_audio = audio_path.resolve().relative_to(self._songs_root).as_posix()
            except ValueError:
                relative_audio = None

        if not parsed.wave:
            import_issues.append('missing-wave')
        if not charts:
            import_issues.append('no-courses')

        if audio_url is None:
            import_issues.append('missing-audio')

        valid_chart_count = sum(1 for chart in charts if chart.valid)
        if valid_chart_count == 0:
            import_issues.append('no-valid-course')

        normalized_title = _normalise_title_key(title_value)

        dir_url = self._build_url(relative_tja.parent)
        if not dir_url.endswith('/'):
            dir_url += '/'

        genre_value = parsed.genre or _derive_genre_from_path(relative_tja, category_title)

        record = TjaImportRecord(
            relative_path=relative_tja.as_posix(),
            relative_dir=relative_tja.parent.as_posix(),
            tja_url=self._build_url(relative_tja),
            dir_url=dir_url,
            audio_url=audio_url,
            audio_path=relative_audio,
            audio_hash=audio_hash,
            audio_mtime_ns=audio_mtime_ns,
            audio_size=audio_size,
            music_type=music_type,
            diagnostics=diagnostics if diagnostics else [],
            title=title_value,
            title_ja=title_ja_value,
            subtitle=subtitle_value,
            subtitle_ja=subtitle_ja_value,
            locale=locale_doc,
            offset=parsed.offset,
            preview=parsed.preview if parsed.preview else 0.0,
            fingerprint=fingerprint,
            tja_hash=file_hash,
            wave=parsed.wave,
            song_id=parsed.song_id,
            genre=genre_value,
            category_id=category_id,
            category_title=category_title,
            charts=charts,
            import_issues=sorted(set(import_issues)),
            normalized_title=normalized_title,
        )
        return record

    def _record_from_state(self, payload: Dict[str, object]) -> Optional[TjaImportRecord]:
        try:
            charts_raw = payload.get('charts') or []
            charts = [
                ChartRecord(
                    course=str(item.get('course', 'Unknown')),
                    raw_course=str(item.get('raw_course', '')),
                    normalised=str(item.get('normalised', '')),
                    level=int(item.get('level', 0)) if item.get('level') is not None else None,
                    branch=bool(item.get('branch', False)),
                    valid=bool(item.get('valid', False)),
                    issues=list(item.get('issues', [])),
                    coerced=bool(item.get('coerced', False)),
                )
                for item in charts_raw
            ]
            record = TjaImportRecord(
                relative_path=str(payload['relative_path']),
                relative_dir=str(payload.get('relative_dir', '')),
                tja_url=str(payload.get('tja_url', '')),
                dir_url=str(payload.get('dir_url', '')),
                audio_url=payload.get('audio_url'),
                audio_path=payload.get('audio_path'),
                audio_hash=payload.get('audio_hash'),
                audio_mtime_ns=payload.get('audio_mtime_ns'),
                audio_size=payload.get('audio_size'),
                music_type=payload.get('music_type'),
                diagnostics=list(payload.get('diagnostics', [])),
                title=str(payload.get('title', UNKNOWN_VALUE)),
                title_ja=payload.get('title_ja'),
                subtitle=str(payload.get('subtitle', UNKNOWN_VALUE)),
                subtitle_ja=payload.get('subtitle_ja'),
                locale=dict(payload.get('locale', {})),
                offset=float(payload.get('offset', 0.0)),
                preview=float(payload.get('preview', 0.0)),
                fingerprint=str(payload.get('fingerprint', '')),
                tja_hash=str(payload.get('tja_hash', '')),
                wave=payload.get('wave'),
                song_id=payload.get('song_id'),
                genre=payload.get('genre'),
                category_id=int(payload.get('category_id', 0)),
                category_title=str(payload.get('category_title', DEFAULT_CATEGORY_TITLE)),
                charts=charts,
                import_issues=list(payload.get('import_issues', [])),
                normalized_title=str(payload.get('normalized_title', '')),
            )
            return record
        except Exception:
            LOGGER.debug('Failed to reconstruct TJA record from state payload')
            return None

    def _determine_group_key(self, record: TjaImportRecord) -> str:
        if record.song_id:
            return f"songid:{record.song_id}"
        audio_part = record.audio_hash or f"missing:{record.relative_dir}:{record.relative_path}"
        return f"audio:{audio_part}:{record.normalized_title}"

    def _select_base_record(self, records: List[TjaImportRecord]) -> TjaImportRecord:
        def _score(record: TjaImportRecord) -> Tuple[int, int, bool]:
            valid = sum(1 for chart in record.charts if chart.valid)
            return (valid, len(record.charts), bool(record.audio_url))

        return max(records, key=_score)

    def _build_song_document(self, key: str, records: List[TjaImportRecord]) -> Dict[str, object]:
        base = self._select_base_record(records)

        canonical_map: Dict[str, Dict[str, object]] = {}
        charts_payload: List[Dict[str, object]] = []
        for record in records:
            for chart in record.charts:
                entry = {
                    'course': chart.course,
                    'level': chart.level,
                    'branch': chart.branch,
                    'valid': chart.valid,
                    'issues': chart.issues,
                    'coerced': chart.coerced,
                    'tja_path': record.relative_path,
                    'tja_url': record.tja_url,
                }
                charts_payload.append(entry)
                if chart.course in COURSE_ORDER:
                    existing = canonical_map.get(chart.course)
                    if existing is None or (chart.valid and not existing['valid']):
                        canonical_map[chart.course] = entry

        courses_doc: Dict[str, Optional[Dict[str, object]]] = {
            legacy: None for legacy in COURSE_LEGACY_MAP.values()
        }
        for canonical, entry in canonical_map.items():
            legacy = COURSE_LEGACY_MAP[canonical]
            courses_doc[legacy] = {
                'stars': entry['level'] or 0,
                'branch': bool(entry['branch']),
            }

        valid_chart_count = sum(1 for chart in canonical_map.values() if chart['valid'])

        import_issues = sorted({issue for record in records for issue in record.import_issues})
        diagnostics = sorted({diag for record in records for diag in record.diagnostics})

        audio_hash = None
        audio_url = None
        audio_path = None
        music_type = None
        audio_mtime_ns = None
        audio_size = None
        for record in records:
            if record.audio_hash and audio_hash is None:
                audio_hash = record.audio_hash
            if record.audio_url and audio_url is None:
                audio_url = record.audio_url
                audio_path = record.audio_path
                music_type = record.music_type
                audio_mtime_ns = record.audio_mtime_ns
                audio_size = record.audio_size

        combined_hash = md5_text("|".join(sorted(record.tja_hash for record in records)))
        combined_fingerprint = md5_text("|".join(sorted(record.fingerprint for record in records)))

        title_lang = {
            'ja': base.title_ja or base.title,
            'en': None,
            'cn': None,
            'tw': None,
            'ko': None,
        }
        subtitle_lang = {
            'ja': base.subtitle_ja or base.subtitle,
            'en': None,
            'cn': None,
            'tw': None,
            'ko': None,
        }

        enabled = bool(audio_url)

        document = {
            'title': base.title,
            'titleJa': base.title_ja,
            'title_lang': title_lang,
            'subtitle': base.subtitle,
            'subtitleJa': base.subtitle_ja,
            'subtitle_lang': subtitle_lang,
            'locale': base.locale,
            'courses': courses_doc,
            'charts': charts_payload,
            'import_issues': import_issues,
            'valid_chart_count': valid_chart_count,
            'enabled': enabled,
            'category_id': base.category_id,
            'type': 'tja',
            'offset': base.offset,
            'skin_id': 0,
            'preview': base.preview,
            'volume': 1.0,
            'maker_id': 0,
            'hash': combined_hash,
            'fingerprint': combined_fingerprint,
            'order': None,
            'paths': {
                'tja_url': base.tja_url,
                'audio_url': audio_url,
                'dir_url': base.dir_url,
            },
            'music_type': music_type,
            'diagnostics': diagnostics,
            'managed_by_scanner': True,
            'titleNormalized': base.normalized_title,
            'group_key': key,
            'genre': base.genre,
        }
        if audio_hash is not None:
            document['audioHash'] = audio_hash
        return document

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
        if len(parts) == 1:
            return 0, DEFAULT_CATEGORY_TITLE
        top_folder = parts[0]
        match = re.match(r'^(\d{2})\s+(.+)$', top_folder)
        if match:
            number = int(match.group(1))
            raw_title = match.group(2).strip()
            title = _clean_metadata_value(raw_title) or DEFAULT_CATEGORY_TITLE
            return number, title
        fallback = _clean_metadata_value(top_folder) or DEFAULT_CATEGORY_TITLE
        return 0, fallback

    def scan(self, *, full: bool = False) -> Dict[str, int]:
        """Scan songs directory and sync metadata with MongoDB."""

        start_time = time.perf_counter()
        with self._scan_lock:
            summary = self._scan_impl(full=full)
        summary['duration_seconds'] = round(time.perf_counter() - start_time, 3)
        return summary

    def _scan_impl(self, *, full: bool) -> Dict[str, int]:
        summary = {
            'found': 0,
            'inserted': 0,
            'updated': 0,
            'disabled': 0,
            'errors': 0,
            'skipped': 0,
        }
        categories: Dict[int, str] = {0: DEFAULT_CATEGORY_TITLE}
        managed_songs: Dict[int, bool] = {}
        seen_song_ids: Set[int] = set()
        seen_state_paths: Set[str] = set()

        state_docs: Dict[str, Dict[str, object]] = {}
        if self._state_collection is not None:
            try:
                for doc in self._state_collection.find():
                    path_value = doc.get('tja_path')
                    if isinstance(path_value, str):
                        state_docs[path_value] = dict(doc)
            except Exception:  # pragma: no cover - tolerate collection access issues
                LOGGER.debug('Failed to read song scanner state collection')

        try:
            cursor = self.db.songs.find({'managed_by_scanner': True}, {'id': 1, 'enabled': 1})
        except AttributeError:
            cursor = []
        except Exception:  # pragma: no cover - defensive when find unsupported
            LOGGER.debug("songs.find is not available on db collection")
            cursor = []

        for doc in cursor:
            doc_id = doc.get('id')
            if isinstance(doc_id, int):
                managed_songs[doc_id] = bool(doc.get('enabled', True))

        if not self.songs_dir.exists():
            LOGGER.warning("Songs directory %s does not exist", self.songs_dir)
            return summary

        aggregated_records: Dict[str, List[TjaImportRecord]] = defaultdict(list)
        records_by_path: Dict[str, TjaImportRecord] = {}
        record_meta: Dict[str, Dict[str, object]] = {}
        group_key_by_path: Dict[str, str] = {}
        dirty_groups: Set[str] = set()

        for tja_path in self._iter_tja_files():
            summary['found'] += 1
            try:
                relative_tja = tja_path.relative_to(self._songs_root)
            except ValueError:
                LOGGER.warning("Skipping chart outside songs dir: %s", tja_path)
                summary['errors'] += 1
                continue

            tja_key = relative_tja.as_posix()
            state_doc = state_docs.get(tja_key)
            seen_state_paths.add(tja_key)

            try:
                tja_stat = tja_path.stat()
            except FileNotFoundError:
                summary['errors'] += 1
                LOGGER.warning("Chart disappeared during scan: %s", tja_path)
                continue

            tja_mtime_ns = getattr(tja_stat, 'st_mtime_ns', int(tja_stat.st_mtime * 1_000_000_000))
            tja_size = tja_stat.st_size

            needs_processing = full or state_doc is None
            if state_doc is not None and not needs_processing:
                if state_doc.get('tja_mtime_ns') != tja_mtime_ns or state_doc.get('tja_size') != tja_size:
                    needs_processing = True

            if state_doc is not None and not needs_processing:
                stored_audio_path = state_doc.get('audio_path') if isinstance(state_doc.get('audio_path'), str) else None
                if stored_audio_path:
                    audio_candidate = (self._songs_root / stored_audio_path).resolve()
                    if audio_candidate.exists():
                        audio_stat = audio_candidate.stat()
                        audio_mtime_ns = getattr(audio_stat, 'st_mtime_ns', int(audio_stat.st_mtime * 1_000_000_000))
                        audio_size = audio_stat.st_size
                        if state_doc.get('audio_mtime_ns') != audio_mtime_ns or state_doc.get('audio_size') != audio_size:
                            needs_processing = True
                    else:
                        needs_processing = True
                else:
                    needs_processing = True

            record: Optional[TjaImportRecord] = None
            diagnostics: List[str] = []
            file_hash: Optional[str] = None
            fingerprint: Optional[str] = None
            was_dirty = needs_processing

            if not needs_processing and state_doc:
                record_payload = state_doc.get('record') if isinstance(state_doc.get('record'), dict) else None
                if record_payload:
                    record = self._record_from_state(record_payload)
                    if record:
                        file_hash = str(state_doc.get('tja_hash') or record.tja_hash)
                        fingerprint = str(state_doc.get('fingerprint') or record.fingerprint)
                        group_key_by_path[tja_key] = state_doc.get('group_key', '') or self._determine_group_key(record)
                        summary['skipped'] += 1
                if record is None:
                    needs_processing = True

            if needs_processing:
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

                audio_url = None
                music_type = None
                audio_hash = None
                audio_mtime_ns = None
                audio_size = None
                if audio_path:
                    try:
                        relative_audio = audio_path.resolve().relative_to(self._songs_root)
                    except ValueError:
                        diagnostics.append('wave-outside-root')
                        relative_audio = None
                    else:
                        audio_url = self._build_url(relative_audio)
                    if audio_url:
                        music_type = audio_path.suffix.lower().lstrip('.')
                        audio_bytes = audio_path.read_bytes()
                        audio_hash = md5_bytes(audio_bytes)
                        audio_stat = audio_path.stat()
                        audio_mtime_ns = getattr(audio_stat, 'st_mtime_ns', int(audio_stat.st_mtime * 1_000_000_000))
                        audio_size = audio_stat.st_size

                category_id, category_title = self._determine_category(tja_path)
                if category_id and category_title:
                    categories[category_id] = category_title

                record = self._build_import_record(
                    tja_path=tja_path,
                    relative_tja=relative_tja,
                    parsed=parsed,
                    fingerprint=fingerprint,
                    file_hash=file_hash,
                    audio_path=audio_path,
                    audio_url=audio_url,
                    audio_hash=audio_hash,
                    audio_mtime_ns=audio_mtime_ns,
                    audio_size=audio_size,
                    music_type=music_type,
                    diagnostics=diagnostics,
                    category_id=category_id,
                    category_title=category_title,
                )

            if record is None:
                summary['errors'] += 1
                continue

            key = group_key_by_path.get(tja_key) or self._determine_group_key(record)
            group_key_by_path[tja_key] = key
            aggregated_records[key].append(record)
            records_by_path[tja_key] = record

            if was_dirty:
                dirty_groups.add(key)

            record_meta[tja_key] = {
                'tja_hash': file_hash or record.tja_hash,
                'tja_mtime_ns': tja_mtime_ns,
                'tja_size': tja_size,
                'audio_hash': record.audio_hash,
                'audio_mtime_ns': record.audio_mtime_ns,
                'audio_size': record.audio_size,
                'fingerprint': fingerprint or record.fingerprint,
            }

            if record.category_id != 0:
                categories[record.category_id] = record.category_title

        song_id_by_key: Dict[str, int] = {}
        for key in sorted(aggregated_records.keys()):
            records = aggregated_records[key]
            document = self._build_song_document(key, records)
            existing = None
            if key:
                existing = self.db.songs.find_one({'group_key': key})
            if not existing and document.get('audioHash') and document.get('titleNormalized'):
                existing = self.db.songs.find_one({'audioHash': document['audioHash'], 'titleNormalized': document['titleNormalized']})
            if not existing and records:
                existing = self.db.songs.find_one({'paths.tja_url': records[0].tja_url})

            if existing:
                if key not in dirty_groups:
                    seen_song_ids.add(existing['id'])
                    song_id_by_key[key] = existing['id']
                    continue
                document.pop('_id', None)
                document['id'] = existing['id']
                document['order'] = existing.get('order', existing['id'])
                self.db.songs.update_one({'id': existing['id']}, {'$set': document})
                summary['updated'] += 1
                seen_song_ids.add(existing['id'])
                song_id_by_key[key] = existing['id']
            else:
                new_id = self._get_next_song_id()
                document['id'] = new_id
                document['order'] = new_id
                try:
                    self.db.songs.insert_one(document)
                    summary['inserted'] += 1
                    seen_song_ids.add(new_id)
                    song_id_by_key[key] = new_id
                except Exception as exc:  # pragma: no cover - exercised with real MongoDB
                    handled = False
                    if DuplicateKeyError and isinstance(exc, DuplicateKeyError):
                        fallback = self.db.songs.find_one({'group_key': key})
                        if not fallback and records:
                            fallback = self.db.songs.find_one({'paths.tja_url': records[0].tja_url})
                        if fallback:
                            document.pop('_id', None)
                            document['id'] = fallback['id']
                            document['order'] = fallback.get('order', fallback['id'])
                            self.db.songs.update_one({'id': fallback['id']}, {'$set': document})
                            summary['updated'] += 1
                            seen_song_ids.add(fallback['id'])
                            song_id_by_key[key] = fallback['id']
                            handled = True
                        else:
                            LOGGER.warning("Duplicate key when inserting %s but no existing song was found", key)
                            summary['errors'] += 1
                            handled = True
                    elif PyMongoError and isinstance(exc, PyMongoError):
                        LOGGER.exception("Failed to insert aggregated song for %s", key)
                        summary['errors'] += 1
                        handled = True
                    if not handled:
                        raise

        if self._state_collection is not None:
            for tja_key, record in records_by_path.items():
                key = group_key_by_path[tja_key]
                song_id = song_id_by_key.get(key)
                if song_id is None:
                    continue
                meta = record_meta.get(tja_key, {})
                payload = {
                    'tja_path': tja_key,
                    'tja_hash': meta.get('tja_hash'),
                    'tja_mtime_ns': meta.get('tja_mtime_ns'),
                    'tja_size': meta.get('tja_size'),
                    'audio_path': record.audio_path,
                    'audio_hash': meta.get('audio_hash'),
                    'audio_mtime_ns': meta.get('audio_mtime_ns'),
                    'audio_size': meta.get('audio_size'),
                    'song_id': song_id,
                    'group_key': key,
                    'fingerprint': meta.get('fingerprint'),
                    'record': asdict(record),
                }
                if tja_key in state_docs:
                    try:
                        self._state_collection.update_one({'tja_path': tja_key}, {'$set': payload}, upsert=True)
                    except Exception:
                        LOGGER.debug('Failed to update song scanner state for %s', tja_key)
                else:
                    try:
                        self._state_collection.insert_one(payload)
                    except Exception:
                        LOGGER.debug('Failed to insert song scanner state for %s', tja_key)

        self._update_sequence()

        if self._state_collection is not None:
            stale_paths = set(state_docs.keys()) - seen_state_paths
            if stale_paths:
                try:
                    self._state_collection.delete_many({'tja_path': {'$in': list(stale_paths)}})
                except Exception:  # pragma: no cover - best effort cleanup
                    LOGGER.debug('Failed to prune %d stale scanner state entries', len(stale_paths))

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

        missing_ids = set(managed_songs.keys()) - seen_song_ids
        for missing_id in sorted(missing_ids):
            previous_enabled = managed_songs.get(missing_id, True)
            self.db.songs.update_one({'id': missing_id}, {'$set': {'enabled': False}})
            if previous_enabled:
                summary['disabled'] += 1

        return summary

    @property
    def watchdog_supported(self) -> bool:
        return self._watchdog_supported

    def start_watcher(self, callback: Optional[Callable[[], None]] = None, debounce_seconds: float = 1.0):
        if not self.watchdog_supported:
            LOGGER.info('watchdog is not available; live song updates disabled')
            return None
        if callback is None:
            callback = lambda: self.scan(full=False)

        class _EventHandler(FileSystemEventHandler):
            def __init__(self, trigger: Callable[[], None], debounce: float) -> None:
                super().__init__()
                self._trigger = trigger
                self._debounce = debounce
                self._timer: Optional[threading.Timer] = None
                self._lock = threading.Lock()

            def _schedule(self) -> None:
                with self._lock:
                    if self._timer:
                        self._timer.cancel()
                    self._timer = threading.Timer(self._debounce, self._trigger)
                    self._timer.daemon = True
                    self._timer.start()

            def on_any_event(self, event):  # type: ignore[override]
                if getattr(event, 'is_directory', False):
                    return
                path = getattr(event, 'src_path', '') or getattr(event, 'dest_path', '')
                suffix = Path(path).suffix.lower()
                if suffix not in ['.tja'] + SUPPORTED_AUDIO_EXTS:
                    return
                self._schedule()

        handler = _EventHandler(callback, debounce_seconds)
        observer = Observer()
        observer.daemon = True
        observer.schedule(handler, str(self.songs_dir), recursive=True)
        observer.start()

        class _WatcherHandle:
            def __init__(self, obs: Observer, hnd: FileSystemEventHandler) -> None:
                self._observer = obs
                self._handler = hnd

            def stop(self) -> None:
                try:
                    self._observer.stop()
                    self._observer.join(timeout=5)
                except Exception:  # pragma: no cover - shutdown best effort
                    LOGGER.debug('Failed to stop song directory watcher cleanly')

        return _WatcherHandle(observer, handler)
