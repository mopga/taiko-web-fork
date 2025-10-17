"""Song scanning and parsing utilities for Taiko Web."""
from __future__ import annotations

import fnmatch
import hashlib
import logging
import re
import threading
import time
from dataclasses import dataclass, field
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
    stars: Optional[int] = None
    branch: bool = False


@dataclass
class ParsedTJA:
    title: str = ""
    title_ja: str = ""
    subtitle: str = ""
    subtitle_ja: str = ""
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
            parsed.wave = clean_value
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
        self._scan_lock = threading.Lock()
        self._state_collection = getattr(self.db, 'song_scanner_state', None)
        if self._state_collection is not None:
            try:
                self._state_collection.create_index('tja_path', unique=True)
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure unique index for song_scanner_state collection')
        self._watchdog_supported = Observer is not None and FileSystemEventHandler is not None

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
            raw_title = match.group(2).strip()
            title = _clean_metadata_value(raw_title) or DEFAULT_CATEGORY_TITLE
            return number, title
        return 0, DEFAULT_CATEGORY_TITLE

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
            tja_stat = tja_path.stat()
            tja_mtime_ns = getattr(tja_stat, 'st_mtime_ns', int(tja_stat.st_mtime * 1_000_000_000))
            tja_size = tja_stat.st_size

            needs_processing = full or state_doc is None
            if state_doc is not None and not needs_processing:
                stored_mtime = state_doc.get('tja_mtime_ns')
                stored_size = state_doc.get('tja_size')
                if stored_mtime != tja_mtime_ns or stored_size != tja_size:
                    needs_processing = True

            stored_audio_path: Optional[str] = None
            if state_doc:
                stored_audio_path = state_doc.get('audio_path') if isinstance(state_doc.get('audio_path'), str) else None
                if not needs_processing and stored_audio_path:
                    audio_candidate = (self._songs_root / stored_audio_path).resolve()
                    if audio_candidate.exists():
                        audio_stat = audio_candidate.stat()
                        audio_mtime_ns = getattr(audio_stat, 'st_mtime_ns', int(audio_stat.st_mtime * 1_000_000_000))
                        audio_size = audio_stat.st_size
                        if state_doc.get('audio_mtime_ns') != audio_mtime_ns or state_doc.get('audio_size') != audio_size:
                            needs_processing = True
                    else:
                        needs_processing = True
                if not needs_processing and not stored_audio_path:
                    # Previously missing audio, recheck occasionally in fast mode.
                    needs_processing = True

            if not needs_processing:
                existing_song_id = state_doc.get('song_id') if state_doc else None
                if isinstance(existing_song_id, int):
                    seen_song_ids.add(existing_song_id)
                summary['skipped'] += 1
                continue

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

            tja_url = self._build_url(relative_tja)
            dir_url = self._build_url(relative_tja.parent)
            if not dir_url.endswith('/'):
                dir_url += '/'

            audio_url = None
            music_type = None
            audio_hash = None
            audio_mtime_ns = None
            audio_size = None
            if audio_path:
                relative_audio = audio_path.resolve().relative_to(self._songs_root)
                audio_url = self._build_url(relative_audio)
                music_type = audio_path.suffix.lower().lstrip('.')
                audio_bytes = audio_path.read_bytes()
                audio_hash = md5_bytes(audio_bytes)
                audio_stat = audio_path.stat()
                audio_mtime_ns = getattr(audio_stat, 'st_mtime_ns', int(audio_stat.st_mtime * 1_000_000_000))
                audio_size = audio_stat.st_size

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

            fallback_title = _clean_metadata_value(tja_path.stem)
            if not fallback_title:
                fallback_title = UNKNOWN_VALUE

            title_value = (parsed.title or "").strip()
            if not title_value:
                title_value = fallback_title
            if not title_value:
                title_value = UNKNOWN_VALUE

            subtitle_value = (parsed.subtitle or "").strip()
            if not subtitle_value:
                subtitle_value = UNKNOWN_VALUE

            title_ja_value = (parsed.title_ja or "").strip() or None
            subtitle_ja_value = (parsed.subtitle_ja or "").strip() or None

            locale_doc: Dict[str, Dict[str, str]] = {
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
            document = {
                'title': title_value,
                'titleJa': title_ja_value,
                'title_lang': {
                    'ja': title_ja_value or title_value,
                    'en': None,
                    'cn': None,
                    'tw': None,
                    'ko': None,
                },
                'subtitle': subtitle_value,
                'subtitleJa': subtitle_ja_value,
                'subtitle_lang': {
                    'ja': subtitle_ja_value or subtitle_value,
                    'en': None,
                    'cn': None,
                    'tw': None,
                    'ko': None,
                },
                'locale': locale_doc,
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
                'managed_by_scanner': True,
            }

            new_id: Optional[int] = None
            existing = self.db.songs.find_one({'hash': file_hash})
            if not existing:
                existing = self.db.songs.find_one({'paths.tja_url': tja_url})

            if existing:
                document.pop('_id', None)
                document['id'] = existing['id']
                document['order'] = existing.get('order', existing['id'])
                self.db.songs.update_one({'id': existing['id']}, {'$set': document})
                summary['updated'] += 1
                seen_song_ids.add(existing['id'])
            else:
                new_id = self._get_next_song_id()
                document['id'] = new_id
                document['order'] = new_id
                try:
                    self.db.songs.insert_one(document)
                except Exception as exc:  # pragma: no cover - exercised with real MongoDB
                    handled = False
                    if DuplicateKeyError and isinstance(exc, DuplicateKeyError):
                        fallback = self.db.songs.find_one({'hash': file_hash}) or self.db.songs.find_one({'paths.tja_url': tja_url})
                        if fallback:
                            document.pop('_id', None)
                            document['id'] = fallback['id']
                            document['order'] = fallback.get('order', fallback['id'])
                            self.db.songs.update_one({'id': fallback['id']}, {'$set': document})
                            summary['updated'] += 1
                            seen_song_ids.add(fallback['id'])
                            handled = True
                            existing = fallback
                            new_id = None
                        else:
                            LOGGER.warning("Duplicate key when inserting %s but no existing song was found", tja_path)
                            summary['errors'] += 1
                            handled = True
                    elif PyMongoError and isinstance(exc, PyMongoError):
                        LOGGER.exception("Failed to insert song %s", tja_path)
                        summary['errors'] += 1
                        handled = True

                    if not handled:
                        raise

            if new_id is not None:
                summary['inserted'] += 1
                seen_song_ids.add(new_id)

            try:
                if self._state_collection is not None:
                    state_payload = {
                        'tja_path': tja_key,
                        'tja_hash': file_hash,
                        'tja_mtime_ns': tja_mtime_ns,
                        'tja_size': tja_size,
                        'audio_path': audio_path.resolve().relative_to(self._songs_root).as_posix() if audio_path else None,
                        'audio_hash': audio_hash,
                        'audio_mtime_ns': audio_mtime_ns,
                        'audio_size': audio_size,
                        'song_id': document['id'],
                    }
                    if state_doc:
                        self._state_collection.update_one({'tja_path': tja_key}, {'$set': state_payload}, upsert=True)
                    else:
                        self._state_collection.insert_one(state_payload)
            except Exception:  # pragma: no cover - state updates are best effort
                LOGGER.debug('Failed to update song scanner state for %s', tja_path)

        self._update_sequence()

        if self._state_collection is not None:
            stale_paths = set(state_docs.keys()) - seen_state_paths
            if stale_paths:
                try:
                    self._state_collection.delete_many({'tja_path': {'$in': list(stale_paths)}})
                except Exception:  # pragma: no cover - best effort cleanup
                    LOGGER.debug('Failed to prune %d stale scanner state entries', len(stale_paths))

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
