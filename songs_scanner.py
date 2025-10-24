"""Song scanning and parsing utilities for Taiko Web."""
from __future__ import annotations

import contextlib
import json
import os
import fnmatch
import hashlib
import logging
import random
import re
import signal
import socket
import sys
import threading
import time
import traceback
from fractions import Fraction
from datetime import UTC, datetime
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    cast,
)
from urllib.parse import unquote, urlparse
import unicodedata

from pymongo.database import Database

from storage.interfaces import LeaderLock, ManifestStore, SongStore

try:  # pragma: no cover - pymongo always available in production
    from pymongo import ReturnDocument, UpdateOne
    from pymongo.errors import DuplicateKeyError, PyMongoError, WriteError
except Exception:  # pragma: no cover - fallback when pymongo unavailable
    class _ReturnDocumentFallback:
        BEFORE = 0
        AFTER = 1

    ReturnDocument = _ReturnDocumentFallback()  # type: ignore[assignment]
    class _UpdateOneFallback:  # pragma: no cover - simple shim for tests
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    UpdateOne = _UpdateOneFallback  # type: ignore[assignment]
    DuplicateKeyError = None  # type: ignore[assignment]
    PyMongoError = None  # type: ignore[assignment]
    WriteError = None  # type: ignore[assignment]


try:  # pragma: no cover - watchdog is optional during tests
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except Exception:  # pragma: no cover - watchdog optional dependency
    FileSystemEventHandler = None  # type: ignore[assignment]
    Observer = None  # type: ignore[assignment]


try:  # pragma: no cover - config import may fail in some test contexts
    from config.config import SCAN_LOG_LEVEL, SCAN_LOG_SUMMARY
except Exception:  # pragma: no cover - fall back to safe defaults
    SCAN_LOG_LEVEL, SCAN_LOG_SUMMARY = "INFO", True


def _resolve_log_level(level_name: str) -> int:
    level_value = getattr(logging, str(level_name).upper(), logging.INFO)
    return level_value if isinstance(level_value, int) else logging.INFO


# Scanner logging respects ``SCAN_LOG_LEVEL`` and ``SCAN_LOG_SUMMARY``. ``DEBUG``
# emits detailed per-file diagnostics without sensitive payloads, while ``INFO``
# restricts output to aggregate markers suitable for production telemetry.
LOGGER = logging.getLogger("taiko.scanner")
SUMMARY_LOGGER = logging.getLogger("taiko.scanner.summary")
LOGGER.setLevel(_resolve_log_level(SCAN_LOG_LEVEL))
SUMMARY_LOGGER.setLevel(_resolve_log_level(SCAN_LOG_LEVEL))
LOGGER.propagate = True
SUMMARY_LOGGER.propagate = True
logging.Logger.manager.loggerDict[__name__] = LOGGER


TJA_LENIENT_FALLBACK = os.getenv("TJA_LENIENT_FALLBACK", "1") == "1"


def _parse_bool_env(value: str) -> bool:
    token = value.strip().lower()
    return token not in {"0", "false", "no", "off"}


CATALOG_ASSUME_VALID = _parse_bool_env(os.getenv("CATALOG_ASSUME_VALID", "0"))


VALIDATION_ERROR_ISSUE = "strict-validation-error"


from tja_validator import get_tja_validator


if TYPE_CHECKING:  # pragma: no cover - optional Redis dependency
    from redis import Redis


TJA_VALIDATOR = get_tja_validator()


_HANG_WATCHDOG_ARMED = False


LEADER_LOCK_TTL_SECONDS = 300


def compute_fs_digest(root: Path, *, ignore_globs: Optional[Iterable[str]] = None) -> Tuple[str, int]:
    """Return a checksum and file count for ``root`` without reading file bodies."""

    try:
        root_path = Path(root).resolve()
    except FileNotFoundError:
        root_path = Path(root)

    if not root_path.exists():
        return hashlib.sha1(b"").hexdigest(), 0

    ignore_patterns = [pattern for pattern in (ignore_globs or []) if pattern]

    def _ignored(relative_path: Path) -> bool:
        if not ignore_patterns:
            return False
        relative_text = relative_path.as_posix()
        return any(fnmatch.fnmatch(relative_text, pattern) for pattern in ignore_patterns)

    hasher = hashlib.sha1()
    files = 0

    for dirpath, dirnames, filenames in os.walk(root_path):
        dirnames.sort()
        filenames.sort()
        base = Path(dirpath)

        filtered_dirs: List[str] = []
        for dirname in dirnames:
            candidate_dir = base / dirname
            try:
                relative_dir = candidate_dir.relative_to(root_path)
            except ValueError:
                continue
            if _ignored(relative_dir):
                continue
            filtered_dirs.append(dirname)
        dirnames[:] = filtered_dirs

        for name in filenames:
            candidate = base / name
            try:
                if candidate.is_symlink():
                    continue
                stat_result = candidate.stat()
            except FileNotFoundError:
                continue
            except OSError:
                LOGGER.debug("Failed to stat %s during digest", candidate, exc_info=True)
                continue
            try:
                relative = candidate.relative_to(root_path)
            except ValueError:
                LOGGER.debug("Skipping file outside root during digest: %s", candidate)
                continue
            if _ignored(relative):
                continue
            mtime_ns = getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000))
            payload = f"{relative.as_posix()}\0{stat_result.st_size}\0{mtime_ns}\n"
            hasher.update(payload.encode("utf-8", "surrogateescape"))
            files += 1

    return hasher.hexdigest(), files


def _coerce_int(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return int(token)
        except ValueError:
            with contextlib.suppress(ValueError):
                return int(float(token))
    return None


class TTLRefresher(contextlib.AbstractContextManager["TTLRefresher"]):
    """Background TTL refresher for ``LeaderLock`` implementations."""

    def __init__(
        self,
        lock: LeaderLock,
        token: str,
        ttl: int,
        *,
        period: int = 60,
        on_release: Optional[Callable[[], None]] = None,
    ) -> None:
        self.lock = lock
        self.token = token
        self.ttl = ttl
        self.period = period
        self._on_release = on_release
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None

    def __enter__(self) -> "TTLRefresher":
        if self.ttl > 0 and self.period > 0:
            self._worker = threading.Thread(
                target=self._refresh_loop,
                name='scanner-lock-ttl-refresh',
                daemon=True,
            )
            self._worker.start()
        return self

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(self.period):
            try:
                if self.lock.refresh(self.token, self.ttl):
                    continue
            except Exception:  # pragma: no cover - ttl refresh best effort
                LOGGER.debug('Failed to refresh scanner leader lock ttl', exc_info=True)
                break
            owner: Optional[str]
            try:
                owner = self.lock.get_owner()
            except Exception:  # pragma: no cover - best effort owner lookup
                LOGGER.debug('Failed to read scanner leader lock owner during refresh', exc_info=True)
                owner = None
            LOGGER.warning(
                'scanner leader lock refresh lost: owner=%s token=%s',
                owner or '<unknown>',
                self.token,
            )
            break

    def stop(self) -> None:
        self._stop_event.set()
        worker = self._worker
        if worker is not None:
            worker.join(timeout=max(float(self.period), 1.0))
            self._worker = None

    def __exit__(self, exc_type, exc, tb) -> Optional[bool]:
        self.stop()
        try:
            self.lock.release(self.token)
        except Exception:  # pragma: no cover - release best effort
            LOGGER.exception('Leader lock release failed')
        finally:
            if self._on_release is not None:
                with contextlib.suppress(Exception):
                    self._on_release()
        return None


def _validation_warning(message: str, *args, **kwargs) -> None:
    if TJA_VALIDATOR.mode == "off":
        return
    LOGGER.warning(message, *args, **kwargs)


def _validation_info(message: str, *args, **kwargs) -> None:
    if TJA_VALIDATOR.mode == "off":
        return
    LOGGER.info(message, *args, **kwargs)


def _dump_stacktrace(signum, frame) -> None:  # pragma: no cover - diagnostic helper
    LOGGER.error(
        "scanner hang watchdog triggered: pid=%d signal=%s",
        os.getpid(),
        signum,
    )
    try:
        current_frames = sys._current_frames()
    except Exception:
        current_frames = {}
    thread_names = {thread.ident: thread.name for thread in threading.enumerate()}
    for ident, thread_frame in current_frames.items():
        thread_name = thread_names.get(ident, "<unknown>")
        stack = "".join(traceback.format_stack(thread_frame))
        LOGGER.error(
            "scanner hang watchdog stack: thread=%s ident=%s\n%s",
            thread_name,
            ident,
            stack,
        )
    if hasattr(signal, "SIGALRM"):
        try:
            signal.alarm(60)
        except Exception:
            LOGGER.debug("failed to re-arm scanner hang watchdog", exc_info=True)


def _maybe_enable_hang_watchdog() -> None:
    global _HANG_WATCHDOG_ARMED
    if _HANG_WATCHDOG_ARMED:
        return
    if not os.getenv("DEBUG_SCANNER_HANG"):
        return
    if not hasattr(signal, "SIGALRM"):
        LOGGER.warning("scanner hang watchdog unavailable: SIGALRM unsupported")
        return
    try:
        signal.signal(signal.SIGALRM, _dump_stacktrace)
        signal.alarm(60)
    except Exception:
        LOGGER.warning("failed to arm scanner hang watchdog", exc_info=True)
        return
    _HANG_WATCHDOG_ARMED = True
    LOGGER.info("scanner hang watchdog armed timeout=60s")

SUPPORTED_AUDIO_EXTS = [
    ".ogg",
    ".mp3",
    ".wav",
    ".m4a",
    ".aac",
    ".flac",
    ".opus",
    ".t3u8",
]

COURSE_ALIASES = {
    "EASY": "easy",
    "KANTAN": "easy",
    "AMAKUCHI": "easy",
    "甘口": "easy",
    "NORMAL": "normal",
    "FUTSUU": "normal",
    "FUTSU": "normal",
    "KARAKUCHI": "normal",
    "辛口": "normal",
    "HARD": "hard",
    "MUZUKASHII": "hard",
    "ONI": "oni",
    "EDIT": "oni",
    "URAONI": "uraoni",
    "URA": "uraoni",
}

COURSE_ORDER = ["easy", "normal", "hard", "oni", "uraoni"]

COURSE_NUMERIC_MAP = {
    0: "easy",
    1: "normal",
    2: "hard",
    3: "oni",
    4: "uraoni",
}

EASY_TASTE_MARKERS = {"ama", "amakuchi", "甘口"}
NORMAL_TASTE_MARKERS = {"kara", "karakuchi", "辛口"}
TASTE_MARKER_SPLIT_RE = re.compile(r"[\s._\-()\[\]]+")

COURSE_LEGACY_MAP = {
    "easy": "easy",
    "normal": "normal",
    "hard": "hard",
    "oni": "oni",
    "uraoni": "ura",
}

DEFAULT_CATEGORY_TITLE = "Unsorted"
UNKNOWN_VALUE = "Unknown"

ENCODINGS = ["utf-8-sig", "utf-16", "utf-8", "shift_jis", "cp932", "latin-1"]

NOTE_TOKEN_CLEAN_RE = re.compile(r"[^0-9]")
NOTE_LINE_RE = re.compile(r"^[0-9,\s\|]+$")

DIR_NUMERIC_PREFIX_RE = re.compile(r"^\s*(\d+)\s*[-_.]?\s*(.*)$")
LEADING_ZERO_TOKEN_RE = re.compile(r"\b0+\d+\b")

SAFE_NOTE_DIRECTIVES = {"#BPMCHANGE", "#MEASURE", "#SCROLL"}

HEADER_KEYS = {
    "TITLE",
    "TITLEJA",
    "SUBTITLE",
    "SUBTITLEJA",
    "BPM",
    "WAVE",
    "OFFSET",
    "DEMOSTART",
    "PREVIEW",
    "LIFE",
    "COURSE",
    "LEVEL",
    "SCOREINIT",
    "SCOREDIFF",
    "GENRE",
    "MAKER",
    "SONGID",
    "BALLOON",
    "BALLOONNOR",
    "BALLOONHARD",
    "BALLOONEX",
}

HIT_NOTE_TYPE_MAP = {
    1: "don",
    2: "ka",
    3: "don",
    4: "ka",
}

HIT_NOTE_VALUES = set(HIT_NOTE_TYPE_MAP.keys())

LONG_NOTE_START_MAP = {
    5: {"kind": "drumroll", "big": False},
    6: {"kind": "drumroll", "big": True},
    7: {"kind": "balloon", "big": False},
    9: {"kind": "balloon", "big": True},
}

LONG_NOTE_END_TOKEN = 8

# Charts that rely on long-note visualisation in the front-end despite lacking hit notes.
SYNTHETIC_NOTE_COURSE_TOKENS = {"TOWER"}


def _resolve_long_end(entry: Dict[str, object], candidate_end: int) -> int:
    """Ensure that a long note entry is terminated at or after its start time."""

    try:
        start_at = int(entry.get('at'))
    except (TypeError, ValueError):
        start_at = None
    if start_at is not None:
        candidate_end = max(candidate_end, start_at + 1)
    entry['end_at'] = candidate_end
    return candidate_end


def _finalise_chart_metrics(
    measures: Sequence[Dict[str, object]],
    *,
    course_label: str,
    mode: str,
    log_result: bool = True,
) -> ChartMetrics:
    total_notes = 0
    hit_notes = 0
    total_longs = 0
    existing_synthetic = 0
    latest_note_at = 0
    latest_long_at = 0
    measures_list = [measure for measure in measures if isinstance(measure, dict)]

    for measure in measures_list:
        notes_list = measure.get('notes')
        if isinstance(notes_list, list):
            for note in notes_list:
                if not isinstance(note, dict):
                    continue
                at_value = note.get('at')
                try:
                    at_int = int(at_value)
                except (TypeError, ValueError):
                    continue
                latest_note_at = max(latest_note_at, at_int)
                total_notes += 1
                if note.get('synthetic'):
                    existing_synthetic += 1
                else:
                    hit_notes += 1
        longs_list = measure.get('longs')
        if isinstance(longs_list, list):
            for long_note in longs_list:
                if not isinstance(long_note, dict):
                    continue
                at_value = long_note.get('at')
                try:
                    at_int = int(at_value)
                except (TypeError, ValueError):
                    continue
                total_longs += 1
                end_value = long_note.get('end_at')
                candidate = None
                try:
                    if end_value is not None:
                        candidate = int(end_value)
                except (TypeError, ValueError):
                    candidate = None
                if candidate is None:
                    candidate = at_int
                latest_long_at = max(latest_long_at, candidate)

    synthetic_injected = 0
    if hit_notes == 0 and total_longs > 0 and existing_synthetic == 0:
        for measure in measures_list:
            longs_list = measure.get('longs')
            if not isinstance(longs_list, list) or not longs_list:
                continue
            notes_list = measure.get('notes')
            if not isinstance(notes_list, list):
                notes_list = []
                measure['notes'] = notes_list
            for long_note in longs_list:
                if not isinstance(long_note, dict):
                    continue
                at_value = long_note.get('at')
                try:
                    at_int = int(at_value)
                except (TypeError, ValueError):
                    continue
                notes_list.append({'type': 'don', 'at': at_int, 'synthetic': True})
                synthetic_injected += 1
                latest_note_at = max(latest_note_at, at_int)
        total_notes += synthetic_injected

    duration_ms = max(latest_note_at, latest_long_at)
    metrics = ChartMetrics(
        total_notes=total_notes,
        hit_notes=hit_notes,
        total_longs=total_longs,
        duration_ms=duration_ms,
        measures=len(measures_list),
        synthetic_injected=synthetic_injected,
    )

    if log_result:
        LOGGER.info(
            "end-notes(%s): course=%s measures=%d notes=%d longs=%d duration_ms=%d",
            mode,
            course_label,
            metrics.measures,
            metrics.total_notes,
            metrics.total_longs,
            metrics.duration_ms,
        )
        if metrics.synthetic_injected:
            LOGGER.info(
                "synth-notes: course=%s injected=%d",
                course_label,
                metrics.synthetic_injected,
            )

    return metrics


def _normalised_requires_synthetic_notes(
    normalised: Optional[str], *, mode: Optional[str] = None
) -> bool:
    if mode == "dojo":
        return True
    token = (normalised or "").upper()
    return token in SYNTHETIC_NOTE_COURSE_TOKENS

# NB: "DAN" is intentionally downcast via COURSE_DOWNCAST_MAP so that dojo packs
# can be scanned in MVP mode without full exam support.
DOJO_COURSE_TOKENS = {"DOJO", "KYUU"}

COURSE_DOWNCAST_MAP = {
    "TOWER": "oni",
    "DAN": "oni",
}

ZERO_WIDTH_CHARACTERS = {
    "\u200b",  # zero width space
    "\u200c",  # zero width non-joiner
    "\u200d",  # zero width joiner
    "\ufeff",  # zero width no-break space / BOM
    "\u2060",  # word joiner
    "\u180e",  # mongolian vowel separator
}


_GROUP_KEY_SLASH_RE = re.compile(r"/+")
_GROUP_KEY_SPACE_RE = re.compile(r"\s+")


def _normalise_group_text(value: Optional[str], *, casefold_value: bool, strip_slashes: bool = False) -> str:
    text = value or ""
    if not text:
        return ""
    text = unquote(text)
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\\", "/")
    text = _GROUP_KEY_SLASH_RE.sub("/", text)
    if strip_slashes:
        text = text.strip("/")
    text = text.strip()
    text = _GROUP_KEY_SPACE_RE.sub(" ", text)
    text = _clean_metadata_value(text)
    if casefold_value:
        text = text.casefold()
    return text


def _sanitise_group_token(token: str, *, fallback: str = "_") -> str:
    cleaned = token.replace(":", "_").strip()
    cleaned = _GROUP_KEY_SPACE_RE.sub(" ", cleaned)
    return cleaned or fallback


def _folder_token_from_record(record: "TjaImportRecord") -> str:
    folder_source = ""
    if record.dir_url:
        try:
            parsed = urlparse(record.dir_url)
        except Exception:
            parsed = None
        else:
            folder_source = parsed.path or ""
    if not folder_source:
        folder_source = record.relative_dir or ""
    if not folder_source and record.relative_path:
        folder_source = Path(record.relative_path).parent.as_posix()
    normalised = _normalise_group_text(folder_source, casefold_value=True, strip_slashes=True)
    if not normalised or normalised == ".":
        normalised = ""
    first_segment = normalised.split("/", 1)[0] if normalised else ""
    relative_normalised = _normalise_group_text(record.relative_dir, casefold_value=True, strip_slashes=True)
    relative_first = relative_normalised.split("/", 1)[0] if relative_normalised and relative_normalised != "." else ""
    if relative_first and first_segment != relative_first:
        if not first_segment or f"/{relative_first}" in normalised or normalised.endswith(relative_first):
            first_segment = relative_first
    token = _sanitise_group_token(first_segment, fallback="_root")
    return token or "_root"


def _stable_path_hash(record: "TjaImportRecord") -> str:
    relative_dir = _normalise_group_text(record.relative_dir, casefold_value=True, strip_slashes=True)
    relative_path = _normalise_group_text(record.relative_path, casefold_value=True)
    components = [component for component in (relative_dir, relative_path) if component]
    combined = "/".join(components)
    if not combined:
        combined = record.relative_path or record.relative_dir or record.tja_hash or record.fingerprint or "missing"
    return md5_text(combined)


def _normalise_song_fs_path(path: Optional[str]) -> str:
    if not path:
        return ""
    normalised = path.replace("\\", "/").strip()
    normalised = re.sub(r"/+", "/", normalised)
    return normalised.casefold()


def _normalise_song_id(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "null":
        return None
    return text


def _make_deterministic_song_id(parts: Sequence[str]) -> str:
    payload = "|".join(parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _select_primary_chart_entry(charts: Sequence[Dict[str, object]]) -> Optional[Dict[str, object]]:
    if not charts:
        return None
    preference = ["oni", "ura", "hard", "normal", "easy"]

    def _rank(chart: Dict[str, object]) -> Tuple[int, str, str]:
        course = str(chart.get('canonical_course') or chart.get('course') or "")
        lowered = course.casefold()
        try:
            index = preference.index(lowered)
        except ValueError:
            index = len(preference)
        path = str(chart.get('tja_path') or "")
        return (index, lowered, path)

    return min(charts, key=_rank)


def compute_group_key(record: "TjaImportRecord") -> str:
    """Return the deterministic group key for a TJA import record."""

    folder_token = _folder_token_from_record(record)

    if record.audio_hash:
        audio_token = _normalise_group_text(record.audio_hash, casefold_value=False)
        audio_token = _sanitise_group_token(audio_token, fallback="missing-hash")
        return f"audio:{audio_token}:{folder_token}"

    fallback_title = record.normalized_title or _normalise_title_key(record.title)
    title_token = _normalise_group_text(fallback_title, casefold_value=True)
    title_token = _sanitise_group_token(title_token, fallback="untitled")
    stable_hash = _stable_path_hash(record)
    return f"missing:{folder_token}:{title_token}:{stable_hash}"


@dataclass
class CourseInfo:
    canonical: str
    raw_name: str
    normalised: str
    mode: str = "standard"
    display_course: Optional[str] = None
    segments: List[Dict[str, object]] = field(default_factory=list)
    unknown_directives: int = 0
    stars: Optional[int] = None
    branch: bool = False
    branch_sections: Set[str] = field(default_factory=set)
    start_blocks: int = 0
    end_blocks: int = 0
    issues: List[str] = field(default_factory=list)
    hit_notes: int = 0
    total_notes: int = 0
    measures: int = 0
    first_note_preview: Optional[str] = None
    chart_data: Optional[Dict[str, object]] = None

    def add_issue(self, issue: str) -> None:
        if issue not in self.issues:
            self.issues.append(issue)

    @property
    def notes_count(self) -> int:
        return self.total_notes


def _course_log_label(course: CourseInfo) -> str:
    if isinstance(course.display_course, str) and course.display_course:
        return course.display_course
    return course.canonical


def _course_requires_synthetic_notes(course: CourseInfo) -> bool:
    return _normalised_requires_synthetic_notes(course.normalised, mode=course.mode)


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
    unknown_directives: int = 0
    has_dojo_course: bool = False
    skipped_charts: int = 0
    mapped_courses: int = 0
    skipped_no_course: int = 0
    skipped_unknown_course: int = 0
    charts: Dict[str, CourseInfo] = field(default_factory=dict)
    implicit_end_due_to_header: int = 0


@dataclass
class ChartRecord:
    course: str
    raw_course: str
    normalised: str
    level: Optional[int]
    branch: bool
    valid: bool
    issues: List[str]
    mode: str = "standard"
    display_course: Optional[str] = None
    segments: List[Dict[str, object]] = field(default_factory=list)
    unknown_directives: int = 0
    coerced: bool = False
    hit_notes: int = 0
    total_notes: int = 0
    measures: int = 0
    first_note_preview: Optional[str] = None
    rank: Optional[str] = None
    chart_data: Optional[Dict[str, object]] = None


@dataclass
class ChartMetrics:
    total_notes: int
    hit_notes: int
    total_longs: int
    duration_ms: int
    measures: int
    synthetic_injected: int = 0


@dataclass
class _CourseParseState:
    measure_index: int = 0
    segments: List[Dict[str, object]] = field(default_factory=list)
    current_segment: Optional[Dict[str, object]] = None
    gogo_start: Optional[int] = None
    pending_total_notes: int = 0
    pending_hit_notes: int = 0
    pending_has_notes: bool = False
    block_line_count: int = 0
    block_note_count: int = 0
    chart_measures: List[Dict[str, object]] = field(default_factory=list)
    measure_tokens: List[str] = field(default_factory=list)
    current_bpm: Optional[float] = None
    default_bpm: Optional[float] = None
    current_scroll: float = 1.0
    current_measure_ratio: float = 1.0
    measure_bpm: Optional[float] = None
    measure_scroll: float = 1.0
    measure_ratio_for_measure: float = 1.0
    measure_start_time_ms: float = 0.0
    current_measure_numerator: int = 4
    current_measure_denominator: int = 4
    measure_time_sig_numerator: int = 4
    measure_time_sig_denominator: int = 4
    parse_failed: bool = False
    unknown_tokens_logged: Set[str] = field(default_factory=set)
    active_long: Optional[Dict[str, object]] = None
    final_metrics: Optional[ChartMetrics] = None
    offset_applied: bool = False


def _clone_chart_data(chart: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
    if not isinstance(chart, dict):
        return None
    course_value = chart.get('course')
    total_notes_value = chart.get('total_notes', 0)
    try:
        total_notes_int = int(total_notes_value)
    except (TypeError, ValueError):
        total_notes_int = 0
    duration_value = chart.get('duration_ms', 0)
    try:
        duration_int = int(duration_value)
    except (TypeError, ValueError):
        duration_int = 0
    measures_payload: List[Dict[str, object]] = []
    measures_source = chart.get('measures')
    if isinstance(measures_source, list):
        for measure in measures_source:
            if not isinstance(measure, dict):
                continue
            measure_copy: Dict[str, object] = {}
            notes_source = measure.get('notes')
            notes_copy: List[Dict[str, object]] = []
            if isinstance(notes_source, list):
                for note in notes_source:
                    if not isinstance(note, dict):
                        continue
                    note_type = note.get('type')
                    at_value = note.get('at')
                    try:
                        at_int = int(at_value)
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(note_type, str):
                        continue
                    note_entry: Dict[str, object] = {'type': note_type, 'at': at_int}
                    if note.get('synthetic'):
                        note_entry['synthetic'] = True
                    notes_copy.append(note_entry)
            measure_copy['notes'] = notes_copy
            longs_source = measure.get('longs')
            longs_copy: List[Dict[str, object]] = []
            if isinstance(longs_source, list):
                for long_note in longs_source:
                    if not isinstance(long_note, dict):
                        continue
                    kind = long_note.get('kind')
                    at_value = long_note.get('at')
                    end_value = long_note.get('end_at')
                    try:
                        at_int = int(at_value)
                    except (TypeError, ValueError):
                        continue
                    end_int: Optional[int]
                    try:
                        end_int = int(end_value) if end_value is not None else None
                    except (TypeError, ValueError):
                        end_int = None
                    if not isinstance(kind, str):
                        continue
                    long_entry: Dict[str, object] = {'kind': kind, 'at': at_int}
                    long_entry['big'] = bool(long_note.get('big', False))
                    if end_int is not None:
                        long_entry['end_at'] = end_int
                    longs_copy.append(long_entry)
            if longs_copy:
                measure_copy['longs'] = longs_copy
            if 'bpm' in measure:
                measure_copy['bpm'] = measure['bpm']
            if 'scroll' in measure:
                measure_copy['scroll'] = measure['scroll']
            if 'start_ms' in measure:
                measure_copy['start_ms'] = measure['start_ms']
            if 'duration_ms' in measure:
                measure_copy['duration_ms'] = measure['duration_ms']
            if 'ratio' in measure:
                measure_copy['ratio'] = measure['ratio']
            if 'time_sig' in measure:
                measure_copy['time_sig'] = measure['time_sig']
            measures_payload.append(measure_copy)
    return {
        'course': course_value,
        'total_notes': total_notes_int,
        'measures': measures_payload,
        'duration_ms': duration_int,
    }


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
    pack: Optional[str] = None


def _normalise_newlines(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines)


def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _strip_inline_comments(value: str, *, allow_without_whitespace: bool = False) -> str:
    """Remove inline // and ; comments from a line of text."""

    comment_markers = ("//", ";")
    lowest_index: Optional[int] = None
    for marker in comment_markers:
        search_start = 0
        while True:
            index = value.find(marker, search_start)
            if index == -1:
                break
            if index == 0:
                should_strip = True
            elif allow_without_whitespace:
                should_strip = True
            else:
                previous = value[index - 1]
                should_strip = previous.isspace()
            if should_strip:
                if lowest_index is None or index < lowest_index:
                    lowest_index = index
                break
            search_start = index + len(marker)
    if lowest_index is None:
        return value
    return value[:lowest_index]


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
    text = unicodedata.normalize("NFC", text.lstrip("\ufeff"))
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


def _split_numeric_prefix(value: Optional[str]) -> Tuple[Optional[int], str]:
    if value is None:
        return (None, "")
    cleaned = _clean_metadata_value(str(value))
    trimmed = cleaned.strip()
    if not trimmed:
        return (None, "")
    match = DIR_NUMERIC_PREFIX_RE.match(trimmed)
    if match:
        remainder = match.group(2).strip()
        if remainder:
            try:
                return int(match.group(1)), remainder
            except ValueError:
                pass
    return (None, trimmed)


def _strip_leading_zero_tokens(value: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        try:
            return str(int(token))
        except ValueError:
            return token

    return LEADING_ZERO_TOKEN_RE.sub(_replace, value)


def _normalise_space_runs(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _resolve_category_mode(category_title: Optional[str]) -> str:
    if not isinstance(category_title, str):
        return "standard"
    lowered = category_title.strip().casefold()
    if lowered == "taiko towers":
        return "tower"
    if lowered == "dan dojo":
        return "dandojo"
    return "standard"


def _normalise_course_token(value: str) -> str:
    token = re.sub(r"[\s\-_]", "", value.upper())
    return token


def _detect_taste_marker(path: Path) -> Optional[str]:
    tokens: Set[str] = set()
    for part in path.parts:
        lowered = part.casefold()
        if lowered:
            tokens.add(lowered)
            tokens.update(token for token in TASTE_MARKER_SPLIT_RE.split(lowered) if token)
    for token in tokens:
        if token in EASY_TASTE_MARKERS:
            return "easy"
    for token in tokens:
        if token in NORMAL_TASTE_MARKERS:
            return "normal"
    return None


def _resolve_course(value: str, *, path: Optional[Path] = None) -> Tuple[str, str, Optional[str]]:
    token = _normalise_course_token(value)
    canonical: Optional[str]
    issue: Optional[str] = None

    downcast = COURSE_DOWNCAST_MAP.get(token)
    if downcast:
        canonical = downcast
        issue = "mapped-course"
    else:
        canonical = COURSE_ALIASES.get(token)
        if canonical is None and path is not None:
            taste = _detect_taste_marker(path)
            if taste:
                canonical = taste

    if canonical is None and token.isdigit():
        try:
            numeric = int(token)
        except ValueError:
            numeric = None
        if numeric is not None and numeric in COURSE_NUMERIC_MAP:
            canonical = COURSE_NUMERIC_MAP[numeric]
        else:
            issue = "unknown_course_numeric"

    return (canonical or "Unknown", token, issue)


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


def _parse_tja_strict(
    path: Path,
    *,
    original_text: str,
    normalised_text: str,
) -> ParsedTJA:
    parsed = ParsedTJA(raw_text=original_text, fingerprint=md5_text(normalised_text))

    validator = TJA_VALIDATOR

    def _report_validation(code: str, course: CourseInfo, *, token: Optional[str] = None) -> None:
        if validator.report(code, path=path, course=course.canonical, token=token):
            course.add_issue(VALIDATION_ERROR_ISSUE)

    offset_seconds = parsed.offset if parsed.offset is not None else 0.0
    try:
        offset_ms = float(offset_seconds) * -1000.0
    except (TypeError, ValueError):
        offset_ms = 0.0

    active_course: Optional[CourseInfo] = None
    known_courses: Dict[str, CourseInfo] = {}
    current_notes_course: Optional[CourseInfo] = None
    parsing_notes = False
    current_wave: Optional[str] = None
    course_states: Dict[int, _CourseParseState] = {}

    def _parse_float(value: str) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _state_for(course: CourseInfo) -> _CourseParseState:
        state = course_states.get(id(course))
        if state is None:
            state = _CourseParseState()
            course_states[id(course)] = state
        return state

    def _reset_measure_buffers(course: CourseInfo) -> None:
        state = _state_for(course)
        state.measure_tokens.clear()
        state.pending_total_notes = 0
        state.pending_hit_notes = 0
        state.pending_has_notes = False
        state.block_line_count = 0
        state.block_note_count = 0
        state.measure_bpm = state.current_bpm
        state.measure_scroll = state.current_scroll
        state.measure_ratio_for_measure = state.current_measure_ratio
        state.measure_time_sig_numerator = state.current_measure_numerator
        state.measure_time_sig_denominator = state.current_measure_denominator
        state.active_long = None

    def _store_measure_entry(course: CourseInfo) -> None:
        state = _state_for(course)
        if not state.measure_tokens:
            state.measure_bpm = state.current_bpm
            state.measure_scroll = state.current_scroll
            state.measure_ratio_for_measure = state.current_measure_ratio
            return
        tokens = list(state.measure_tokens)
        state.measure_tokens.clear()
        ratio_value = state.measure_ratio_for_measure if state.measure_ratio_for_measure and state.measure_ratio_for_measure > 0 else 1.0
        bpm_basis = state.measure_bpm if state.measure_bpm is not None else state.current_bpm
        if bpm_basis is None or bpm_basis <= 0:
            if state.default_bpm and state.default_bpm > 0:
                bpm_basis = state.default_bpm
            else:
                bpm_basis = 120.0
        duration_ms = (240000.0 / bpm_basis) * ratio_value
        total_slots = len(tokens)
        notes: List[Dict[str, object]] = []
        longs: List[Dict[str, object]] = []

        def _close_active_long(at_value: int) -> None:
            if state.active_long and isinstance(state.active_long.get('entry'), dict):
                entry_ref = state.active_long['entry']
                _resolve_long_end(entry_ref, at_value)
                state.active_long = None
            else:
                _report_validation('end_without_start', course)

        for index, token in enumerate(tokens):
            if not token:
                continue
            try:
                note_value = int(token)
            except ValueError:
                continue
            position = (index / total_slots) if total_slots else 0.0
            at_value = int(round(state.measure_start_time_ms + position * duration_ms))

            if note_value in HIT_NOTE_VALUES:
                note_type = HIT_NOTE_TYPE_MAP.get(note_value)
                if note_type:
                    notes.append({'type': note_type, 'at': at_value})
                continue

            if note_value in LONG_NOTE_START_MAP:
                long_spec = LONG_NOTE_START_MAP[note_value]
                if state.active_long and isinstance(state.active_long.get('entry'), dict):
                    _report_validation('overlap_start', course, token=token)
                    entry_ref = state.active_long['entry']
                    _resolve_long_end(entry_ref, at_value)
                    state.active_long = None
                long_entry = {
                    'kind': long_spec['kind'],
                    'big': long_spec['big'],
                    'at': at_value,
                }
                longs.append(long_entry)
                state.active_long = {'entry': long_entry}
                continue

            if note_value == LONG_NOTE_END_TOKEN:
                _close_active_long(at_value)
                continue

            if note_value == 0:
                continue

            if token not in state.unknown_tokens_logged:
                state.unknown_tokens_logged.add(token)
                _report_validation('unknown_note_token', course, token=token)

        entry: Dict[str, object] = {'notes': notes}
        if longs:
            entry['longs'] = longs
        entry['bpm'] = bpm_basis
        if state.measure_scroll is not None:
            entry['scroll'] = state.measure_scroll
        start_int = int(round(state.measure_start_time_ms))
        duration_int = int(round(duration_ms))
        if duration_int < 0:
            duration_int = 0
        entry['start_ms'] = start_int
        entry['duration_ms'] = duration_int
        entry['ratio'] = ratio_value
        time_sig_num = state.measure_time_sig_numerator or 4
        time_sig_den = state.measure_time_sig_denominator or 4
        try:
            time_sig_num_int = int(round(time_sig_num))
        except (TypeError, ValueError):
            time_sig_num_int = 4
        try:
            time_sig_den_int = int(round(time_sig_den))
        except (TypeError, ValueError):
            time_sig_den_int = 4
        if time_sig_num_int <= 0:
            time_sig_num_int = 4
        if time_sig_den_int <= 0:
            time_sig_den_int = 4
        if not (time_sig_num_int == 4 and time_sig_den_int == 4):
            entry['time_sig'] = {'num': time_sig_num_int, 'den': time_sig_den_int}
        state.chart_measures.append(entry)
        state.measure_start_time_ms += duration_ms
        state.measure_bpm = state.current_bpm
        state.measure_scroll = state.current_scroll
        state.measure_ratio_for_measure = state.current_measure_ratio

    def _mark_course_failed(course: CourseInfo) -> None:
        state = _state_for(course)
        state.parse_failed = True
        state.chart_measures.clear()
        state.measure_tokens.clear()
        state.measure_start_time_ms = offset_ms
        state.offset_applied = True
        state.pending_total_notes = 0
        state.pending_hit_notes = 0
        state.pending_has_notes = False
        state.active_long = None
        course.total_notes = 0
        course.hit_notes = 0
        course.measures = 0
        course.add_issue("strict-parse-failed")

    def _current_audio() -> Optional[str]:
        return current_wave if current_wave is not None else parsed.wave

    def _close_gogo(course: CourseInfo, *, end_measure: Optional[int] = None) -> None:
        state = _state_for(course)
        if state.gogo_start is None:
            return
        segment = state.current_segment
        if segment is None:
            state.gogo_start = None
            return
        end_value = state.measure_index if end_measure is None else end_measure
        if end_value < state.gogo_start:
            end_value = state.gogo_start
        ranges = segment.setdefault('gogo_ranges', [])
        ranges.append({'start': state.gogo_start, 'end': end_value})
        state.gogo_start = None

    def _start_segment(course: CourseInfo, audio: Optional[str]) -> None:
        state = _state_for(course)
        segment = {
            'audio': audio,
            'start_measure': state.measure_index,
            'end_measure': None,
            'bpm_map': [],
            'gogo_ranges': [],
        }
        state.current_segment = segment
        state.segments.append(segment)

    def _end_segment(course: CourseInfo) -> None:
        state = _state_for(course)
        if state.current_segment is None:
            return
        _close_gogo(course)
        if state.current_segment.get('end_measure') is None:
            state.current_segment['end_measure'] = state.measure_index
        state.current_segment = None

    def _reset_pending_notes(course: CourseInfo) -> None:
        state = _state_for(course)
        state.pending_total_notes = 0
        state.pending_hit_notes = 0
        state.pending_has_notes = False

    def _commit_pending_measure(course: CourseInfo) -> None:
        state = _state_for(course)
        _store_measure_entry(course)
        if state.pending_has_notes:
            course.total_notes += state.pending_total_notes
            course.hit_notes += state.pending_hit_notes
        if course.mode == "dojo":
            if state.current_segment is None:
                _start_segment(course, _current_audio())
            state.measure_index += 1
        else:
            state.measure_index += 1
        _reset_pending_notes(course)

    def _flush_pending_notes(course: CourseInfo) -> None:
        _commit_pending_measure(course)

    def _finalise_notes_block() -> None:
        nonlocal parsing_notes, current_notes_course
        if current_notes_course:
            state = _state_for(current_notes_course)
            _flush_pending_notes(current_notes_course)
            if state.active_long and isinstance(state.active_long.get('entry'), dict):
                entry_ref = state.active_long['entry']
                last_tick = int(round(state.measure_start_time_ms))
                _resolve_long_end(entry_ref, last_tick)
                state.active_long = None
            measures = state.chart_measures

            metrics = _finalise_chart_metrics(
                measures,
                course_label=_course_log_label(current_notes_course),
                mode='strict',
            )
            current_notes_course.total_notes = metrics.total_notes
            current_notes_course.hit_notes = metrics.hit_notes
            current_notes_course.measures = metrics.measures
            state.final_metrics = metrics
            state.block_line_count = 0
            state.block_note_count = 0
            if current_notes_course.mode == "dojo":
                _end_segment(current_notes_course)
        parsing_notes = False
        current_notes_course = None

    def _process_metadata_line(metadata_line: str) -> None:
        nonlocal active_course, current_notes_course, parsing_notes, current_wave
        if ":" not in metadata_line:
            return
        key, value = metadata_line.split(":", 1)
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
            clean_wave = clean_value or None
            if not parsing_notes:
                parsed.wave = clean_wave
            current_wave = clean_wave
            if parsing_notes and current_notes_course and current_notes_course.mode == "dojo":
                _end_segment(current_notes_course)
        elif key_upper == "GENRE":
            parsed.genre = clean_value or None
        elif key_upper == "SONGID":
            parsed.song_id = clean_value or None
        elif key_upper == "BPM":
            bpm_value = _parse_float(value_stripped)
            if bpm_value is not None:
                if active_course:
                    state = _state_for(active_course)
                    state.current_bpm = bpm_value
                    if state.default_bpm is None:
                        state.default_bpm = bpm_value
                        state.measure_bpm = bpm_value
                else:
                    for existing_course in parsed.courses:
                        state = _state_for(existing_course)
                        if state.default_bpm is None:
                            state.default_bpm = bpm_value
        elif key_upper == "COURSE":
            raw_course_value = value_stripped.strip()
            raw_course_lower = raw_course_value.casefold()
            special_mode: Optional[str] = None
            special_display: Optional[str] = None
            canonical_override: Optional[str] = None

            if raw_course_lower.startswith("tower"):
                canonical_override = "oni"
                special_mode = "tower"
                special_display = "tower"
            elif raw_course_lower.startswith("dan"):
                canonical_override = "oni"
                special_mode = "dan"
                special_display = "dan"

            normalised_token = _normalise_course_token(raw_course_value)
            if normalised_token in DOJO_COURSE_TOKENS:
                canonical_lower = "dojo"
                active_course = CourseInfo(
                    canonical=canonical_lower,
                    raw_name=raw_course_value,
                    normalised=normalised_token,
                    mode="dojo",
                )
                parsed.courses.append(active_course)
                parsed.has_dojo_course = True
                parsed.charts[canonical_lower] = active_course
            else:
                canonical, token, issue = _resolve_course(raw_course_value, path=path)
                if canonical_override:
                    canonical = canonical_override
                if canonical == "Unknown":
                    if issue == "unknown_course_numeric":
                        _validation_warning('Unknown numeric COURSE "%s" → skip chart block', raw_course_value)
                    else:
                        _validation_warning('Unknown COURSE "%s" → skip chart block', raw_course_value)
                    parsed.skipped_charts += 1
                    parsed.skipped_unknown_course += 1
                    active_course = None
                    current_notes_course = None
                    parsing_notes = False
                else:
                    if issue == "mapped-course":
                        _validation_info(
                            "mapped-course(parser): %s→%s",
                            raw_course_value.upper(),
                            canonical.upper(),
                        )
                        parsed.mapped_courses += 1
                    canonical_lower = canonical.casefold()
                    existing = known_courses.get(canonical_lower)
                    if existing:
                        active_course = existing
                        active_course.raw_name = raw_course_value
                        active_course.normalised = token
                        if special_mode:
                            active_course.mode = special_mode
                            active_course.display_course = special_display
                        elif active_course.mode not in {"dojo"}:
                            active_course.mode = "standard"
                            if active_course.display_course in {"tower", "dan"}:
                                active_course.display_course = None
                    else:
                        active_course = CourseInfo(
                            canonical=canonical_lower,
                            raw_name=raw_course_value,
                            normalised=token,
                            mode=special_mode or "standard",
                            display_course=special_display,
                        )
                        known_courses[canonical_lower] = active_course
                        parsed.courses.append(active_course)
                    parsed.charts[active_course.canonical] = active_course
                    if issue:
                        active_course.add_issue(issue)
                    if special_mode and active_course.mode != special_mode:
                        active_course.mode = special_mode
                    elif not special_mode and active_course.mode in {"tower", "dan"}:
                        active_course.mode = "standard"
                    if special_display and active_course.display_course != special_display:
                        active_course.display_course = special_display
                    elif not special_display and active_course.display_course in {"tower", "dan"}:
                        active_course.display_course = None
        elif key_upper == "LEVEL" and active_course:
            try:
                level_value = float(value_stripped)
            except ValueError:
                _validation_warning("Invalid LEVEL value '%s' in %s", value_stripped, path)
                active_course.add_issue("invalid-level")
                return
            level_int = int(round(level_value))
            clamped = max(1, min(10, level_int))
            if level_int != level_value:
                active_course.add_issue("level-non-integer")
            if clamped != level_int:
                _validation_warning(
                    "LEVEL value %s for course '%s' in %s out of range; clamped to %s",
                    value_stripped,
                    active_course.raw_name,
                    path,
                    clamped,
                )
                active_course.add_issue("level-out-of-range")
            active_course.stars = clamped

    line_number = 0
    for raw_line in normalised_text.splitlines():
        line_number += 1

        state_for_current: Optional[_CourseParseState] = None
        if parsing_notes and current_notes_course:
            state_for_current = _state_for(current_notes_course)

        raw_line = raw_line.lstrip("\ufeff")
        stripped_pre = raw_line.strip()
        if not stripped_pre:
            continue
        if stripped_pre.startswith("//") or stripped_pre.startswith(";"):
            continue

        base_header_line = _strip_inline_comments(
            raw_line, allow_without_whitespace=False
        ).strip()
        stripped_comments = _strip_inline_comments(
            raw_line, allow_without_whitespace=parsing_notes
        )
        line = stripped_comments.strip()
        if not line and not base_header_line:
            continue

        if (
            parsing_notes
            and current_notes_course
            and base_header_line
            and ":" in base_header_line
            and not base_header_line.startswith("#")
        ):
            key_candidate = base_header_line.split(":", 1)[0].strip().upper()
            if key_candidate in HEADER_KEYS:
                if key_candidate == "WAVE":
                    _process_metadata_line(base_header_line)
                    continue
                current_notes_course.end_blocks += 1
                _validation_warning(
                    'implicit-end: header "%s" inside notes; closing previous chart',
                    key_candidate,
                )
                parsed.implicit_end_due_to_header += 1
                _finalise_notes_block()
                _process_metadata_line(base_header_line)
                continue

        if line.startswith("#"):
            upper_line = line.upper()
            directive = upper_line.split(None, 1)[0]
            directive_payload = line[len(directive) :].strip()
            handled_directive = False
            if directive == "#START":
                if active_course:
                    course_key = active_course.canonical
                    _validation_info(
                        "start-notes(strict): course=%s file=%s line=%d",
                        course_key,
                        path,
                        line_number,
                    )
                    active_course.start_blocks += 1
                    current_notes_course = active_course
                    parsing_notes = True
                    _reset_pending_notes(current_notes_course)
                    state = _state_for(current_notes_course)
                    first_block = active_course.start_blocks == 1
                    if first_block:
                        current_notes_course.total_notes = 0
                        current_notes_course.hit_notes = 0
                        state.chart_measures.clear()
                    if first_block or not state.offset_applied:
                        state.measure_start_time_ms = offset_ms
                        state.offset_applied = True
                    state.parse_failed = False
                    if state.current_scroll is None:
                        state.current_scroll = 1.0
                    _reset_measure_buffers(current_notes_course)
                    state.block_line_count = 0
                    state.block_note_count = 0
                    if current_notes_course.mode == "dojo":
                        state.measure_index = 0
                        _end_segment(current_notes_course)
                        _start_segment(current_notes_course, _current_audio())
                    handled_directive = True
                else:
                    _validation_warning(
                        "skip notes: no course before #START (line %d)",
                        line_number,
                    )
                    parsed.skipped_no_course += 1
                    current_notes_course = None
                    parsing_notes = False
                    handled_directive = True
            elif active_course:
                if directive == "#END":
                    active_course.end_blocks += 1
                    _finalise_notes_block()
                    handled_directive = True
                elif directive.startswith("#BRANCH"):
                    active_course.branch = True
                    if directive.startswith("#BRANCHSTART"):
                        active_course.branch_sections.add("START")
                    handled_directive = True
                elif directive in {"#N", "#E", "#M"}:
                    active_course.branch_sections.add(directive[1:])
                    handled_directive = True
            if parsing_notes and current_notes_course:
                state = _state_for(current_notes_course)
                if directive == "#NEXTSONG":
                    _flush_pending_notes(current_notes_course)
                    if current_notes_course.mode == "dojo":
                        _end_segment(current_notes_course)
                    handled_directive = True
                elif directive == "#GOGOSTART":
                    if current_notes_course.mode == "dojo":
                        if state.current_segment is None:
                            _start_segment(current_notes_course, _current_audio())
                        state.gogo_start = state.measure_index
                    handled_directive = True
                elif directive == "#GOGOEND":
                    if current_notes_course.mode == "dojo":
                        _close_gogo(current_notes_course)
                    handled_directive = True
                elif directive == "#BPMCHANGE":
                    handled_directive = True
                    if state.measure_tokens:
                        _commit_pending_measure(current_notes_course)
                    bpm_value = None
                    if directive_payload:
                        try:
                            bpm_value = float(directive_payload.split()[0])
                        except ValueError:
                            bpm_value = None
                    if bpm_value is not None:
                        state.current_bpm = bpm_value
                        if state.default_bpm is None:
                            state.default_bpm = bpm_value
                        state.measure_bpm = state.current_bpm
                    if current_notes_course.mode == "dojo":
                        if state.current_segment is None:
                            _start_segment(current_notes_course, _current_audio())
                        if bpm_value is not None:
                            state.current_segment.setdefault('bpm_map', []).append(
                                {'measure': state.measure_index, 'value': bpm_value}
                            )
                elif directive == "#SCROLL":
                    handled_directive = True
                    if state.measure_tokens:
                        _commit_pending_measure(current_notes_course)
                    scroll_value = _parse_float(directive_payload.split()[0]) if directive_payload else None
                    if scroll_value is not None:
                        state.current_scroll = scroll_value
                        state.measure_scroll = scroll_value
                elif directive == "#MEASURE":
                    handled_directive = True
                    if state.measure_tokens:
                        _commit_pending_measure(current_notes_course)
                    fraction = directive_payload or ""
                    if "/" in fraction:
                        numerator_str, denominator_str = fraction.split("/", 1)
                        try:
                            numerator_fraction = Fraction(numerator_str.strip())
                            denominator_fraction = Fraction(denominator_str.strip())
                        except (ValueError, ZeroDivisionError):
                            numerator_fraction = None
                            denominator_fraction = None
                        if (
                            numerator_fraction is not None
                            and denominator_fraction is not None
                            and denominator_fraction != 0
                        ):
                            ratio_fraction = numerator_fraction / denominator_fraction
                            state.current_measure_ratio = float(ratio_fraction)
                            state.measure_ratio_for_measure = state.current_measure_ratio
                            if (
                                numerator_fraction.denominator == 1
                                and denominator_fraction.denominator == 1
                            ):
                                state.current_measure_numerator = numerator_fraction.numerator
                                state.current_measure_denominator = denominator_fraction.numerator
                            else:
                                simplified = ratio_fraction.limit_denominator(4096)
                                state.current_measure_numerator = simplified.numerator
                                state.current_measure_denominator = simplified.denominator
                            state.measure_time_sig_numerator = state.current_measure_numerator
                            state.measure_time_sig_denominator = state.current_measure_denominator
                elif directive in SAFE_NOTE_DIRECTIVES:
                    handled_directive = True
                elif directive.startswith("#EXAM"):
                    handled_directive = True
            if parsing_notes and current_notes_course and not handled_directive:
                current_notes_course.unknown_directives += 1
                parsed.unknown_directives += 1
            continue

        if parsing_notes and current_notes_course and ":" not in line:
            try:
                measure_line = stripped_comments.strip()
                if not measure_line:
                    continue
                if not NOTE_LINE_RE.match(measure_line):
                    continue
                tokens = stripped_comments.split(",")
                saw_digits = False
                state = _state_for(current_notes_course)
                state.block_line_count += 1
                for index, token in enumerate(tokens):
                    cleaned = NOTE_TOKEN_CLEAN_RE.sub("", token)
                    if cleaned:
                        saw_digits = True
                        if not state.measure_tokens:
                            state.measure_bpm = state.current_bpm
                            state.measure_scroll = state.current_scroll
                            state.measure_ratio_for_measure = state.current_measure_ratio
                        state.measure_tokens.extend(list(cleaned))
                        notes = [int(ch) for ch in cleaned]
                        hit_count = sum(1 for note in notes if note in HIT_NOTE_VALUES)
                        state.pending_total_notes += hit_count
                        state.pending_hit_notes += hit_count
                        if hit_count > 0:
                            state.pending_has_notes = True
                        state.block_note_count += hit_count
                    if index < len(tokens) - 1:
                        _commit_pending_measure(current_notes_course)
                if measure_line.endswith(","):
                    _commit_pending_measure(current_notes_course)
                if (
                    saw_digits
                    and current_notes_course.first_note_preview is None
                ):
                    preview = stripped_comments.strip()
                    if preview:
                        current_notes_course.first_note_preview = preview[:120]
                continue
            except Exception:
                LOGGER.error(
                    "strict-parse-failed: course=%s file=%s",
                    current_notes_course.canonical,
                    path,
                    exc_info=True,
                )
                _mark_course_failed(current_notes_course)
                _finalise_notes_block()
                continue

        if (
            not parsing_notes
            and base_header_line
            and ":" in base_header_line
            and not base_header_line.startswith("#")
        ):
            _process_metadata_line(base_header_line)
            continue

        if (
            parsing_notes
            and current_notes_course
            and base_header_line
            and ":" in base_header_line
            and not base_header_line.startswith("#")
        ):
            current_notes_course.add_issue("unknown-metadata")
            parsed.unknown_directives += 1


    if parsing_notes:
        _finalise_notes_block()

    for course in parsed.courses:
        state = course_states.get(id(course))
        measures_payload: List[Dict[str, object]] = []
        if state:
            if state.current_segment is not None:
                _end_segment(course)
            course.segments = state.segments

            if not state.parse_failed:
                for measure in state.chart_measures:
                    if not isinstance(measure, dict):
                        continue
                    entry: Dict[str, object] = {}
                    notes_source = measure.get('notes')
                    notes_payload: List[Dict[str, object]] = []
                    if isinstance(notes_source, list):
                        for note in notes_source:
                            if not isinstance(note, dict):
                                continue
                            note_type = note.get('type')
                            at_value = note.get('at')
                            try:
                                at_int = int(at_value)
                            except (TypeError, ValueError):
                                continue
                            if isinstance(note_type, str):
                                note_entry: Dict[str, object] = {'type': note_type, 'at': at_int}
                                if note.get('synthetic'):
                                    note_entry['synthetic'] = True
                                notes_payload.append(note_entry)
                    entry['notes'] = notes_payload
                    longs_source = measure.get('longs')
                    longs_payload: List[Dict[str, object]] = []
                    if isinstance(longs_source, list):
                        for long_note in longs_source:
                            if not isinstance(long_note, dict):
                                continue
                            kind = long_note.get('kind')
                            at_value = long_note.get('at')
                            end_value = long_note.get('end_at')
                            try:
                                at_int = int(at_value)
                            except (TypeError, ValueError):
                                continue
                            try:
                                end_int = int(end_value) if end_value is not None else None
                            except (TypeError, ValueError):
                                end_int = None
                            if not isinstance(kind, str):
                                continue
                            long_entry: Dict[str, object] = {'kind': kind, 'at': at_int}
                            long_entry['big'] = bool(long_note.get('big', False))
                            if end_int is not None:
                                long_entry['end_at'] = end_int
                            longs_payload.append(long_entry)
                    if longs_payload:
                        entry['longs'] = longs_payload
                    if 'bpm' in measure:
                        entry['bpm'] = measure['bpm']
                    if 'scroll' in measure:
                        entry['scroll'] = measure['scroll']
                    measures_payload.append(entry)
            if state.parse_failed:
                measures_payload = []
        else:
            measures_payload = []

        metrics = None
        if state and state.final_metrics:
            metrics = state.final_metrics
        else:
            metrics = _finalise_chart_metrics(
                measures_payload,
                course_label=_course_log_label(course),
                mode='strict',
                log_result=False,
            )
        course.total_notes = metrics.total_notes
        course.hit_notes = metrics.hit_notes
        course.measures = metrics.measures
        course.chart_data = {
            'course': course.canonical,
            'total_notes': metrics.total_notes,
            'measures': measures_payload,
            'duration_ms': metrics.duration_ms,
        }

        parsed.charts = {course.canonical: course for course in parsed.courses}

    return parsed


def _parse_tja_lenient(
    path: Path,
    *,
    original_text: str,
    normalised_text: str,
) -> ParsedTJA:
    parsed = ParsedTJA(raw_text=original_text, fingerprint=md5_text(normalised_text))

    offset_seconds = parsed.offset if parsed.offset is not None else 0.0
    try:
        offset_ms = float(offset_seconds) * -1000.0
    except (TypeError, ValueError):
        offset_ms = 0.0

    course_token = "oni"
    course_raw = "oni"
    in_notes = False
    chart_measures: List[Dict[str, object]] = []
    measure_tokens: List[str] = []
    current_bpm: Optional[float] = None
    default_bpm: Optional[float] = None
    current_scroll: float = 1.0
    current_measure_ratio: float = 1.0
    measure_bpm: Optional[float] = None
    measure_scroll: float = 1.0
    measure_ratio_for_measure: float = 1.0
    measure_start_time_ms: float = 0.0
    current_measure_numerator: int = 4
    current_measure_denominator: int = 4
    measure_time_sig_numerator: int = 4
    measure_time_sig_denominator: int = 4
    total_notes = 0
    unknown_tokens_logged: Set[str] = set()
    active_long: Optional[Dict[str, object]] = None
    best_chart_measures: List[Dict[str, object]] = []
    best_total_notes = -1
    best_metrics: Optional[ChartMetrics] = None
    best_course_token = course_token
    best_course_raw = course_raw
    offset_applied = False
    def _parse_float(value: str) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _measure_duration_ms(bpm_value: Optional[float], ratio: float) -> float:
        bpm = bpm_value if bpm_value and bpm_value > 0 else None
        if bpm is None and default_bpm and default_bpm > 0:
            bpm = default_bpm
        if bpm is None:
            bpm = 120.0
        if ratio <= 0:
            ratio = 1.0
        return (240000.0 / bpm) * ratio

    def _snapshot_measures(source: List[Dict[str, object]]) -> List[Dict[str, object]]:
        snapshot: List[Dict[str, object]] = []
        for measure in source:
            if not isinstance(measure, dict):
                continue
            entry: Dict[str, object] = {}
            notes_source = measure.get('notes')
            notes_copy: List[Dict[str, object]] = []
            if isinstance(notes_source, list):
                for note in notes_source:
                    if not isinstance(note, dict):
                        continue
                    note_type = note.get('type')
                    at_value = note.get('at')
                    try:
                        at_int = int(at_value)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(note_type, str):
                        note_entry: Dict[str, object] = {'type': note_type, 'at': at_int}
                        if note.get('synthetic'):
                            note_entry['synthetic'] = True
                        notes_copy.append(note_entry)
            entry['notes'] = notes_copy
            longs_source = measure.get('longs')
            longs_copy: List[Dict[str, object]] = []
            if isinstance(longs_source, list):
                for long_note in longs_source:
                    if not isinstance(long_note, dict):
                        continue
                    kind = long_note.get('kind')
                    at_value = long_note.get('at')
                    end_value = long_note.get('end_at')
                    try:
                        at_int = int(at_value)
                    except (TypeError, ValueError):
                        continue
                    end_int: Optional[int]
                    try:
                        end_int = int(end_value) if end_value is not None else None
                    except (TypeError, ValueError):
                        end_int = None
                    if not isinstance(kind, str):
                        continue
                    long_entry: Dict[str, object] = {'kind': kind, 'at': at_int}
                    long_entry['big'] = bool(long_note.get('big', False))
                    if end_int is not None:
                        long_entry['end_at'] = end_int
                    longs_copy.append(long_entry)
            if longs_copy:
                entry['longs'] = longs_copy
            if 'bpm' in measure:
                entry['bpm'] = measure['bpm']
            if 'scroll' in measure:
                entry['scroll'] = measure['scroll']
            if 'start_ms' in measure:
                entry['start_ms'] = measure['start_ms']
            if 'duration_ms' in measure:
                entry['duration_ms'] = measure['duration_ms']
            if 'ratio' in measure:
                entry['ratio'] = measure['ratio']
            if 'time_sig' in measure:
                entry['time_sig'] = measure['time_sig']
            snapshot.append(entry)
        return snapshot

    def _flush_measure() -> None:
        nonlocal measure_tokens, measure_bpm, measure_scroll, measure_ratio_for_measure
        nonlocal measure_start_time_ms, total_notes, active_long
        nonlocal measure_time_sig_numerator, measure_time_sig_denominator
        if not measure_tokens:
            return
        tokens = list(measure_tokens)
        measure_tokens.clear()
        duration_ms = _measure_duration_ms(measure_bpm, measure_ratio_for_measure)
        total_slots = len(tokens)
        measure_notes: List[Dict[str, object]] = []
        measure_longs: List[Dict[str, object]] = []

        def _close_active_long(at_value: int) -> None:
            nonlocal active_long
            if active_long and isinstance(active_long.get('entry'), dict):
                entry_ref = active_long['entry']
                _resolve_long_end(entry_ref, at_value)
                active_long = None
            else:
                _validation_warning('lenient-long-end-without-start: file=%s', path)

        for index, token in enumerate(tokens):
            if not token:
                continue
            if token.isdigit():
                try:
                    note_value = int(token)
                except ValueError:
                    continue
                position = (index / total_slots) if total_slots else 0.0
                at_value = int(round(measure_start_time_ms + position * duration_ms))
                if note_value in HIT_NOTE_VALUES:
                    note_type = HIT_NOTE_TYPE_MAP.get(note_value)
                    if note_type:
                        measure_notes.append({'type': note_type, 'at': at_value})
                    continue
                if note_value in LONG_NOTE_START_MAP:
                    long_spec = LONG_NOTE_START_MAP[note_value]
                    if active_long and isinstance(active_long.get('entry'), dict):
                        _validation_warning(
                            'lenient-long-start-overlap: file=%s token=%s',
                            path,
                            token,
                        )
                        entry_ref = active_long['entry']
                        _resolve_long_end(entry_ref, at_value)
                        active_long = None
                    long_entry = {
                        'kind': long_spec['kind'],
                        'big': long_spec['big'],
                        'at': at_value,
                    }
                    measure_longs.append(long_entry)
                    active_long = {'entry': long_entry}
                    continue
                if note_value == LONG_NOTE_END_TOKEN:
                    _close_active_long(at_value)
                    continue
                if note_value == 0:
                    continue
                if token not in unknown_tokens_logged:
                    unknown_tokens_logged.add(token)
                    _validation_warning(
                        'lenient-unknown-note-token: token=%s file=%s',
                        token,
                        path,
                    )
        measure_entry: Dict[str, object] = {'notes': measure_notes}
        if measure_longs:
            measure_entry['longs'] = measure_longs
        bpm_to_store = measure_bpm if measure_bpm is not None else current_bpm or default_bpm
        if bpm_to_store is not None:
            measure_entry['bpm'] = bpm_to_store
        if measure_scroll is not None:
            measure_entry['scroll'] = measure_scroll
        start_int = int(round(measure_start_time_ms))
        duration_int = int(round(duration_ms))
        if duration_int < 0:
            duration_int = 0
        measure_entry['start_ms'] = start_int
        measure_entry['duration_ms'] = duration_int
        measure_entry['ratio'] = measure_ratio_for_measure
        time_sig_num = measure_time_sig_numerator or 4
        time_sig_den = measure_time_sig_denominator or 4
        try:
            time_sig_num_int = int(round(time_sig_num))
        except (TypeError, ValueError):
            time_sig_num_int = 4
        try:
            time_sig_den_int = int(round(time_sig_den))
        except (TypeError, ValueError):
            time_sig_den_int = 4
        if time_sig_num_int <= 0:
            time_sig_num_int = 4
        if time_sig_den_int <= 0:
            time_sig_den_int = 4
        if not (time_sig_num_int == 4 and time_sig_den_int == 4):
            measure_entry['time_sig'] = {'num': time_sig_num_int, 'den': time_sig_den_int}
        chart_measures.append(measure_entry)
        total_notes += len(measure_notes)
        measure_start_time_ms += duration_ms
        measure_bpm = current_bpm
        measure_scroll = current_scroll
        measure_ratio_for_measure = current_measure_ratio
        measure_time_sig_numerator = current_measure_numerator
        measure_time_sig_denominator = current_measure_denominator

    for line_number, raw in enumerate(normalised_text.splitlines(), 1):
        stripped = raw.strip().lstrip("\ufeff")
        if not stripped or stripped.startswith("//") or stripped.startswith(";"):
            continue
        upper = stripped.upper()

        if not in_notes:
            if upper.startswith("COURSE:"):
                raw_value = stripped.split(":", 1)[1].strip()
                course_raw = raw_value or course_raw
                mapped = {"TOWER": "oni", "DAN": "oni"}.get(raw_value.upper())
                course_token = mapped or (raw_value.casefold() or course_token)
                continue
            if ":" in stripped and not stripped.startswith("#"):
                key, value = stripped.split(":", 1)
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
                elif key_upper == "WAVE":
                    parsed.wave = clean_value or None
                elif key_upper == "GENRE":
                    parsed.genre = clean_value or None
                elif key_upper == "SONGID":
                    parsed.song_id = clean_value or None
                elif key_upper == "BPM":
                    bpm_value = _parse_float(value_stripped)
                    if bpm_value is not None:
                        current_bpm = bpm_value
                        if default_bpm is None:
                            default_bpm = bpm_value
                continue

        if upper.startswith("#START"):
            _validation_info(
                "start-notes(lenient): course=%s file=%s line=%d",
                course_token,
                path,
                line_number,
            )
            in_notes = True
            measure_tokens.clear()
            chart_measures.clear()
            measure_start_time_ms = offset_ms
            measure_bpm = current_bpm
            measure_scroll = current_scroll
            measure_ratio_for_measure = current_measure_ratio
            measure_time_sig_numerator = current_measure_numerator
            measure_time_sig_denominator = current_measure_denominator
            offset_applied = True
            active_long = None
            continue

        if not in_notes:
            continue

        if upper.startswith("#END"):
            _flush_measure()
            if active_long and isinstance(active_long.get('entry'), dict):
                entry_ref = active_long['entry']
                last_tick = int(round(measure_start_time_ms))
                _resolve_long_end(entry_ref, last_tick)
                active_long = None
            metrics = _finalise_chart_metrics(
                chart_measures,
                course_label=course_token,
                mode='lenient',
            )
            if metrics.total_notes > best_total_notes:
                best_total_notes = metrics.total_notes
                best_chart_measures = _snapshot_measures(chart_measures)
                best_course_token = course_token
                best_course_raw = course_raw
                best_metrics = metrics
            in_notes = False
            measure_tokens = []
            chart_measures = []
            total_notes = 0
            measure_start_time_ms = 0.0
            measure_bpm = current_bpm
            measure_scroll = current_scroll
            measure_ratio_for_measure = current_measure_ratio
            active_long = None
            continue

        if upper.startswith("#BPMCHANGE"):
            if measure_tokens:
                _flush_measure()
            payload = stripped.split(None, 1)[-1] if " " in stripped else stripped[len("#BPMCHANGE") :]
            bpm_value = _parse_float(payload.strip())
            if bpm_value is not None:
                current_bpm = bpm_value
                if default_bpm is None:
                    default_bpm = bpm_value
                measure_bpm = current_bpm
            continue

        if upper.startswith("#SCROLL"):
            if measure_tokens:
                _flush_measure()
            payload = stripped.split(None, 1)[-1] if " " in stripped else stripped[len("#SCROLL") :]
            scroll_value = _parse_float(payload.strip())
            if scroll_value is not None:
                current_scroll = scroll_value
                measure_scroll = current_scroll
            continue

        if upper.startswith("#MEASURE"):
            if measure_tokens:
                _flush_measure()
            payload = stripped.split(None, 1)[-1] if " " in stripped else stripped[len("#MEASURE") :]
            fraction = payload.strip()
            if "/" in fraction:
                numerator_str, denominator_str = fraction.split("/", 1)
                try:
                    numerator_fraction = Fraction(numerator_str.strip())
                    denominator_fraction = Fraction(denominator_str.strip())
                except (ValueError, ZeroDivisionError):
                    numerator_fraction = None
                    denominator_fraction = None
                if (
                    numerator_fraction is not None
                    and denominator_fraction is not None
                    and denominator_fraction != 0
                ):
                    ratio_fraction = numerator_fraction / denominator_fraction
                    current_measure_ratio = float(ratio_fraction)
                    measure_ratio_for_measure = current_measure_ratio
                    if (
                        numerator_fraction.denominator == 1
                        and denominator_fraction.denominator == 1
                    ):
                        current_measure_numerator = numerator_fraction.numerator
                        current_measure_denominator = denominator_fraction.numerator
                    else:
                        simplified = ratio_fraction.limit_denominator(4096)
                        current_measure_numerator = simplified.numerator
                        current_measure_denominator = simplified.denominator
                    measure_time_sig_numerator = current_measure_numerator
                    measure_time_sig_denominator = current_measure_denominator
            continue

        if NOTE_LINE_RE.match(stripped):
            for ch in stripped:
                if ch.isdigit():
                    if not measure_tokens:
                        measure_bpm = current_bpm
                        measure_scroll = current_scroll
                        measure_ratio_for_measure = current_measure_ratio
                    measure_tokens.append(ch)
                elif ch == ",":
                    _flush_measure()
            continue

    if in_notes:
        _flush_measure()
        if active_long and isinstance(active_long.get('entry'), dict):
            entry_ref = active_long['entry']
            last_tick = int(round(measure_start_time_ms))
            _resolve_long_end(entry_ref, last_tick)
            active_long = None
        metrics = _finalise_chart_metrics(
            chart_measures,
            course_label=course_token,
            mode='lenient',
        )
        if metrics.total_notes > best_total_notes:
            best_total_notes = metrics.total_notes
            best_chart_measures = _snapshot_measures(chart_measures)
            best_course_token = course_token
            best_course_raw = course_raw
            best_metrics = metrics

    if best_total_notes >= 0:
        chart_measures = best_chart_measures
        total_notes = best_total_notes
        course_token = best_course_token
        course_raw = best_course_raw
        metrics = best_metrics
    else:
        metrics = None

    if metrics is None:
        metrics = _finalise_chart_metrics(
            chart_measures,
            course_label=course_token,
            mode='lenient',
            log_result=False,
        )

    total_notes = metrics.total_notes
    hit_total = metrics.hit_notes

    canonical = course_token.casefold() or "oni"
    canonical = COURSE_LEGACY_MAP.get(canonical, canonical)
    if canonical not in {"easy", "normal", "hard", "oni", "ura"}:
        canonical = "oni"
    normalised = _normalise_course_token(course_raw) if course_raw else canonical.upper()
    course_info = CourseInfo(
        canonical=canonical,
        raw_name=course_raw,
        normalised=normalised,
    )
    course_info.total_notes = total_notes
    course_info.hit_notes = hit_total
    course_info.measures = metrics.measures
    course_info.chart_data = {
        'course': canonical,
        'total_notes': total_notes,
        'measures': chart_measures,
        'duration_ms': metrics.duration_ms,
    }
    course_info.add_issue("lenient-fallback")
    parsed.courses.append(course_info)
    parsed.charts[canonical] = course_info

    raw_upper = (course_raw or "").strip().upper()
    if raw_upper in {"TOWER", "DAN"}:
        parsed.mapped_courses += 1

    return parsed


def parse_tja(path: Path) -> ParsedTJA:
    validator = TJA_VALIDATOR
    validator.register_file(path)
    original_text, normalised_text = read_tja(path)
    result: ParsedTJA
    try:
        try:
            parsed = _parse_tja_strict(
                path,
                original_text=original_text,
                normalised_text=normalised_text,
            )
        except Exception:
            LOGGER.error("strict-parse-crash: file=%s", path, exc_info=True)
            if TJA_LENIENT_FALLBACK:
                _validation_warning("lenient-trigger: file=%s reason=strict-crash", path)
                result = _parse_tja_lenient(
                    path,
                    original_text=original_text,
                    normalised_text=normalised_text,
                )
            else:
                _validation_warning(
                    "lenient-trigger: file=%s reason=strict-crash-disabled", path
                )
                result = ParsedTJA(
                    raw_text=original_text,
                    fingerprint=md5_text(normalised_text),
                )
        else:
            if not TJA_LENIENT_FALLBACK:
                result = parsed
            else:
                has_courses = bool(parsed.courses)
                has_valid_course = any(course.total_notes > 0 for course in parsed.courses)
                if has_valid_course or not has_courses:
                    result = parsed
                else:
                    _validation_warning("lenient-trigger: file=%s reason=no-valid-courses", path)
                    fallback = _parse_tja_lenient(
                        path,
                        original_text=original_text,
                        normalised_text=normalised_text,
                    )

                    if not fallback.courses:
                        result = parsed
                    else:
                        fallback_course = fallback.courses[0]
                        target_course: Optional[CourseInfo] = None
                        fallback_preference = ["oni", "hard", "normal", "easy", "ura"]
                        for preferred in fallback_preference:
                            for candidate in parsed.courses:
                                if candidate.canonical == preferred:
                                    target_course = candidate
                                    break
                            if target_course is not None:
                                break
                        if target_course is None:
                            target_course = parsed.courses[0]

                        chart_data_copy = _clone_chart_data(fallback_course.chart_data)
                        if chart_data_copy is None:
                            measures_source: List[Dict[str, object]] = []
                            fallback_duration = 0
                            if isinstance(fallback_course.chart_data, dict):
                                measures_candidate = fallback_course.chart_data.get('measures')
                                if isinstance(measures_candidate, list):
                                    measures_source = [
                                        measure.copy() if isinstance(measure, dict) else {}
                                        for measure in measures_candidate
                                    ]
                                duration_candidate = fallback_course.chart_data.get('duration_ms', 0)
                                try:
                                    fallback_duration = int(duration_candidate)
                                except (TypeError, ValueError):
                                    fallback_duration = 0
                            chart_data_copy = {
                                'course': target_course.canonical,
                                'total_notes': fallback_course.total_notes,
                                'measures': measures_source,
                                'duration_ms': fallback_duration,
                            }
                        else:
                            chart_data_copy['course'] = target_course.canonical
                            chart_data_copy['total_notes'] = fallback_course.total_notes
                        if 'measures' not in chart_data_copy or not isinstance(chart_data_copy['measures'], list):
                            chart_data_copy['measures'] = []
                        if 'duration_ms' not in chart_data_copy:
                            chart_data_copy['duration_ms'] = (
                                fallback_course.chart_data.get('duration_ms', 0)
                                if isinstance(fallback_course.chart_data, dict)
                                else 0
                            )

                        if fallback_course.total_notes > 0:
                            target_course.total_notes = fallback_course.total_notes
                            target_course.hit_notes = fallback_course.hit_notes
                            target_course.measures = fallback_course.measures
                        chart_data_copy['total_notes'] = target_course.total_notes
                        chart_data_copy['course'] = target_course.canonical
                        target_course.chart_data = chart_data_copy
                        fallback_metrics = _finalise_chart_metrics(
                            chart_data_copy.get('measures', []),
                            course_label=_course_log_label(target_course),
                            mode='lenient-fallback',
                        )
                        target_course.total_notes = fallback_metrics.total_notes
                        target_course.hit_notes = fallback_metrics.hit_notes
                        target_course.measures = fallback_metrics.measures
                        target_course.chart_data['total_notes'] = fallback_metrics.total_notes
                        target_course.chart_data['duration_ms'] = fallback_metrics.duration_ms
                        target_course.add_issue("lenient-fallback")
                        parsed.charts[target_course.canonical] = target_course
                        result = parsed
    finally:
        validator.finalize_file(path)

    return result


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
        redis_client: Optional["Redis"] = None,
        song_store: Optional[SongStore] = None,
        manifest_store: Optional[ManifestStore] = None,
        leader_lock: Optional[LeaderLock] = None,
    ) -> None:
        LOGGER.info("scanner worker online pid=%d", os.getpid())
        _maybe_enable_hang_watchdog()
        self.db = db
        raw_song_store = song_store if song_store is not None else getattr(db, 'songs', None)
        self._song_store: Optional[SongStore]
        if raw_song_store is not None:
            self._song_store = cast(SongStore, raw_song_store)
        else:
            self._song_store = None
        raw_manifest_store = manifest_store if manifest_store is not None else getattr(db, 'songs_manifest', None)
        self._manifest_store: Optional[ManifestStore]
        if raw_manifest_store is not None:
            self._manifest_store = cast(ManifestStore, raw_manifest_store)
        else:
            self._manifest_store = None
        self.songs_dir = songs_dir
        self._songs_root = songs_dir.resolve()
        self.songs_baseurl = songs_baseurl
        self.ignore_globs = list(ignore_globs or [])
        validated_redis: Optional["Redis"] = None
        if redis_client is not None:
            try:
                from redis import Redis as _Redis  # type: ignore
            except Exception:  # pragma: no cover - redis optional at runtime
                _Redis = None  # type: ignore[assignment]
            if _Redis is not None and isinstance(redis_client, _Redis):
                validated_redis = redis_client
            else:
                LOGGER.debug(
                    "scanner redis client rejected: type=%s",
                    type(redis_client).__name__,
                )
        self._redis: Optional["Redis"] = validated_redis
        self._coerce_unknown_course: Optional[str] = None
        if coerce_unknown_course:
            token = coerce_unknown_course.strip()
            if token:
                lowered = token.casefold()
                for canonical in COURSE_ORDER:
                    if canonical.casefold() == lowered or COURSE_LEGACY_MAP[canonical] == lowered:
                        self._coerce_unknown_course = canonical
                        break
        self._scan_lock = threading.Lock()
        self._group_locks: Dict[str, threading.Lock] = {}
        self._group_locks_guard = threading.Lock()
        self._state_collection = getattr(self.db, 'song_scanner_state', None)
        self._manifest_collection = getattr(self.db, 'songs_manifest', None)
        self._meta_collection = getattr(self.db, 'meta', None)
        self._manifest_checksum: Optional[str] = None
        self._active_summary: Optional[Dict[str, int]] = None
        self._active_refresher_stack: Optional[contextlib.ExitStack] = None
        self._leader_lock_token: Optional[str] = None
        self._leader_lock_key = 'taiko:scanner:leader'
        ttl_default = 300
        ttl_env = os.getenv('SCAN_LEADER_TTL_SECONDS')
        if ttl_env:
            with contextlib.suppress(ValueError):
                parsed_ttl = int(ttl_env)
                if parsed_ttl > 0:
                    ttl_default = parsed_ttl
        self._leader_lock_ttl = ttl_default
        self._leader_lock: Optional[LeaderLock] = leader_lock
        db_name = getattr(self.db, 'name', None)
        client = getattr(self.db, 'client', None)
        host_label: Optional[str] = None
        if client is not None:
            with contextlib.suppress(Exception):
                address = getattr(client, 'address', None)
                if isinstance(address, tuple) and len(address) == 2:
                    host_label = f"{address[0]}:{address[1]}"
        LOGGER.info(
            'scanner mongo target: db=%s host=%s',
            db_name or '<unknown>',
            host_label or '<unknown>',
        )
        if hasattr(self.db, 'command'):
            try:
                ping_result = self.db.command('ping')  # type: ignore[call-arg]
            except Exception:
                LOGGER.error('scanner mongo ping failed', exc_info=True)
            else:
                ok_value = ping_result.get('ok') if isinstance(ping_result, dict) else None
                LOGGER.info(
                    'scanner mongo ping ok: db=%s ok=%s',
                    db_name or '<unknown>',
                    ok_value,
                )
        ensure_indexes_lock_collection = getattr(self.db, 'admin_locks', None)
        ensure_indexes_owner = f"{os.getpid()}-{time.time()}"
        ensure_indexes_lock_timeout = 150.0
        ensure_indexes_poll_interval = 0.2
        ensure_indexes_target = 'songs_group_key_scanner_unique'
        required_song_indexes = {
            'songs_id_unique',
            ensure_indexes_target,
            'songs_scanner_stable_id_unique',
            'songs_group_key_lookup',
        }

        self._ensure_manifest_indexes()

        def _collection_index_names(collection, label: str) -> Optional[Set[str]]:
            try:
                names: Set[str] = set()
                for index in collection.list_indexes():
                    if not isinstance(index, dict):
                        continue
                    name = index.get('name')
                    if isinstance(name, str):
                        names.add(name)
                return names
            except Exception:  # pragma: no cover - tolerate transient list indexes errors
                LOGGER.debug('Failed to list indexes for collection %s', label, exc_info=True)
                return None

        def _ensure_state_unique_index() -> None:
            if self._state_collection is None:
                return
            try:
                self._state_collection.create_index('tja_path', unique=True)
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure unique index for song_scanner_state collection')

        def _index_present() -> bool:
            song_store = self._song_store
            if song_store is None:
                return False
            song_index_names = _collection_index_names(song_store, 'songs')
            if not song_index_names or not required_song_indexes.issubset(song_index_names):
                return False
            if self._state_collection is not None:
                state_index_names = _collection_index_names(self._state_collection, 'song_scanner_state')
                if state_index_names is None or 'tja_path_1' not in state_index_names:
                    return False
            counters = getattr(self.db, 'counters', None)
            if counters is not None:
                try:
                    if counters.count_documents({'_id': 'songs'}, limit=1) == 0:
                        return False
                except Exception:  # pragma: no cover - tolerate transient counter checks
                    LOGGER.debug('Failed to check songs counter readiness', exc_info=True)
                    return False
            return True

        def _run_index_migration() -> None:
            _ensure_state_unique_index()
            song_store = self._song_store
            if song_store is None:
                return
            try:
                song_store.drop_index('id_1')
            except Exception:  # pragma: no cover - tolerate legacy index absence
                pass
            try:
                song_store.drop_index('songs_id_unique')
            except Exception:  # pragma: no cover - tolerate missing index
                pass
            try:
                id_string_partial_filter = {'id': {'$type': 'string'}}
                song_store.create_index(
                    'id',
                    name='songs_id_unique',
                    unique=True,
                    partialFilterExpression=id_string_partial_filter,
                )
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure partial unique index for songs.id')
            try:
                song_store.drop_index('group_key_1')
            except Exception:  # pragma: no cover - tolerate legacy index absence
                pass
            try:
                song_store.drop_index('songs_group_key_unique')
            except Exception:
                pass
            scanner_stable_string_partial_filter = {'scanner_stable_id': {'$type': 'string'}}
            try:
                song_store.create_index(
                    [('group_key', 1), ('scanner_stable_id', 1)],
                    name=ensure_indexes_target,
                    unique=True,
                    partialFilterExpression=scanner_stable_string_partial_filter,
                )
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure compound unique index for songs group key')
            try:
                song_store.drop_index('songs_scanner_stable_unique')
            except Exception:  # pragma: no cover - tolerate legacy index absence
                pass
            try:
                song_store.create_index(
                    'scanner_stable_id',
                    name='songs_scanner_stable_id_unique',
                    unique=True,
                    partialFilterExpression=scanner_stable_string_partial_filter,
                )
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure unique index for scanner stable id')
            try:
                song_store.create_index(
                    'group_key',
                    name='songs_group_key_lookup',
                )
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure non-unique index for songs group key')
            try:
                counters = getattr(self.db, 'counters', None)
                if counters is not None:
                    counters.update_one(
                        {'_id': 'songs'},
                        {'$setOnInsert': {'seq': 0}},
                        upsert=True,
                    )
            except Exception:  # pragma: no cover - tolerate best effort counter initialisation
                LOGGER.debug('Failed to ensure songs counter document')

            song_store = self._song_store
            if song_store is not None:
                try:
                    song_store.update_many(
                        {
                            'source_type': 'dan_dojo',
                            '$or': [
                                {'valid_charts': {'$gt': 0}},
                                {'valid_chart_count': {'$gt': 0}},
                            ],
                        },
                        {'$set': {'is_playable': True}},
                    )
                except Exception:  # pragma: no cover - tolerate transient issues
                    LOGGER.debug(
                        'Failed to backfill dan dojo songs is_playable flag',
                        exc_info=True,
                    )

        self._run_index_migration = _run_index_migration

        lock_acquired = False
        if ensure_indexes_lock_collection is not None and hasattr(ensure_indexes_lock_collection, 'find_one_and_update'):
            try:
                lock_result = ensure_indexes_lock_collection.find_one_and_update(
                    {'_id': 'ensure_indexes'},
                    {'$setOnInsert': {'owner': ensure_indexes_owner, 'ts': time.time()}},
                    upsert=True,
                    return_document=ReturnDocument.BEFORE,
                )
            except Exception:  # pragma: no cover - tolerate lock acquisition failure
                LOGGER.debug('Failed to acquire ensure_indexes advisory lock', exc_info=True)
            else:
                if lock_result is None:
                    lock_acquired = True
                elif _index_present():
                    LOGGER.debug('Indexes already present; skipping ensure')
                else:
                    wait_started = time.time()
                    while time.time() - wait_started < ensure_indexes_lock_timeout:
                        if _index_present():
                            break
                        time.sleep(ensure_indexes_poll_interval)
                    if not _index_present():
                        try:
                            takeover_result = ensure_indexes_lock_collection.find_one_and_update(
                                {
                                    '_id': 'ensure_indexes',
                                    'ts': {'$lt': time.time() - ensure_indexes_lock_timeout},
                                },
                                {'$set': {'owner': ensure_indexes_owner, 'ts': time.time()}},
                                return_document=ReturnDocument.BEFORE,
                            )
                        except Exception:  # pragma: no cover - tolerate takeover failure
                            LOGGER.debug('Failed to steal stale ensure_indexes lock', exc_info=True)
                        else:
                            if takeover_result is not None:
                                lock_acquired = True
        if lock_acquired or ensure_indexes_lock_collection is None:
            _run_index_migration()
            if ensure_indexes_lock_collection is not None:
                try:
                    ensure_indexes_lock_collection.update_one(
                        {'_id': 'ensure_indexes', 'owner': ensure_indexes_owner},
                        {'$set': {'ts': time.time(), 'ready': True}},
                    )
                except Exception:  # pragma: no cover - tolerate failure to update lock metadata
                    LOGGER.debug('Failed to update ensure_indexes lock metadata', exc_info=True)
        else:
            if not _index_present():
                LOGGER.warning('songs_group_key_scanner_unique index still missing after waiting')
                _run_index_migration()
        LOGGER.info('init songs indexes ok')
        self._import_issues_collection = getattr(self.db, 'import_issues', None)
        if self._import_issues_collection is not None:
            try:
                self._import_issues_collection.create_index(
                    [('reason', 1), ('path', 1), ('course_raw', 1)],
                    unique=True,
                )
            except Exception:  # pragma: no cover - tolerate missing create_index
                LOGGER.debug('Failed to ensure unique index for import issues collection')
        self._watchdog_supported = Observer is not None and FileSystemEventHandler is not None
        self._metrics = _ScanMetrics()
        self._seed_legacy_scanner_ids()

    def _build_chart_records(
        self,
        parsed: ParsedTJA,
        tja_path: Path,
        *,
        category_mode: str = "standard",
    ) -> Tuple[List[ChartRecord], List[str]]:
        records: List[ChartRecord] = []
        import_issues: List[str] = []
        parts = list(tja_path.parts)

        def _compute_display_course(course: CourseInfo) -> Optional[str]:
            if course.display_course:
                return course.display_course
            candidates: List[str] = []
            for part in reversed(parts[:-1]):
                cleaned_part = _clean_metadata_value(part)
                if cleaned_part:
                    candidates.append(cleaned_part)
            metadata_candidates = [
                parsed.title,
                parsed.subtitle,
                parsed.title_ja,
                parsed.subtitle_ja,
            ]
            for value in metadata_candidates:
                if value:
                    candidates.append(value)
            seen: Set[str] = set()
            for candidate in candidates:
                cleaned_candidate = _clean_metadata_value(candidate)
                lowered = cleaned_candidate.casefold()
                if lowered in seen:
                    continue
                seen.add(lowered)
                if "dan" in lowered or "kyuu" in lowered:
                    return cleaned_candidate
            fallback = _clean_metadata_value(course.raw_name) if course.raw_name else None
            if fallback:
                return fallback
            normalised = _clean_metadata_value(course.normalised) if course.normalised else None
            return normalised

        category_mode = (category_mode or "standard").strip().casefold()

        for course in parsed.courses:
            course_name = course.canonical
            coerced = False
            issues = list(course.issues)
            mode = course.mode or "standard"

            if course_name == "Unknown" and mode == "standard":
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
            has_notes = course.total_notes > 0
            has_hits = course.hit_notes > 0
            needs_synthetic = _course_requires_synthetic_notes(course)

            if not has_notes:
                issues.append("empty-chart")
            elif mode == "standard" and not has_hits and not needs_synthetic:
                issues.append("empty-chart")
            if course.branch:
                required_sections = {"N", "E", "M"}
                if not required_sections.issubset(course.branch_sections):
                    issues.append("invalid-branch-sections")

            display_course = course.display_course
            if mode == "dojo":
                display_course = _compute_display_course(course)
                course.display_course = display_course

            if mode == "standard":
                level_value = course.stars if course.stars is not None else 0
                if course.stars is None:
                    issues.append("missing-level")
            else:
                level_value = course.stars if course.stars is not None else 0

            if mode == "standard":
                valid = (
                    course_name in COURSE_ORDER
                    and "missing-chart-content" not in issues
                    and "unknown-course" not in issues
                    and has_notes
                    and (has_hits or needs_synthetic)
                )
            else:
                valid = has_notes

            if course.branch and "invalid-branch-sections" in issues:
                valid = False

            if VALIDATION_ERROR_ISSUE in issues:
                valid = False

            if mode == "dojo":
                if not course.segments:
                    issues.append("dojo_no_segments")
                    valid = False

            segments_copy: List[Dict[str, object]] = []
            for segment in course.segments:
                segment_copy = {
                    'audio': segment.get('audio'),
                    'start_measure': segment.get('start_measure'),
                    'end_measure': segment.get('end_measure'),
                    'bpm_map': [dict(item) for item in segment.get('bpm_map', [])],
                    'gogo_ranges': [dict(item) for item in segment.get('gogo_ranges', [])],
                }
                segments_copy.append(segment_copy)

            chart_data_copy = _clone_chart_data(course.chart_data)

            output_mode = mode
            output_display_course = display_course
            rank_value: Optional[str] = None

            if category_mode == "tower":
                output_mode = "tower"
                output_display_course = "tower"
            elif category_mode == "dandojo":
                output_mode = "dandojo"
                output_display_course = "dandojo"
                rank_candidates: Sequence[Optional[str]] = (
                    display_course,
                    course.raw_name,
                    course.normalised,
                )
                for candidate in rank_candidates:
                    if not isinstance(candidate, str):
                        continue
                    cleaned_rank = _normalise_space_runs(_clean_metadata_value(candidate))
                    if cleaned_rank:
                        rank_value = cleaned_rank
                        break
            else:
                if mode in {"tower", "dan", "dojo"}:
                    output_mode = "standard"

            record = ChartRecord(
                course=course_name,
                raw_course=course.raw_name,
                normalised=course.normalised,
                level=level_value,
                branch=course.branch,
                valid=valid,
                issues=sorted(set(issues)),
                mode=output_mode,
                display_course=output_display_course,
                segments=segments_copy,
                unknown_directives=course.unknown_directives,
                coerced=coerced,
                hit_notes=course.hit_notes,
                total_notes=course.total_notes,
                measures=course.measures,
                first_note_preview=course.first_note_preview,
                rank=rank_value,
                chart_data=chart_data_copy,
            )
            LOGGER.debug(
                "course-mapped: title=%s raw=%s mode=%s display=%s total_notes=%d",
                parsed.title or UNKNOWN_VALUE,
                course.raw_name,
                record.mode,
                record.display_course,
                record.total_notes,
            )
            LOGGER.debug(
                "Chart summary mode=%s course=%s raw=%s notes=%d measures=%d first=\"%s\" issues=%s",
                record.mode,
                record.course,
                course.raw_name,
                record.total_notes,
                record.measures,
                (record.first_note_preview or ""),
                ",".join(record.issues),
            )
            records.append(record)

            if record.issues:
                import_issues.extend(record.issues)

        return records, sorted(set(import_issues))

    def _update_empty_chart_issues(self, relative_tja: Path, record: TjaImportRecord) -> None:
        if self._import_issues_collection is None:
            return
        path = relative_tja.as_posix()
        for chart in record.charts:
            course_label = chart.raw_course or chart.course
            filter_doc = {
                'reason': 'empty_chart',
                'path': path,
                'course_raw': course_label,
            }
            try:
                self._import_issues_collection.delete_many(filter_doc)
                if 'empty-chart' in chart.issues:
                    payload = dict(filter_doc)
                    if chart.first_note_preview:
                        payload['first_note_preview'] = chart.first_note_preview
                    self._import_issues_collection.insert_one(payload)
            except Exception:  # pragma: no cover - tolerate collection issues
                LOGGER.debug('Failed to record empty chart issue for %s (%s)', path, chart.raw_course)

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
        category_mode = _resolve_category_mode(category_title)
        charts, chart_issues = self._build_chart_records(
            parsed,
            tja_path,
            category_mode=category_mode,
        )
        import_issues = list(chart_issues)

        parts = list(relative_tja.parts)
        pack_dir: Optional[str] = None
        if len(parts) >= 3:
            pack_dir = parts[1]
        elif len(parts) == 2:
            parent = relative_tja.parent
            parent_name = parent.name
            if parent_name and parent_name != parts[0]:
                pack_dir = parent_name

        pack_value: Optional[str] = None
        pack_title: Optional[str] = None
        if pack_dir:
            pack_candidate = _clean_metadata_value(pack_dir)
            pack_candidate = _normalise_space_runs(pack_candidate) if pack_candidate else ""
            if pack_candidate:
                pack_value = pack_candidate
            _, pack_remainder = _split_numeric_prefix(pack_dir)
            if pack_remainder:
                pack_title = _normalise_space_runs(
                    _strip_leading_zero_tokens(pack_remainder)
                )

        fallback_title = pack_title or _clean_metadata_value(tja_path.stem)
        if fallback_title:
            fallback_title = _normalise_space_runs(_strip_leading_zero_tokens(fallback_title))
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
            pack=pack_value,
            charts=charts,
            import_issues=sorted(set(import_issues)),
            normalized_title=normalized_title,
        )
        self._update_empty_chart_issues(relative_tja, record)
        return record

    def _record_from_state(self, payload: Dict[str, object]) -> Optional[TjaImportRecord]:
        try:
            charts_raw = payload.get('charts') or []

            def _restore_course(item: Dict[str, object]) -> str:
                canonical = item.get('canonical_course')
                if isinstance(canonical, str) and canonical:
                    return canonical
                stored = str(item.get('course', ''))
                lowered = stored.casefold()
                for canonical_name, legacy in COURSE_LEGACY_MAP.items():
                    if lowered == legacy:
                        return canonical_name
                if lowered == UNKNOWN_VALUE.casefold():
                    return UNKNOWN_VALUE
                return stored or UNKNOWN_VALUE

            def _restore_rank(value: object) -> Optional[str]:
                if isinstance(value, str):
                    cleaned = _clean_metadata_value(value).strip()
                    return cleaned or None
                if isinstance(value, (int, float)):
                    return str(value)
                return None

            charts = [
                ChartRecord(
                    course=_restore_course(item),
                    raw_course=str(item.get('raw_course', '')),
                    normalised=str(item.get('normalised', '')),
                    level=int(item.get('level', 0)) if item.get('level') is not None else None,
                    branch=bool(item.get('branch', False)),
                    mode=str(item.get('mode', 'standard')),
                    display_course=item.get('display_course'),
                    segments=[dict(segment) for segment in item.get('segments', [])] if isinstance(item.get('segments'), list) else [],
                    unknown_directives=int(item.get('unknown_directives', 0)) if item.get('unknown_directives') is not None else 0,
                    valid=bool(item.get('valid', False)),
                    issues=list(item.get('issues', [])),
                    coerced=bool(item.get('coerced', False)),
                    hit_notes=int(item.get('hit_notes', 0)) if item.get('hit_notes') is not None else 0,
                    total_notes=int(item.get('total_notes', 0)) if item.get('total_notes') is not None else 0,
                    measures=int(item.get('measures', 0)) if item.get('measures') is not None else 0,
                    first_note_preview=item.get('first_note_preview'),
                    rank=_restore_rank(item.get('rank')),
                    chart_data=_clone_chart_data(item.get('chart_data')),
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
                pack=_clean_metadata_value(str(payload.get('pack'))) if isinstance(payload.get('pack'), str) and payload.get('pack').strip() else None,
                charts=charts,
                import_issues=list(payload.get('import_issues', [])),
                normalized_title=str(payload.get('normalized_title', '')),
            )
            return record
        except Exception:
            LOGGER.debug('Failed to reconstruct TJA record from state payload')
            return None

    def _determine_group_key(self, record: TjaImportRecord) -> str:
        return compute_group_key(record)

    @contextlib.contextmanager
    def _group_key_lock(self, key: str):
        if not key:
            yield
            return
        with self._group_locks_guard:
            lock = self._group_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._group_locks[key] = lock
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def _record_invalid_group_key(self, records: List[TjaImportRecord], key: Optional[str]) -> None:
        if self._import_issues_collection is None:
            return
        first_record = records[0] if records else None
        payload = {
            'reason': 'invalid_group_key',
            'group_key': key,
            'paths': [record.relative_path for record in records],
        }
        if first_record is not None:
            payload['path'] = first_record.relative_path
            payload['tja_url'] = first_record.tja_url
            payload['dir_url'] = first_record.dir_url
        try:
            self._import_issues_collection.insert_one(payload)
        except Exception:  # pragma: no cover - diagnostics must not crash the scanner
            LOGGER.debug('Failed to record invalid group key issue for %s', key)

    def _unsafe_upsert_song_document(
        self,
        key: str,
        records: List[TjaImportRecord],
        document: Dict[str, object],
        charts_payload: List[Dict[str, object]],
        dirty_groups: Set[str],
        summary: Dict[str, int],
    ) -> Optional[int]:
        if not isinstance(key, str) or not key:
            self._metrics.increment('invalid_group_key_total')
            self._record_invalid_group_key(records, key)
            summary['errors'] += 1
            return None

        base_document = {
            k: v for k, v in document.items() if k not in {'id', 'order', '_id', 'charts'}
        }
        base_document['group_key'] = key

        stable_song_id = None
        if isinstance(document.get('scanner_stable_id'), str):
            stable_song_id = document['scanner_stable_id'] or None
        if not stable_song_id:
            LOGGER.warning('Song document for %s missing stable id; skipping', key)
            summary['errors'] += 1
            return None

        now_utc = datetime.now(UTC)
        base_document['updated_at'] = now_utc

        insert_only_fields = {'title', 'group_key', 'scanner_stable_id'}
        mutable_fields = {'charts', 'summary', 'tags', 'last_scanned_at', 'metadata'}

        insert_document = {
            field: base_document[field]
            for field in insert_only_fields
            if field in base_document
        }
        insert_document['created_at'] = now_utc

        mutable_conflicts = mutable_fields.intersection(insert_document.keys())
        if mutable_conflicts:
            LOGGER.error(
                'insert-once-mutable-conflict: fields=%s key=%s',
                sorted(mutable_conflicts),
                key,
            )
            summary['errors'] += 1
            return None

        base_document = {
            k: v for k, v in base_document.items() if k not in insert_only_fields
        }

        insert_document.setdefault('scanner_stable_id', stable_song_id)

        song_store = self._song_store
        if song_store is None:
            LOGGER.error('Song store unavailable during upsert for %s', key)
            summary['errors'] += 1
            return None

        stable_group_filter: Dict[str, object] = {'scanner_stable_id': stable_song_id, 'group_key': key}
        stable_filter: Dict[str, object] = {'scanner_stable_id': stable_song_id}
        legacy_filter: Dict[str, object] = {'group_key': key, 'scanner_stable_id': {'$exists': False}}

        def _validate_update_doc(update: Dict[str, Dict[str, object]]) -> bool:
            if '$set' in update and '$setOnInsert' in update:
                dup = set(update['$set']).intersection(update['$setOnInsert'])
                if dup:
                    LOGGER.error(
                        'update-conflict-keys: fields=%s key=%s',
                        sorted(dup),
                        key,
                    )
                    return False
            return True

        update_existing = {'$set': base_document}
        legacy_update_existing = {'$set': dict(base_document)}
        legacy_update_existing['$set']['scanner_stable_id'] = stable_song_id
        update_insert = {'$setOnInsert': insert_document, '$set': base_document}

        if not _validate_update_doc(update_existing):
            summary['errors'] += 1
            return None
        if not _validate_update_doc(update_insert):
            summary['errors'] += 1
            return None

        result_doc: Optional[Dict[str, object]] = None
        final_mode = 'unknown'
        mode_detail: Optional[str] = None
        final_result = None
        result_doc_override: Optional[Dict[str, object]] = None

        class _SyntheticUpdateResult:
            __slots__ = ('matched_count', 'modified_count', 'upserted_id', 'acknowledged')

            def __init__(
                self,
                *,
                matched_count: int = 0,
                modified_count: int = 0,
                upserted_id: Optional[int] = None,
            ) -> None:
                self.matched_count = matched_count
                self.modified_count = modified_count
                self.upserted_id = upserted_id
                self.acknowledged = True

        def _log_duplicate(exc: Exception, attempt: int, phase: str) -> None:
            details: Dict[str, object] = {}
            exc_details = getattr(exc, 'details', None)
            if isinstance(exc_details, dict):
                details = {
                    'index': exc_details.get('indexName'),
                    'keyPattern': exc_details.get('keyPattern'),
                    'keyValue': exc_details.get('keyValue'),
                }
            payload = {
                'group_key': key,
                'stable_id': stable_song_id,
                'title': document.get('title'),
                'attempt': attempt + 1,
                'details': details,
            }
            LOGGER.error('Duplicate key during song %s update: %s', phase, payload)

        def _execute_update(
            filter_doc: Dict[str, object],
            update_doc: Dict[str, object],
            *,
            upsert: bool,
            phase: str,
            duplicate_retry_filter: Optional[Dict[str, object]] = None,
            duplicate_retry_update: Optional[Dict[str, object]] = None,
        ):
            nonlocal result_doc_override
            store = self._song_store
            if store is None:
                LOGGER.error('Song store unavailable during %s phase', phase)
                return None
            for attempt in range(3):
                try:
                    return store.update_one(filter_doc, update_doc, upsert=upsert)
                except Exception as exc:  # pragma: no cover - defensive around DB driver
                    if WriteError and isinstance(exc, WriteError):
                        if getattr(exc, 'code', None) == 40:
                            LOGGER.error(
                                "write-error-40: conflict at path; set=%s setOnInsert=%s",
                                list(update_doc.get('$set', {}).keys()),
                                list(update_doc.get('$setOnInsert', {}).keys()),
                                exc_info=True,
                            )
                            return None
                    if DuplicateKeyError and isinstance(exc, DuplicateKeyError):
                        self._metrics.increment('duplicate_key_retries_total')
                        _log_duplicate(exc, attempt, phase)
                        if duplicate_retry_filter is not None:
                            try:
                                retry_doc = store.find_one_and_update(
                                    duplicate_retry_filter,
                                    duplicate_retry_update or update_doc,
                                    upsert=False,
                                    return_document=getattr(ReturnDocument, 'AFTER', 1),
                                )
                            except Exception:  # pragma: no cover - tolerate retry lookup issues
                                retry_doc = None
                            else:
                                if isinstance(retry_doc, dict):
                                    result_doc_override = retry_doc
                                    return _SyntheticUpdateResult(matched_count=1, modified_count=1)
                        jitter = random.random() * 0.025
                        time.sleep(0.05 * (attempt + 1) + jitter)
                        continue
                    if PyMongoError and isinstance(exc, PyMongoError):
                        if attempt == 0:
                            LOGGER.warning(
                                'transient write error during %s phase; retrying',
                                phase,
                                exc_info=True,
                            )
                            jitter = random.random() * 0.025
                            time.sleep(0.05 + jitter)
                            continue
                    LOGGER.error(
                        'song update failed: phase=%s attempt=%d filter=%s',
                        phase,
                        attempt + 1,
                        filter_doc,
                        exc_info=True,
                    )
                    return None
            return None

        with self._group_key_lock(key):
            primary_result = _execute_update(stable_group_filter, update_existing, upsert=False, phase='new')
            if primary_result and getattr(primary_result, 'matched_count', 0):
                final_result = primary_result
                final_mode = 'new'
                mode_detail = 'stable-group'
            else:
                stable_result = _execute_update(stable_filter, update_existing, upsert=False, phase='stable')
                if stable_result and getattr(stable_result, 'matched_count', 0):
                    final_result = stable_result
                    final_mode = 'new'
                    mode_detail = 'stable-only'
                else:
                    legacy_result = _execute_update(
                        legacy_filter,
                        legacy_update_existing,
                        upsert=False,
                        phase='legacy',
                    )
                    if legacy_result and getattr(legacy_result, 'matched_count', 0):
                        final_result = legacy_result
                        final_mode = 'legacy'
                        mode_detail = 'legacy-group'
                    else:
                        final_result = _execute_update(
                            stable_group_filter,
                            update_insert,
                            upsert=True,
                            phase='insert',
                            duplicate_retry_filter=stable_group_filter,
                            duplicate_retry_update=update_existing,
                        )
                        if final_result is None:
                            LOGGER.warning("Failed to upsert aggregated song for %s", key)
                            summary['errors'] += 1
                            return None
                        if getattr(final_result, 'upserted_id', None) is not None:
                            final_mode = 'insert'
                            mode_detail = 'stable-group'
                        elif getattr(final_result, 'matched_count', 0):
                            final_mode = 'new'
                            mode_detail = 'stable-group-race'
                        else:
                            final_mode = 'insert'
                            mode_detail = 'stable-group'
            if result_doc_override is not None:
                result_doc = result_doc_override
            else:
                try:
                    result_doc = song_store.find_one({'scanner_stable_id': stable_song_id})
                except Exception:  # pragma: no cover - tolerate lookup issues
                    result_doc = None

        if not isinstance(result_doc, dict):
            LOGGER.warning("Failed to load song document for %s after upsert", key)
            summary['errors'] += 1
            return None

        self._metrics.increment('songs_upserted_total')
        if final_mode == 'new':
            self._metrics.increment('songs_upserted_new_total')
        elif final_mode == 'legacy':
            self._metrics.increment('songs_updated_legacy_total')
        elif final_mode == 'insert':
            self._metrics.increment('songs_inserted_total')

        LOGGER.info(
            'songs upsert result',
            {
                'mode': final_mode,
                'mode_detail': mode_detail,
                'group_key': key,
                'stable_id': stable_song_id,
                'matched_count': getattr(final_result, 'matched_count', None),
                'upserted_id': getattr(final_result, 'upserted_id', None),
            },
        )

        song_filter = {'scanner_stable_id': stable_song_id}
        if result_doc.get('_id') is not None:
            song_filter = {'_id': result_doc['_id']}
        elif result_doc.get('id') is not None:
            song_filter = {'id': result_doc['id']}

        existing_id = None
        if isinstance(result_doc, dict) and isinstance(result_doc.get('id'), int):
            existing_id = result_doc['id']

        inserted = existing_id is None
        song_id = existing_id

        if inserted:
            assignment_filter: Dict[str, object] = dict(song_filter)
            assignment_filter['id'] = {'$exists': False}
            assigned = False
            for attempt in range(3):
                new_id = self._get_next_song_id()
                try:
                    song_store.update_one(
                        assignment_filter,
                        {'$set': {'id': new_id, 'order': new_id}},
                    )
                except Exception as exc:  # pragma: no cover - tolerate transient driver issues
                    if DuplicateKeyError and isinstance(exc, DuplicateKeyError):
                        self._metrics.increment('duplicate_key_retries_total')
                        jitter = random.random() * 0.025
                        time.sleep(0.05 * (attempt + 1) + jitter)
                        continue
                    if PyMongoError and isinstance(exc, PyMongoError):
                        LOGGER.exception("Failed to assign song id for %s", key)
                        summary['errors'] += 1
                        return None
                    LOGGER.error(
                        'song id assignment failed: key=%s attempt=%d filter=%s',
                        key,
                        attempt + 1,
                        assignment_filter,
                        exc_info=True,
                    )
                    summary['errors'] += 1
                    return None
                latest_doc = None
                try:
                    latest_doc = song_store.find_one(song_filter)
                except Exception:  # pragma: no cover - tolerate missing find support
                    latest_doc = None
                if isinstance(latest_doc, dict) and isinstance(latest_doc.get('id'), int):
                    song_id = latest_doc['id']
                    if song_id == new_id:
                        summary['inserted'] += 1
                    assigned = True
                    break
                song_id = new_id
                summary['inserted'] += 1
                assigned = True
                break

            if not assigned or song_id is None:
                LOGGER.warning("Failed to assign song id for %s", key)
                summary['errors'] += 1
                return None

        needs_refresh = inserted or key in dirty_groups

        if needs_refresh:
            try:
                song_store.update_one(song_filter, {'$set': base_document})
                if key in dirty_groups and not inserted:
                    summary['updated'] += 1
            except Exception as exc:  # pragma: no cover - tolerate transient driver issues
                if PyMongoError and isinstance(exc, PyMongoError):
                    LOGGER.exception("Failed to update aggregated song for %s", key)
                    summary['errors'] += 1
                else:
                    LOGGER.error(
                        'song aggregate refresh failed: key=%s filter=%s',
                        key,
                        song_filter,
                        exc_info=True,
                    )
                    summary['errors'] += 1
                    return None

            try:
                self._sync_song_charts(song_filter, charts_payload)
            except Exception:  # pragma: no cover - tolerate chart sync issues
                LOGGER.debug('Failed to synchronise charts for %s', key)
            else:
                self._metrics.increment('charts_synced_total')

        return song_id

    def _upsert_song_document(
        self,
        key: str,
        records: List[TjaImportRecord],
        document: Dict[str, object],
        charts_payload: List[Dict[str, object]],
        dirty_groups: Set[str],
        summary: Dict[str, int],
    ) -> Optional[int]:
        try:
            return self._unsafe_upsert_song_document(
                key,
                records,
                document,
                charts_payload,
                dirty_groups,
                summary,
            )
        except Exception:
            LOGGER.error('song-document-upsert-crash: key=%s', key, exc_info=True)
            summary['errors'] += 1
            return None

    def _cleanup_invalid_group_keys(self) -> None:
        song_store = self._song_store
        if song_store is None:
            return
        try:
            candidates = list(song_store.find())
        except Exception:  # pragma: no cover - tolerate missing find support
            LOGGER.debug('Failed to enumerate songs for invalid group key cleanup')
            return
        invalid_docs: List[Dict[str, object]] = []
        for doc in candidates:
            if not isinstance(doc, dict):
                continue
            if not isinstance(doc.get('group_key'), str):
                invalid_docs.append(doc)
        if not invalid_docs:
            return
        invalid_keys: Set[Optional[str]] = set()
        for doc in invalid_docs:
            group_key = doc.get('group_key')
            invalid_keys.add(group_key)
            delete_filter: Dict[str, object]
            if doc.get('_id') is not None:
                delete_filter = {'_id': doc['_id']}
            else:
                delete_filter = {'group_key': group_key}
            try:
                song_store.delete_many(delete_filter)
            except TypeError:
                song_store.delete_many({'group_key': group_key})
            except Exception:  # pragma: no cover - tolerate transient issues
                LOGGER.debug('Failed to delete invalid song document for %s', group_key)
        if self._state_collection is not None and invalid_keys:
            for key in invalid_keys:
                try:
                    self._state_collection.delete_many({'group_key': key})
                except TypeError:
                    self._state_collection.delete_many({'group_key': key})
                except Exception:  # pragma: no cover - tolerate transient issues
                    LOGGER.debug('Failed to prune state for invalid group key %r', key)

    def _sync_song_charts(
        self,
        song_filter: Dict[str, object],
        charts: List[Dict[str, object]],
    ) -> None:
        song_store = self._song_store
        if song_store is None:
            LOGGER.debug('Song store unavailable while syncing charts for %s', song_filter)
            return
        if not charts:
            try:
                song_store.update_one(song_filter, {'$set': {'charts': []}})
            except Exception:  # pragma: no cover - collection issues are non-fatal
                LOGGER.debug('Failed to reset charts for %s', song_filter)
            return

        desired_courses: Set[str] = set()
        unknown_raw_courses: Set[str] = set()

        for chart in charts:
            chart_doc = dict(chart)
            chart_doc['updatedAt'] = int(time.time() * 1000)
            course_name = chart_doc.get('course')
            canonical_course = chart_doc.get('canonical_course')
            if isinstance(course_name, str):
                desired_courses.add(course_name)
            raw_course = chart_doc.get('raw_course')
            if course_name == UNKNOWN_VALUE and isinstance(raw_course, str):
                unknown_raw_courses.add(raw_course)

            match_values: List[str] = []
            if isinstance(course_name, str) and course_name:
                match_values.append(course_name)
            if isinstance(canonical_course, str) and canonical_course:
                match_values.append(canonical_course)
            if match_values:
                match_filter = {'c.course': {'$in': match_values}}
            else:
                match_filter = {'c.course': course_name}
            if course_name == UNKNOWN_VALUE and isinstance(raw_course, str):
                match_filter['c.raw_course'] = raw_course
            array_filters = [match_filter]

            try:
                song_store.update_one(
                    song_filter,
                    {'$set': {'charts.$[c]': chart_doc}},
                    array_filters=array_filters,
                )
            except TypeError:  # pragma: no cover - fallback for in-memory tests
                song_store.update_one(song_filter, {'$set': {'charts': charts}})
                return
            except Exception:  # pragma: no cover - tolerate transient issues
                LOGGER.debug('Failed to refresh chart %s for %s', course_name, song_filter)

            try:
                song_store.update_one(song_filter, {'$addToSet': {'charts': chart_doc}})
            except TypeError:  # pragma: no cover - fallback for in-memory tests
                song_store.update_one(song_filter, {'$set': {'charts': charts}})
                return
            except Exception:  # pragma: no cover - tolerate transient issues
                LOGGER.debug('Failed to add chart %s for %s', course_name, song_filter)

        if desired_courses:
            keep_courses = sorted(desired_courses)
            try:
                song_store.update_one(
                    song_filter,
                    {'$pull': {'charts': {'course': {'$nin': keep_courses}}}},
                )
            except TypeError:  # pragma: no cover - fallback for in-memory tests
                pass
            except Exception:  # pragma: no cover - tolerate transient issues
                LOGGER.debug('Failed to prune charts for %s', song_filter)

        if unknown_raw_courses:
            try:
                song_store.update_one(
                    song_filter,
                    {
                        '$pull': {
                            'charts': {
                                'course': UNKNOWN_VALUE,
                                'raw_course': {'$nin': sorted(unknown_raw_courses)},
                            }
                        }
                    },
                )
            except TypeError:  # pragma: no cover - fallback for in-memory tests
                pass
            except Exception:  # pragma: no cover - tolerate transient issues
                LOGGER.debug('Failed to prune unknown charts for %s', song_filter)

    def _select_base_record(self, records: List[TjaImportRecord]) -> TjaImportRecord:
        def _score(record: TjaImportRecord) -> Tuple[int, int, bool]:
            valid = sum(1 for chart in record.charts if chart.valid)
            return (valid, len(record.charts), bool(record.audio_url))

        return max(records, key=_score)

    def _build_song_document(self, key: str, records: List[TjaImportRecord]) -> Dict[str, object]:
        base = self._select_base_record(records)

        sorted_records = sorted(records, key=lambda rec: rec.relative_path)

        representative_paths = [record.relative_path for record in sorted_records if record.relative_path]
        primary_path = representative_paths[0] if representative_paths else sorted_records[0].relative_path

        chart_by_key: Dict[Tuple[str, Optional[str]], Dict[str, object]] = {}
        duplicate_courses: Set[str] = set()

        def _dedup_key(chart: ChartRecord) -> Tuple[str, Optional[str]]:
            if chart.mode != "standard":
                label = chart.rank or chart.display_course or chart.raw_course or chart.normalised or chart.course
                return (f"{chart.mode}:{chart.course}", label)
            if chart.course == UNKNOWN_VALUE:
                raw = chart.raw_course or chart.normalised or ""
                return (chart.course, raw)
            return (chart.course, None)

        def _storage_course_key(chart: ChartRecord) -> str:
            canonical = chart.course or ""
            if canonical in COURSE_LEGACY_MAP:
                return COURSE_LEGACY_MAP[canonical]
            return canonical.casefold()

        for record in sorted_records:
            for chart in record.charts:
                entry_issues = sorted(set(chart.issues))
                storage_course = _storage_course_key(chart)
                entry = {
                    'course': storage_course,
                    'canonical_course': chart.course,
                    'raw_course': chart.raw_course,
                    'normalised': chart.normalised,
                    'mode': chart.mode,
                    'display_course': chart.display_course,
                    'level': chart.level,
                    'branch': chart.branch,
                    'valid': chart.valid,
                    'issues': entry_issues,
                    'coerced': chart.coerced,
                    'hit_notes': chart.hit_notes,
                    'total_notes': chart.total_notes,
                    'measures': chart.measures,
                    'first_note_preview': chart.first_note_preview,
                    'segments': chart.segments,
                    'unknown_directives': chart.unknown_directives,
                    'tja_path': record.relative_path,
                    'tja_url': record.tja_url,
                    'rank': chart.rank,
                    'chart_data': _clone_chart_data(chart.chart_data),
                }
                key = _dedup_key(chart)
                existing = chart_by_key.get(key)
                if existing is None:
                    chart_by_key[key] = entry
                else:
                    label = chart.course
                    if chart.course == UNKNOWN_VALUE:
                        label = f"Unknown:{chart.raw_course or chart.normalised or ''}"
                    if chart.mode != "standard":
                        label = f"{chart.mode}:{label}:{chart.display_course or chart.raw_course or chart.normalised or ''}"
                    duplicate_courses.add(label)
                    existing_issues = set(existing.get('issues', []))
                    existing_issues.add('duplicate-course')
                    existing['issues'] = sorted(existing_issues)
                    entry['issues'] = sorted(set(entry['issues']) | {'duplicate-course'})
                    if not existing['valid'] and chart.valid:
                        chart_by_key[key] = entry

        def _chart_sort_key(item: Dict[str, object]) -> Tuple[int, str, str]:
            canonical_course = str(item.get('canonical_course') or item.get('course', ''))
            mode = str(item.get('mode', 'standard'))
            try:
                index = COURSE_ORDER.index(canonical_course)
            except ValueError:
                index = len(COURSE_ORDER)
            mode_rank = 0 if mode == 'standard' else 1
            return (mode_rank, index, canonical_course, str(item.get('tja_path', '')))

        charts_payload = sorted(chart_by_key.values(), key=_chart_sort_key)

        canonical_map: Dict[str, Dict[str, object]] = {}
        for entry in charts_payload:
            canonical_course = entry.get('canonical_course') or entry.get('course')
            if canonical_course in COURSE_ORDER:
                canonical_map[canonical_course] = entry

        courses_doc: Dict[str, Optional[Dict[str, object]]] = {
            legacy: None for legacy in COURSE_LEGACY_MAP.values()
        }
        difficulties_doc: Dict[str, Optional[Dict[str, object]]] = {
            legacy: None for legacy in COURSE_LEGACY_MAP.values()
        }
        for canonical, entry in canonical_map.items():
            legacy = COURSE_LEGACY_MAP[canonical]
            stars_value = entry.get('level')
            try:
                stars_int = int(stars_value) if stars_value is not None else 0
            except (TypeError, ValueError):
                stars_int = 0
            courses_doc[legacy] = {
                'stars': stars_int,
                'branch': bool(entry['branch']),
            }
            issues_value = entry.get('issues') if isinstance(entry.get('issues'), list) else []
            normalized_issues = [
                str(issue) for issue in issues_value if isinstance(issue, str)
            ]
            difficulties_doc[legacy] = {
                'stars': stars_int,
                'level': stars_int,
                'branch': bool(entry['branch']),
                'valid': bool(entry.get('valid', True)),
                'issues': normalized_issues,
            }

        valid_chart_count = sum(1 for chart in canonical_map.values() if chart['valid'])

        import_issue_set = {issue for record in sorted_records for issue in record.import_issues}
        if duplicate_courses:
            import_issue_set.add('duplicate_course')
        import_issues = sorted(import_issue_set)
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

        primary_chart = _select_primary_chart_entry(charts_payload)
        primary_course = ""
        primary_difficulty = ""
        if primary_chart:
            course_value = primary_chart.get('canonical_course') or primary_chart.get('course') or ""
            primary_course = str(course_value).casefold()
            level_value = primary_chart.get('level')
            if level_value is None:
                level_value = primary_chart.get('stars')
            if level_value is None:
                level_value = ""
            primary_difficulty = str(level_value)

        source_song_id = _normalise_song_id(base.song_id)
        fs_path_token = _normalise_song_fs_path(primary_path)
        stable_song_id = source_song_id or _make_deterministic_song_id(
            [
                fs_path_token,
                base.title or "",
                primary_course,
                primary_difficulty,
            ]
        )

        document = {
            'title': base.title,
            'title_lc': base.title.casefold(),
            'titleJa': base.title_ja,
            'title_lang': title_lang,
            'subtitle': base.subtitle,
            'subtitleJa': base.subtitle_ja,
            'subtitle_lang': subtitle_lang,
            'locale': base.locale,
            'courses': courses_doc,
            'difficulties': difficulties_doc,
            'charts': charts_payload,
            'import_issues': import_issues,
            'valid_chart_count': valid_chart_count,
            'valid_charts': valid_chart_count,
            'is_playable': valid_chart_count > 0,
            'enabled': enabled,
            'category_id': base.category_id,
            'category': base.category_title,
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
            'scanner_stable_id': stable_song_id,
            'scanner_primary_course': primary_course,
            'scanner_primary_difficulty': primary_difficulty,
        }
        if base.pack:
            document['pack'] = base.pack
        if audio_hash is not None:
            document['audioHash'] = audio_hash
        if source_song_id is not None:
            document['scanner_source_song_id'] = source_song_id

        if CATALOG_ASSUME_VALID:
            paths_dict = document.get('paths') if isinstance(document.get('paths'), dict) else {}
            tja_path = paths_dict.get('tja_url') if isinstance(paths_dict, dict) else None
            if tja_path:
                document['is_playable'] = True
                difficulties_value = document.get('difficulties')
                difficulties_empty = True
                if isinstance(difficulties_value, dict):
                    for difficulty_entry in difficulties_value.values():
                        if difficulty_entry:
                            difficulties_empty = False
                            break
                else:
                    difficulties_value = {}
                if difficulties_empty:
                    document['difficulties'] = {'oni': {'valid': True}}
        return document

    def _build_manifest_entry(
        self,
        document: Dict[str, object],
        records: List[TjaImportRecord],
        record_meta: Dict[str, Dict[str, object]],
    ) -> Optional[Dict[str, object]]:
        if not document.get('enabled', True):
            return None
        stable_id = document.get('scanner_stable_id') or document.get('id')
        if not isinstance(stable_id, str) or not stable_id:
            return None

        title_value = str(document.get('title') or '')
        category_value = document.get('category')
        if not isinstance(category_value, str) or not category_value.strip():
            category_value = DEFAULT_CATEGORY_TITLE
        else:
            category_value = category_value.strip()

        courses_doc = document.get('courses') if isinstance(document.get('courses'), dict) else {}
        raw_difficulties = document.get('difficulties') if isinstance(document.get('difficulties'), dict) else {}

        def _difficulty_available(value: object) -> bool:
            if isinstance(value, dict):
                return bool(value.get('valid', True))
            return bool(value)

        difficulties = {}
        for legacy in ('easy', 'normal', 'hard', 'oni', 'ura'):
            if legacy in raw_difficulties:
                difficulties[legacy] = _difficulty_available(raw_difficulties.get(legacy))
            else:
                difficulties[legacy] = bool(courses_doc.get(legacy))

        charts_payload = document.get('charts') if isinstance(document.get('charts'), list) else []
        max_duration = 0
        for chart in charts_payload:
            if not isinstance(chart, dict):
                continue
            chart_data = chart.get('chart_data')
            if not isinstance(chart_data, dict):
                continue
            duration_val = chart_data.get('duration_ms')
            try:
                duration_int = int(duration_val)
            except (TypeError, ValueError):
                duration_int = 0
            if duration_int > max_duration:
                max_duration = duration_int

        if max_duration <= 0:
            duration_candidate = document.get('duration_ms')
            try:
                duration_int = int(duration_candidate)
            except (TypeError, ValueError):
                duration_int = 0
            if duration_int > 0:
                max_duration = duration_int

        sha1_values: List[str] = []
        mtime_values: List[int] = []
        parse_failures: List[datetime] = []
        for record in records:
            meta = record_meta.get(record.relative_path)
            if not isinstance(meta, dict):
                continue
            sha1_value = meta.get('tja_sha1') or meta.get('tja_hash')
            if isinstance(sha1_value, str) and sha1_value:
                sha1_values.append(sha1_value)
            mtime_value = meta.get('tja_mtime_ns')
            if isinstance(mtime_value, int):
                mtime_values.append(mtime_value)
            elif isinstance(mtime_value, (float, str)):
                try:
                    mtime_values.append(int(mtime_value))
                except (TypeError, ValueError):
                    continue
            failure_value = meta.get('parse_failed_at')
            if isinstance(failure_value, datetime):
                parse_failures.append(failure_value)
        
        if sha1_values:
            sha1_payload = '|'.join(sorted(sha1_values))
            sha1_combined = hashlib.sha1(sha1_payload.encode('utf-8')).hexdigest()
        else:
            sha1_combined = hashlib.sha1(stable_id.encode('utf-8')).hexdigest()

        file_mtime = max(mtime_values) if mtime_values else None
        parse_failed_at = max(parse_failures) if parse_failures else None

        base_record = self._select_base_record(records)
        file_path = base_record.relative_path if base_record.relative_path else None

        preview_available = False
        preview_candidates: List[Path] = []
        preview_extensions = ("mp3", "ogg", "wav", "m4a", "flac", "opus")
        preview_base: Optional[Path] = None
        if base_record:
            if base_record.audio_path:
                audio_rel = Path(base_record.audio_path)
                preview_base = audio_rel.parent
                for ext in preview_extensions:
                    preview_candidates.append(audio_rel.with_name(f"preview.{ext}"))
            if preview_base is None:
                if base_record.relative_dir:
                    preview_base = Path(base_record.relative_dir)
                elif base_record.relative_path:
                    preview_base = Path(base_record.relative_path).parent
        if preview_base is not None:
            for ext in preview_extensions:
                preview_candidates.append(preview_base / f"preview.{ext}")
        for relative_candidate in preview_candidates:
            try:
                candidate = (self._songs_root / relative_candidate).resolve()
                candidate.relative_to(self._songs_root)
            except Exception:
                continue
            if candidate.is_file():
                preview_available = True
                break

        raw_paths = document.get('paths') if isinstance(document.get('paths'), dict) else None
        manifest_paths = None
        if raw_paths:
            manifest_paths = {
                key: raw_paths.get(key)
                for key in ('tja_url', 'audio_url', 'dir_url')
                if raw_paths.get(key)
            }

        manifest_entry: Dict[str, object] = {
            'id': stable_id,
            'title': title_value,
            'subtitle': str(document.get('subtitle') or ''),
            'title_lc': title_value.casefold(),
            'category': category_value,
            'difficulties': difficulties,
            'duration_ms': int(max_duration) if max_duration else 0,
            'preview_available': preview_available,
            'source_type': str(document.get('source_type') or document.get('type') or 'tja'),
            'paths': manifest_paths,
            'file_path': file_path,
            'mtime': file_mtime,
            'sha1': sha1_combined,
            'parse_failed_at': parse_failed_at,
        }

        return manifest_entry

    def _compute_manifest_checksum(self, entries: Dict[str, Dict[str, object]]) -> str:
        checksum_inputs: List[str] = []
        for entry_id in sorted(entries):
            entry = entries[entry_id]
            difficulties = entry.get('difficulties') if isinstance(entry.get('difficulties'), dict) else {}

            def _manifest_available(value: object) -> bool:
                if isinstance(value, dict):
                    return bool(value.get('valid', True))
                return bool(value)

            difficulty_tuple = tuple(
                _manifest_available(difficulties.get(level))
                for level in ('easy', 'normal', 'hard', 'oni', 'ura')
            )
            normalized_paths: List[Tuple[str, str]] = []
            raw_paths = entry.get('paths') if isinstance(entry.get('paths'), dict) else None
            if raw_paths:
                for key in sorted(raw_paths):
                    value = raw_paths.get(key)
                    if value:
                        normalized_paths.append((key, str(value)))
            payload = {
                'id': entry_id,
                'sha1': str(entry.get('sha1') or ''),
                'title': str(entry.get('title') or ''),
                'subtitle': str(entry.get('subtitle') or ''),
                'category': str(entry.get('category') or ''),
                'duration_ms': int(entry.get('duration_ms') or 0),
                'preview_available': bool(entry.get('preview_available')),
                'difficulties': difficulty_tuple,
                'source_type': str(entry.get('source_type') or ''),
                'paths': normalized_paths,
            }
            checksum_inputs.append(json.dumps(payload, sort_keys=True, separators=(',', ':')))
        checksum_source = '|'.join(checksum_inputs)
        if not checksum_inputs:
            return hashlib.sha1(b'').hexdigest()
        return hashlib.sha1(checksum_source.encode('utf-8')).hexdigest()

    def _sync_manifest_entries(self, entries: Dict[str, Dict[str, object]]) -> Optional[str]:
        store = self._manifest_store
        if store is None:
            return None
        now = datetime.now(UTC)
        existing_ids: Set[str] = set()
        try:
            cursor = store.find({'_id': {'$ne': '__meta__'}}, {'_id': 1})
        except Exception:
            cursor = []
        for doc in cursor:
            if isinstance(doc, dict):
                identifier = doc.get('_id')
                if isinstance(identifier, str):
                    existing_ids.add(identifier)

        desired_ids = set(entries.keys())
        stale_ids = existing_ids - desired_ids
        if stale_ids:
            try:
                store.delete_many({'_id': {'$in': list(stale_ids)}})
            except Exception:  # pragma: no cover - best effort cleanup
                LOGGER.debug('Failed to prune %d stale songs manifest entries', len(stale_ids))

        operations: List[UpdateOne] = []
        for entry_id, entry in entries.items():
            payload = dict(entry)
            payload['_id'] = entry_id
            payload['updated_at'] = now
            if 'title_lc' not in payload and isinstance(payload.get('title'), str):
                payload['title_lc'] = payload['title'].casefold()
            operations.append(UpdateOne({'_id': entry_id}, {'$set': payload}, upsert=True))
            if len(operations) >= 500:
                try:
                    store.bulk_write(operations, ordered=False)
                except Exception:  # pragma: no cover - tolerate bulk write failures
                    LOGGER.debug('Failed to bulk write songs manifest chunk size=%d', len(operations), exc_info=True)
                operations = []

        if operations:
            try:
                store.bulk_write(operations, ordered=False)
            except Exception:  # pragma: no cover - tolerate bulk write failures
                LOGGER.debug('Failed to bulk write songs manifest final chunk', exc_info=True)

        checksum = self._compute_manifest_checksum(entries)

        previous_checksum = self._manifest_checksum
        if previous_checksum is None:
            try:
                meta_doc = store.find_one(
                    {'_id': '__meta__'},
                    {'manifestChecksum': 1, 'manifest_checksum': 1, 'checksum': 1},
                )
            except Exception:  # pragma: no cover - tolerate lookup issues
                LOGGER.debug('Failed to load existing songs manifest meta', exc_info=True)
                meta_doc = None
            if isinstance(meta_doc, dict):
                for key in ('manifestChecksum', 'manifest_checksum', 'checksum'):
                    candidate = meta_doc.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        previous_checksum = candidate.strip()
                        break

        if previous_checksum != checksum:
            try:
                store.update_one(
                    {'_id': '__meta__'},
                    {
                        '$set': {
                            'checksum': checksum,
                            'updated_at': now,
                            'count': len(entries),
                            'manifest_checksum': checksum,
                            'manifestChecksum': checksum,
                        }
                    },
                    upsert=True,
                )
            except Exception:  # pragma: no cover - tolerate meta update failures
                LOGGER.debug('Failed to update songs manifest meta', exc_info=True)

        self._manifest_checksum = checksum
        return checksum

    def _seed_legacy_scanner_ids(self) -> None:
        song_store = self._song_store
        if song_store is None:
            return
        try:
            cursor = song_store.find({'scanner_stable_id': {'$exists': False}})
        except Exception:  # pragma: no cover - tolerate driver absence
            LOGGER.debug('Failed to enumerate legacy songs without stable id')
            return
        seeded = 0
        for document in cursor:
            if not isinstance(document, dict):
                continue
            stable_id = self._stable_id_for_legacy_document(document)
            if not stable_id:
                continue
            update_filter: Dict[str, object]
            if document.get('_id') is not None:
                update_filter = {'_id': document['_id']}
            elif document.get('group_key'):
                update_filter = {'group_key': document['group_key']}
            else:
                continue
            try:
                result = song_store.update_one(update_filter, {'$set': {'scanner_stable_id': stable_id}})
            except Exception:  # pragma: no cover - tolerate update issues
                LOGGER.debug('Failed to seed stable id for legacy song %r', document.get('group_key'))
                continue
            matched = getattr(result, 'matched_count', 0)
            if matched:
                seeded += 1
        if seeded:
            LOGGER.info('Seeded scanner stable ids for %d legacy songs', seeded)
            self._metrics.increment('songs_seeded_legacy_total', seeded)

    def _stable_id_for_legacy_document(self, document: Dict[str, object]) -> str:
        paths = document.get('paths') if isinstance(document.get('paths'), dict) else {}
        fs_path = ''
        if isinstance(paths, dict):
            for candidate in ('tja_url', 'dir_url', 'audio_url'):
                value = paths.get(candidate)
                if isinstance(value, str) and value:
                    fs_path = value
                    break
        if not fs_path and isinstance(document.get('path'), str):
            fs_path = str(document['path'])
        if not fs_path and isinstance(document.get('group_key'), str):
            fs_path = str(document['group_key'])
        normalised_path = _normalise_song_fs_path(fs_path)
        title = str(document.get('title') or '')
        course_value = document.get('scanner_primary_course') or document.get('primary_course') or ''
        difficulty_value = (
            document.get('scanner_primary_difficulty')
            or document.get('primary_difficulty')
            or document.get('difficulty')
            or ''
        )
        course = str(course_value).casefold()
        difficulty = str(difficulty_value)
        return _make_deterministic_song_id([
            normalised_path,
            title,
            course,
            difficulty,
        ])

    def _current_song_id_ceiling(self, *, include_counter: bool = True) -> int:
        current = 0
        if include_counter:
            counters = getattr(self.db, 'counters', None)
            if counters is not None:
                try:
                    counter_doc = counters.find_one({'_id': 'songs'})
                except Exception:  # pragma: no cover - tolerate driver errors
                    counter_doc = None
                if counter_doc and isinstance(counter_doc.get('seq'), int):
                    current = max(current, int(counter_doc['seq']))
        seq = getattr(self.db, 'seq', None)
        if seq is not None:
            try:
                seq_doc = seq.find_one({'name': 'songs'})
            except Exception:  # pragma: no cover - tolerate driver errors
                seq_doc = None
            if seq_doc and isinstance(seq_doc.get('value'), int):
                current = max(current, int(seq_doc['value']))
        song_store = self._song_store
        if song_store is not None:
            try:
                max_song = song_store.find_one(sort=[('id', -1)])
            except Exception:  # pragma: no cover - tolerate driver errors
                max_song = None
        else:
            max_song = None
        if max_song and isinstance(max_song.get('id'), int):
            current = max(current, int(max_song['id']))
        return current

    def _get_next_song_id(self) -> int:
        counters = getattr(self.db, 'counters', None)
        if counters is not None:
            floor = self._current_song_id_ceiling()
            try:
                counters.update_one(
                    {'_id': 'songs'},
                    {
                        '$setOnInsert': {'seq': floor},
                        '$max': {'seq': floor},
                    },
                    upsert=True,
                )
            except Exception:  # pragma: no cover - tolerate driver issues
                LOGGER.debug('Failed to ensure songs counter floor at %d', floor, exc_info=True)
            for attempt in range(3):
                try:
                    result = counters.find_one_and_update(
                        {'_id': 'songs'},
                        {'$inc': {'seq': 1}},
                        upsert=True,
                        return_document=ReturnDocument.AFTER,
                    )
                except Exception as exc:  # pragma: no cover - tolerate transient driver issues
                    if PyMongoError and isinstance(exc, PyMongoError):
                        LOGGER.warning(
                            'Failed to increment songs counter (attempt %d/3)',
                            attempt + 1,
                            exc_info=True,
                        )
                    else:
                        LOGGER.debug('Failed to increment songs counter on attempt %d: %s', attempt + 1, exc)
                    time.sleep(0.05 * (attempt + 1))
                    continue
                if isinstance(result, dict) and isinstance(result.get('seq'), int):
                    seq_value = int(result['seq'])
                    existing_ceiling = self._current_song_id_ceiling(include_counter=False)
                    if seq_value <= existing_ceiling:
                        LOGGER.warning(
                            'Songs counter produced stale value %d (existing ceiling %d); clamping and retrying',
                            seq_value,
                            existing_ceiling,
                        )
                        try:
                            counters.update_one(
                                {'_id': 'songs'},
                                {
                                    '$setOnInsert': {'seq': existing_ceiling},
                                    '$max': {'seq': existing_ceiling},
                                },
                                upsert=True,
                            )
                        except Exception:  # pragma: no cover - tolerate driver issues
                            LOGGER.debug('Failed to clamp songs counter to %d', existing_ceiling, exc_info=True)
                            break
                        floor = max(floor, existing_ceiling)
                        continue
                    return seq_value
                break
            else:
                LOGGER.warning('Failed to increment songs counter after retries; falling back')

        seq = getattr(self.db, 'seq', None)
        if seq is not None:
            try:
                current = self._current_song_id_ceiling()
                seq_doc = seq.find_one({'name': 'songs'})
                song_store = self._song_store
                max_song = None
                if song_store is not None:
                    try:
                        max_song = song_store.find_one(sort=[('id', -1)])
                    except Exception:
                        max_song = None
                if seq_doc and isinstance(seq_doc.get('value'), int):
                    current = max(current, seq_doc['value'])
                if max_song and isinstance(max_song.get('id'), int):
                    current = max(current, max_song['id'])
                next_value = current + 1
                seq.update_one({'name': 'songs'}, {'$set': {'value': next_value}}, upsert=True)
                return next_value
            except Exception:  # pragma: no cover - tolerate legacy sequence failures
                LOGGER.debug('Falling back to on-demand song id allocation')

        song_store = self._song_store
        max_song = None
        if song_store is not None:
            try:
                max_song = song_store.find_one(sort=[('id', -1)])
            except Exception:
                max_song = None
        if max_song and isinstance(max_song.get('id'), int):
            return int(max_song['id']) + 1
        return 1

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

        def _find_hls_playlist() -> Optional[Path]:
            candidates: List[Path] = []
            hls_dir = tja_path.parent / "HLS"
            if hls_dir.is_dir():
                candidates.extend(sorted(hls_dir.glob('*.t3u8'), key=lambda p: p.name.lower()))
            candidates.extend(sorted(tja_path.parent.glob('*.t3u8'), key=lambda p: p.name.lower()))
            for candidate in candidates:
                try:
                    resolved = candidate.resolve()
                except FileNotFoundError:
                    continue
                try:
                    resolved.relative_to(self._songs_root)
                except ValueError:
                    continue
                if resolved.is_file():
                    return resolved
            return None

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
        has_dojo_charts = parsed.has_dojo_course or any(
            course.normalised in {"DAN", "DOJO", "KYUU"} for course in parsed.courses
        )

        if has_dojo_charts:
            playlist = _find_hls_playlist()
            if playlist is not None:
                return playlist, diagnostics
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
        if len(parts) <= 1:
            return 0, DEFAULT_CATEGORY_TITLE
        top_folder = parts[0]
        prefix, title_candidate = _split_numeric_prefix(top_folder)
        if title_candidate:
            title_value = _normalise_space_runs(title_candidate)
        else:
            title_value = _normalise_space_runs(_clean_metadata_value(top_folder)) if top_folder else ""
        if not title_value:
            title_value = DEFAULT_CATEGORY_TITLE
        category_id = prefix if prefix is not None else 0
        return category_id, title_value

    def scan(self, *, full: bool = False) -> Dict[str, int]:
        """Scan songs directory and sync metadata with MongoDB."""

        TJA_VALIDATOR.reset_run()
        start_perf = time.perf_counter()
        start_wall = time.time()
        mode_str = 'full' if full else 'incremental'
        summary: Dict[str, int] = {}
        counter_handler = _ScanLogCounter()
        log_targets = [LOGGER]
        for target in log_targets:
            target.addHandler(counter_handler)

        if SCAN_LOG_SUMMARY:
            SUMMARY_LOGGER.info("scan:start ts=%d mode=%s", int(start_wall), mode_str)

        try:
            with self._scan_lock:
                summary = self._scan_impl(full=full)
        finally:
            elapsed = time.perf_counter() - start_perf
            if elapsed < 0:
                elapsed = 0.0
            computed_duration = round(elapsed, 3)
            existing_duration = summary.get('duration_seconds') if isinstance(summary, dict) else None
            final_duration = computed_duration
            if existing_duration is not None:
                try:
                    final_duration = float(existing_duration)
                except (TypeError, ValueError):
                    final_duration = computed_duration
            summary['duration_seconds'] = final_duration

            active_summary: Dict[str, int] = summary
            if not active_summary and isinstance(self._active_summary, dict):
                active_summary = self._active_summary

            checksum_value = summary.get('manifest_checksum')
            checksum_str = checksum_value if checksum_value else '-'

            TJA_VALIDATOR.flush_summary()

            error_count = counter_handler.error_count

            for target in log_targets:
                with contextlib.suppress(Exception):
                    target.removeHandler(counter_handler)

            if SCAN_LOG_SUMMARY:
                try:
                    SUMMARY_LOGGER.info(
                        "scan: mode=%s found=%d inserted=%d updated=%d disabled=%d errors=%d skipped=%d duration=%.3fs checksum=%s",
                        mode_str,
                        int(active_summary.get('found', 0)),
                        int(active_summary.get('inserted', 0)),
                        int(active_summary.get('updated', 0)),
                        int(active_summary.get('disabled', 0)),
                        int(max(active_summary.get('errors', 0), error_count)),
                        int(active_summary.get('skipped', 0)),
                        final_duration,
                        checksum_str,
                    )
                except Exception as exc:  # pragma: no cover - defensive logging path
                    SUMMARY_LOGGER.info("scan:summary(format_error=%s)", exc)

            active_stack = getattr(self, '_active_refresher_stack', None)
            if active_stack is not None:
                with contextlib.suppress(Exception):
                    active_stack.close()
            self._active_refresher_stack = None

            self._active_summary = None

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
        self._active_summary = summary
        performed_scan = False
        refresher_stack = contextlib.ExitStack()
        self._active_refresher_stack = refresher_stack
        manifest_meta = self._load_manifest_meta() or {}
        manifest_checksum: Optional[str] = None
        manifest_documents = 0
        manifest_files_count: Optional[int] = None
        force_scan = False
        if isinstance(manifest_meta, dict):
            for candidate in (
                manifest_meta.get('manifest_checksum'),
                manifest_meta.get('checksum'),
            ):
                if isinstance(candidate, str) and candidate.strip():
                    manifest_checksum = candidate.strip()
                    break
            manifest_documents_value = _coerce_int(manifest_meta.get('manifest_documents'))
            if manifest_documents_value is not None:
                manifest_documents = manifest_documents_value
            manifest_files_count = _coerce_int(manifest_meta.get('files_count'))
            if manifest_files_count is None:
                manifest_files_count = manifest_documents_value
            force_scan = bool(manifest_meta.get('force'))
        summary['manifest_documents'] = manifest_documents
        if manifest_checksum:
            summary['manifest_checksum'] = manifest_checksum
        summary['fast_path'] = False
        summary['leader'] = False
        summary['reason'] = 'digest_changed'
        summary['skipped_due_to_leader'] = False

        checksum, files_count = compute_fs_digest(self.songs_dir, ignore_globs=self.ignore_globs)
        summary['files_count'] = files_count
        summary['fs_checksum'] = checksum

        if not self.songs_dir.exists():
            LOGGER.warning("Songs directory %s does not exist", self.songs_dir)
            reason = 'digest_equal' if manifest_checksum == checksum and manifest_documents == files_count else 'digest_changed'
            summary['fast_path'] = True
            self._log_scan_outcome(summary, fast_path=True, reason=reason)
            refresher_stack.close()
            self._active_refresher_stack = None
            return summary

        if force_scan:
            full = True

        redis_available = self._leader_lock is not None or self._redis is not None

        stored_manifest_files = manifest_files_count if manifest_files_count is not None else manifest_documents

        digest_equal = (
            not full
            and bool(manifest_checksum)
            and manifest_checksum == checksum
            and stored_manifest_files == files_count
        )

        songs_count_value = self._count_enabled_songs()
        if songs_count_value is not None:
            summary['songs_count_before'] = songs_count_value

        if (
            digest_equal
            and manifest_documents == 0
            and (songs_count_value is None or songs_count_value == 0)
        ):
            summary['fast_path'] = True
            summary['reason'] = 'digest_equal_empty'
            self._log_scan_outcome(summary, fast_path=True, reason='digest_equal_empty')
            refresher_stack.close()
            self._active_refresher_stack = None
            return summary

        manifest_has_documents = manifest_documents > 0
        safe_fast_path = (
            digest_equal
            and manifest_has_documents
            and songs_count_value is not None
            and songs_count_value >= manifest_documents
            and manifest_documents > 0
        )

        rehydrate_mode: Optional[str] = None
        if (
            digest_equal
            and manifest_has_documents
            and songs_count_value is not None
        ):
            if songs_count_value <= 0:
                rehydrate_mode = 'full'
            elif songs_count_value < manifest_documents:
                rehydrate_mode = 'missing'

        if safe_fast_path:
            summary['fast_path'] = True
            if redis_available and not self._acquire_leader_lock():
                summary['skipped_due_to_leader'] = True
                self._log_scan_outcome(summary, fast_path=True, reason='lock_miss')
                refresher_stack.close()
                self._active_refresher_stack = None
                return summary
            self._log_scan_outcome(summary, fast_path=True, reason='digest_equal')
            refresher_stack.close()
            self._active_refresher_stack = None
            return summary

        if rehydrate_mode is not None:
            if redis_available and not self._acquire_leader_lock():
                summary['skipped_due_to_leader'] = True
                self._log_scan_outcome(summary, fast_path=False, reason='lock_miss')
                refresher_stack.close()
                self._active_refresher_stack = None
                return summary
            summary['rehydrate_mode'] = rehydrate_mode
            summary.setdefault('rehydrated', 0)
            if rehydrate_mode == 'full':
                LOGGER.info(
                    'Songs collection is empty while manifest exists; materializing songs from manifest...'
                )
            else:
                LOGGER.info(
                    'Songs collection partially diverged from manifest (songs=%s < manifest=%s); '
                    'rehydrating missing entries...',
                    songs_count_value,
                    manifest_documents,
                )
            rehydrate_result = self._materialize_songs_from_manifest(
                summary,
                mode=rehydrate_mode,
                manifest_documents=manifest_documents,
                songs_count_before=songs_count_value,
            )
            if rehydrate_result is not None:
                summary['reason'] = 'rehydrate_from_manifest'
                songs_count_after = rehydrate_result.get('songs_count_after')
                summary['songs_count_after'] = songs_count_after
                consistent = rehydrate_result.get('consistent')
                rehydrated_total = rehydrate_result.get('rehydrated', 0)
                if consistent:
                    if manifest_checksum and files_count is not None:
                        self._update_manifest_meta(manifest_checksum, files_count, manifest_documents)
                    LOGGER.info(
                        'Songs collection reconciled with manifest (mode=%s, rehydrated=%s, total=%s)',
                        rehydrate_mode,
                        rehydrated_total,
                        songs_count_after,
                    )
                    self._log_scan_outcome(summary, fast_path=False, reason='rehydrate_from_manifest')
                    refresher_stack.close()
                    self._active_refresher_stack = None
                    return summary
                LOGGER.info('Manifest rehydration incomplete; running full scan...')
            else:
                LOGGER.info('Manifest rehydration unavailable; running full scan...')

        if redis_available:
            if not self._acquire_leader_lock():
                summary['skipped_due_to_leader'] = True
                self._log_scan_outcome(summary, fast_path=False, reason='lock_miss')
                refresher_stack.close()
                self._active_refresher_stack = None
                return summary

        performed_scan = True
        summary['fast_path'] = False
        summary['reason'] = 'digest_changed'

        if self._leader_lock is not None:
            token_for_refresh = self._leader_lock_token
            if token_for_refresh:
                ttl_value = LEADER_LOCK_TTL_SECONDS
                def _clear_token(token_to_clear: str = token_for_refresh) -> None:
                    if self._leader_lock_token == token_to_clear:
                        self._leader_lock_token = None

                refresher_stack.enter_context(
                    TTLRefresher(
                        self._leader_lock,
                        token_for_refresh,
                        ttl_value,
                        period=60,
                        on_release=_clear_token,
                    )
                )

        self._cleanup_invalid_group_keys()
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

        failed_state_updates: Dict[str, Dict[str, object]] = {}

        def _safe_sha1(path: Path) -> Optional[str]:
            try:
                return hashlib.sha1(path.read_bytes()).hexdigest()
            except Exception:
                return None

        song_store = self._song_store
        if song_store is not None:
            try:
                cursor = song_store.find({'managed_by_scanner': True}, {'id': 1, 'enabled': 1})
            except AttributeError:
                cursor = []
            except Exception:  # pragma: no cover - defensive when find unsupported
                LOGGER.debug("songs.find is not available on song store")
                cursor = []
        else:
            cursor = []

        for doc in cursor:
            doc_id = doc.get('id')
            if isinstance(doc_id, int):
                managed_songs[doc_id] = bool(doc.get('enabled', True))

        aggregated_records: Dict[str, List[TjaImportRecord]] = defaultdict(list)
        records_by_path: Dict[str, TjaImportRecord] = {}
        record_meta: Dict[str, Dict[str, object]] = {}
        group_key_by_path: Dict[str, str] = {}
        dirty_groups: Set[str] = set()
        manifest_entries_by_id: Dict[str, Dict[str, object]] = {}
        manifest_entry_checksum: Optional[str] = None

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

            current_file_sha1: Optional[str] = None
            parse_failed_at: Optional[datetime] = None
            failure_state_sha1: Optional[str] = None
            last_ok_sha1: Optional[str] = None
            if state_doc is not None:
                raw_failed_at = state_doc.get('parse_failed_at')
                if isinstance(raw_failed_at, datetime):
                    parse_failed_at = raw_failed_at
                elif isinstance(raw_failed_at, str) and raw_failed_at:
                    with contextlib.suppress(ValueError):
                        parsed_dt = datetime.fromisoformat(raw_failed_at)
                        if parsed_dt.tzinfo is None:
                            parse_failed_at = parsed_dt.replace(tzinfo=UTC)
                        else:
                            parse_failed_at = parsed_dt
                elif isinstance(raw_failed_at, (int, float)) and raw_failed_at > 0:
                    with contextlib.suppress(Exception):
                        parse_failed_at = datetime.fromtimestamp(raw_failed_at, UTC)
                sha1_candidate = state_doc.get('tja_sha1')
                if isinstance(sha1_candidate, str) and sha1_candidate:
                    failure_state_sha1 = sha1_candidate
                last_ok_candidate = state_doc.get('last_ok_sha1')
                if isinstance(last_ok_candidate, str) and last_ok_candidate:
                    last_ok_sha1 = last_ok_candidate

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

            skip_due_to_failure = False
            if parse_failed_at and not full and not needs_processing:
                if failure_state_sha1:
                    current_file_sha1 = _safe_sha1(tja_path)
                    if current_file_sha1 and current_file_sha1 != failure_state_sha1:
                        needs_processing = True
                    else:
                        skip_due_to_failure = True
                else:
                    skip_due_to_failure = True

            record: Optional[TjaImportRecord] = None
            diagnostics: List[str] = []
            file_hash: Optional[str] = None
            file_sha1: Optional[str] = None
            fingerprint: Optional[str] = None

            if not needs_processing and state_doc:
                record_payload = state_doc.get('record') if isinstance(state_doc.get('record'), dict) else None
                if record_payload:
                    record = self._record_from_state(record_payload)
                    if record:
                        file_hash = str(state_doc.get('tja_hash') or record.tja_hash)
                        sha1_value = state_doc.get('tja_sha1')
                        if isinstance(sha1_value, str) and sha1_value:
                            file_sha1 = sha1_value
                        fingerprint = str(state_doc.get('fingerprint') or record.fingerprint)
                        group_key_by_path[tja_key] = compute_group_key(record)
                        summary['skipped'] += 1
                if record is None:
                    if skip_due_to_failure:
                        summary['skipped'] += 1
                    else:
                        needs_processing = True

            was_dirty = needs_processing

            if needs_processing:
                LOGGER.debug('scan-job-start: %s', tja_path)
                try:
                    parsed = parse_tja(tja_path)
                    total_notes = sum(course.total_notes for course in parsed.courses)
                    if total_notes:
                        self._metrics.increment('tja_notes_total', total_notes)
                    if parsed.unknown_directives:
                        self._metrics.increment('tja_unknown_directives_total', parsed.unknown_directives)
                    if parsed.skipped_charts:
                        self._metrics.increment('tja_skipped_charts_total', parsed.skipped_charts)
                    if parsed.mapped_courses:
                        self._metrics.increment('tja_mapped_course_total', parsed.mapped_courses)
                    if parsed.skipped_no_course:
                        self._metrics.increment('tja_skipped_no_course_total', parsed.skipped_no_course)
                    if parsed.skipped_unknown_course:
                        self._metrics.increment('tja_skipped_unknown_course_total', parsed.skipped_unknown_course)
                    if parsed.has_dojo_course:
                        self._metrics.increment('tja_dojo_parsed_total')
                    audio_path, diagnostics = self._detect_audio(tja_path, parsed)
                    tja_bytes = tja_path.read_bytes()
                    file_hash = md5_bytes(tja_bytes)
                    file_sha1 = hashlib.sha1(tja_bytes).hexdigest()
                    current_file_sha1 = file_sha1
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
                except Exception:
                    LOGGER.error('scan-job-crash: file=%s', tja_path, exc_info=True)
                    summary['errors'] += 1
                    failure_sha1 = current_file_sha1 or failure_state_sha1 or _safe_sha1(tja_path)
                    if failure_sha1 is not None or parse_failed_at is not None:
                        failure_payload = {
                            'tja_path': tja_key,
                            'tja_mtime_ns': tja_mtime_ns,
                            'tja_size': tja_size,
                            'tja_sha1': failure_sha1,
                            'parse_failed_at': datetime.now(UTC),
                        }
                        if last_ok_sha1:
                            failure_payload['last_ok_sha1'] = last_ok_sha1
                        failed_state_updates[tja_key] = failure_payload
                    continue
                LOGGER.debug('scan-job-finish: %s', tja_path)

            if record is None:
                if skip_due_to_failure:
                    continue
                summary['errors'] += 1
                failure_sha1 = current_file_sha1 or failure_state_sha1 or _safe_sha1(tja_path)
                failure_payload = {
                    'tja_path': tja_key,
                    'tja_mtime_ns': tja_mtime_ns,
                    'tja_size': tja_size,
                    'tja_sha1': failure_sha1,
                    'parse_failed_at': datetime.now(UTC),
                }
                if last_ok_sha1:
                    failure_payload['last_ok_sha1'] = last_ok_sha1
                failed_state_updates[tja_key] = failure_payload
                continue

            if file_sha1 is None:
                try:
                    tja_bytes = tja_path.read_bytes()
                except Exception:
                    file_sha1 = None
                else:
                    file_sha1 = hashlib.sha1(tja_bytes).hexdigest()
            if file_sha1 is not None:
                current_file_sha1 = file_sha1

            key = group_key_by_path.get(tja_key) or compute_group_key(record)
            group_key_by_path[tja_key] = key
            aggregated_records[key].append(record)
            records_by_path[tja_key] = record

            if was_dirty:
                dirty_groups.add(key)

            record_meta[tja_key] = {
                'tja_hash': file_hash or record.tja_hash,
                'tja_mtime_ns': tja_mtime_ns,
                'tja_size': tja_size,
                'tja_sha1': file_sha1,
                'audio_hash': record.audio_hash,
                'audio_mtime_ns': record.audio_mtime_ns,
                'audio_size': record.audio_size,
                'fingerprint': fingerprint or record.fingerprint,
                'parse_failed_at': None if was_dirty else parse_failed_at,
                'last_ok_sha1': file_sha1 if was_dirty else (last_ok_sha1 or file_sha1),
            }

            if record.category_id != 0:
                categories[record.category_id] = record.category_title

        song_id_by_key: Dict[str, int] = {}
        for key in sorted(aggregated_records.keys()):
            records = aggregated_records[key]
            document = self._build_song_document(key, records)
            charts_payload: List[Dict[str, object]] = list(document.get('charts', []))
            manifest_entry = self._build_manifest_entry(
                document,
                records,
                record_meta,
            )
            if manifest_entry:
                document['preview_available'] = bool(manifest_entry.get('preview_available'))
                if manifest_entry.get('sha1'):
                    document['sha1'] = manifest_entry.get('sha1')
                if manifest_entry.get('mtime') is not None:
                    document['mtime'] = manifest_entry.get('mtime')
                document['parse_failed_at'] = manifest_entry.get('parse_failed_at')
            else:
                document['preview_available'] = False
                document.pop('sha1', None)
                document.pop('mtime', None)
                document['parse_failed_at'] = None
            try:
                song_id = self._upsert_song_document(
                    key,
                    records,
                    document,
                    charts_payload,
                    dirty_groups,
                    summary,
                )
            except Exception:
                LOGGER.error(
                    'songs upsert failed: title=%s course=%s',
                    document.get('title'),
                    key,
                    exc_info=True,
                )
                summary['errors'] += 1
                continue
            if song_id is not None:
                if manifest_entry:
                    manifest_entries_by_id[manifest_entry['id']] = manifest_entry
                for chart_entry in charts_payload:
                    course_label = (
                        chart_entry.get('canonical_course')
                        or chart_entry.get('course')
                        or UNKNOWN_VALUE
                    )
                    total_notes_value = chart_entry.get('total_notes', 0)
                    try:
                        total_notes_int = int(total_notes_value)
                    except (TypeError, ValueError):
                        total_notes_int = 0
                    longs_total = 0
                    measures_payload = chart_entry.get('measures')
                    if isinstance(measures_payload, list):
                        for measure in measures_payload:
                            if not isinstance(measure, dict):
                                continue
                            longs_list = measure.get('longs')
                            if isinstance(longs_list, list):
                                longs_total += sum(
                                    1 for long_note in longs_list if isinstance(long_note, dict)
                                )
                    LOGGER.debug(
                        'upserted-chart: title=%s course=%s notes=%d longs=%d',
                        document.get('title'),
                        course_label,
                        total_notes_int,
                        longs_total,
                    )
                seen_song_ids.add(song_id)
                song_id_by_key[key] = song_id

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
                    'tja_sha1': meta.get('tja_sha1'),
                    'audio_path': record.audio_path,
                    'audio_hash': meta.get('audio_hash'),
                    'audio_mtime_ns': meta.get('audio_mtime_ns'),
                    'audio_size': meta.get('audio_size'),
                    'song_id': song_id,
                    'group_key': key,
                    'fingerprint': meta.get('fingerprint'),
                    'parse_failed_at': meta.get('parse_failed_at'),
                    'last_ok_sha1': meta.get('last_ok_sha1'),
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

        if self._state_collection is not None and failed_state_updates:
            for tja_key, failure_payload in failed_state_updates.items():
                payload = dict(failure_payload)
                payload.setdefault('tja_path', tja_key)
                try:
                    self._state_collection.update_one({'tja_path': tja_key}, {'$set': payload}, upsert=True)
                except Exception:
                    LOGGER.debug('Failed to persist failure state for %s', tja_key)

        if self._state_collection is not None:
            stale_paths = set(state_docs.keys()) - seen_state_paths
            if stale_paths:
                try:
                    self._state_collection.delete_many({'tja_path': {'$in': list(stale_paths)}})
                except Exception:  # pragma: no cover - best effort cleanup
                    LOGGER.debug('Failed to prune %d stale scanner state entries', len(stale_paths))

        manifest_document_count = len(manifest_entries_by_id)
        if manifest_entries_by_id:
            try:
                manifest_checksum_value = self._sync_manifest_entries(manifest_entries_by_id)
                if manifest_checksum_value:
                    manifest_entry_checksum = manifest_checksum_value
            except Exception:
                LOGGER.debug('Failed to synchronise songs manifest entries', exc_info=True)
        summary['manifest_documents'] = manifest_document_count

        if manifest_entry_checksum:
            summary['manifest_entry_checksum'] = manifest_entry_checksum

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
            if song_store is not None:
                try:
                    song_store.update_one({'id': missing_id}, {'$set': {'enabled': False}})
                except Exception:
                    LOGGER.debug('Failed to disable missing song %d', missing_id)
            if previous_enabled:
                summary['disabled'] += 1

        if performed_scan:
            manifest_documents_for_meta = int(summary.get('manifest_documents') or 0)
            final_manifest_checksum = checksum or summary.get('fs_checksum') or ''
            if final_manifest_checksum is None:
                final_manifest_checksum = ''
            summary['manifest_checksum'] = final_manifest_checksum
            self._update_manifest_meta(final_manifest_checksum, files_count, manifest_documents_for_meta)

        self._metrics.flush()

        if performed_scan:
            self._log_scan_outcome(summary, fast_path=False, reason=summary.get('reason', 'digest_changed'))

        refresher_stack.close()
        self._active_refresher_stack = None
        return summary

    @property
    def watchdog_supported(self) -> bool:
        return self._watchdog_supported

    @property
    def leader_lock_key(self) -> str:
        return self._leader_lock_key

    def has_leader_lock(self) -> bool:
        lock = self._leader_lock
        if lock is not None:
            token = self._leader_lock_token
            if token is None:
                LOGGER.debug('scanner leader lock unavailable: token missing')
                return False
            try:
                owner = lock.get_owner()
            except Exception:  # pragma: no cover - storage access best effort
                LOGGER.debug('Failed to read scanner leader lock state', exc_info=True)
                self._leader_lock_token = None
                return False
            if owner == token:
                return True
            LOGGER.debug('scanner leader lock unavailable: token mismatch')
            self._leader_lock_token = None
            return False
        client = self._redis
        if client is None:
            if self._leader_lock_token is not None:
                LOGGER.debug('scanner leader lock unavailable: redis client missing')
            self._leader_lock_token = None
            return False
        token = self._leader_lock_token
        if token is None:
            LOGGER.debug('scanner leader lock unavailable: token missing')
            return False
        try:
            value = client.get(self._leader_lock_key)
        except Exception:  # pragma: no cover - redis access best effort
            LOGGER.debug('Failed to read scanner leader lock state', exc_info=True)
            self._leader_lock_token = None
            return False
        if value is None:
            LOGGER.debug('scanner leader lock unavailable: key missing on redis')
            self._leader_lock_token = None
            return False
        if isinstance(value, bytes):
            try:
                value = value.decode('utf-8')
            except Exception:
                value = None
        if value == token:
            return True
        LOGGER.debug('scanner leader lock unavailable: token mismatch')
        self._leader_lock_token = None
        return False

    def _ensure_leader_token(self) -> str:
        token = self._leader_lock_token
        if token:
            return token
        hostname = socket.gethostname() or 'localhost'
        token = f"{hostname}:{os.getpid()}"
        self._leader_lock_token = token
        return token

    def _acquire_leader_lock(self) -> bool:
        lock = self._leader_lock
        if lock is not None:
            ttl_value = LEADER_LOCK_TTL_SECONDS
            token = self._ensure_leader_token()
            owner: Optional[str] = None
            try:
                owner = lock.get_owner()
            except Exception:  # pragma: no cover - storage access best effort
                LOGGER.debug('Failed to read scanner leader lock state', exc_info=True)
            if owner == token:
                try:
                    if lock.refresh(token, ttl_value):
                        return True
                except Exception:  # pragma: no cover - ttl refresh best effort
                    LOGGER.debug('Failed to refresh scanner leader lock ttl', exc_info=True)
            try:
                if lock.acquire(token, ttl_value):
                    self._leader_lock_token = token
                    return True
            except Exception:  # pragma: no cover - storage access best effort
                LOGGER.debug('Failed to acquire scanner leader lock', exc_info=True)
                self._leader_lock_token = None
                return False
            if owner != token:
                owner_label = owner
                if owner_label is None:
                    try:
                        owner_label = lock.get_owner()
                    except Exception:  # pragma: no cover - storage access best effort
                        LOGGER.debug('Failed to read scanner leader lock owner', exc_info=True)
                        owner_label = None
                LOGGER.info(
                    'Song scanner leader lock miss: key=%s owner=%s',
                    self._leader_lock_key,
                    owner_label or '<unknown>',
                )
            self._leader_lock_token = None
            return False
        client = self._redis
        if client is None:
            LOGGER.debug('scanner leader lock unavailable: redis client missing')
            self._leader_lock_token = None
            return False
        token = self._leader_lock_token
        if token is not None:
            try:
                current = client.get(self._leader_lock_key)
            except Exception:  # pragma: no cover - redis access best effort
                LOGGER.debug('Failed to refresh scanner leader lock', exc_info=True)
                self._leader_lock_token = None
                return False
            if isinstance(current, bytes):
                try:
                    current = current.decode('utf-8')
                except Exception:
                    current = None
            if current == token:
                try:
                    client.expire(self._leader_lock_key, self._leader_lock_ttl)
                except Exception:  # pragma: no cover - ttl refresh best effort
                    LOGGER.debug('Failed to refresh scanner leader lock ttl', exc_info=True)
                return True
            self._leader_lock_token = None
        new_token = self._ensure_leader_token()
        try:
            acquired = client.set(self._leader_lock_key, new_token, nx=True, ex=self._leader_lock_ttl)
        except Exception:  # pragma: no cover - redis access best effort
            LOGGER.debug('Failed to acquire scanner leader lock', exc_info=True)
            self._leader_lock_token = None
            return False
        if acquired:
            self._leader_lock_token = new_token
            return True
        self._leader_lock_token = None
        return False

    def _load_manifest_meta(self) -> Optional[Dict[str, object]]:
        collection = self._meta_collection
        if collection is None:
            return None
        try:
            doc = collection.find_one({'_id': 'songs_manifest'})
        except Exception:  # pragma: no cover - tolerate transient meta errors
            LOGGER.debug('Failed to load songs manifest meta document', exc_info=True)
            return None
        if isinstance(doc, dict):
            return dict(doc)
        return None

    def _update_manifest_meta(self, checksum: str, files_count: int, manifest_documents: int) -> None:
        collection = self._meta_collection
        if collection is None:
            return
        payload = {
            'checksum': checksum,
            'manifest_checksum': checksum,
            'files_count': files_count,
            'manifest_documents': manifest_documents,
            'updated_at': datetime.now(UTC),
        }
        update = {
            '$set': payload,
            '$unset': {'force': ''},
        }
        try:
            collection.update_one({'_id': 'songs_manifest'}, update, upsert=True)
        except Exception:  # pragma: no cover - tolerate transient meta errors
            LOGGER.debug('Failed to update songs manifest meta document', exc_info=True)

    def _log_scan_outcome(self, summary: Dict[str, object], *, fast_path: bool, reason: str) -> None:
        leader = self.has_leader_lock()
        summary['leader'] = bool(leader)
        summary['fast_path'] = fast_path
        summary['reason'] = reason
        files_count_value = _coerce_int(summary.get('files_count')) or 0
        manifest_documents_value = _coerce_int(summary.get('manifest_documents')) or 0
        SUMMARY_LOGGER.info(
            'scan: pid=%d fast_path=%s leader=%s reason=%s files_count=%d manifest_documents=%d',
            os.getpid(),
            fast_path,
            leader,
            reason,
            files_count_value,
            manifest_documents_value,
        )

    def _count_enabled_songs(self) -> Optional[int]:
        song_store = self._song_store
        if song_store is None:
            return None
        try:
            count_enabled = getattr(song_store, 'count_enabled', None)
            if callable(count_enabled):
                raw_count = count_enabled()  # type: ignore[misc]
            else:
                counter = getattr(song_store, 'count_documents', None)
                if not callable(counter):
                    return None
                filter_doc = {
                    '$or': [
                        {'disabled': False},
                        {'disabled': {'$exists': False}},
                    ]
                }
                raw_count = counter(filter_doc)
        except Exception:  # pragma: no cover - tolerate storage access failures
            LOGGER.debug('Failed to count enabled songs', exc_info=True)
            return None
        try:
            value = int(raw_count)
        except (TypeError, ValueError):
            return None
        if value < 0:
            return None
        return value

    def _materialize_songs_from_manifest(
        self,
        summary: Dict[str, object],
        *,
        mode: str,
        manifest_documents: int,
        songs_count_before: Optional[int],
    ) -> Optional[Dict[str, object]]:
        song_store = self._song_store
        manifest_store = self._manifest_store
        if song_store is None or manifest_store is None:
            LOGGER.debug(
                'Skipping manifest materialization: song_store=%s manifest_store=%s',
                song_store,
                manifest_store,
            )
            return None
        try:
            cursor = manifest_store.find({'_id': {'$ne': '__meta__'}})
        except Exception:  # pragma: no cover - tolerate manifest enumeration errors
            LOGGER.debug('Failed to enumerate manifest entries for rehydration', exc_info=True)
            return None

        existing_enabled_ids: Set[str] = set()
        if mode == 'missing':
            try:
                existing_cursor = song_store.find(
                    {'scanner_stable_id': {'$type': 'string'}},
                    {'scanner_stable_id': 1, 'disabled': 1},
                )
                for raw_doc in existing_cursor:
                    if not isinstance(raw_doc, Mapping):
                        continue
                    stable_id = raw_doc.get('scanner_stable_id')
                    if not isinstance(stable_id, str) or not stable_id:
                        continue
                    disabled_flag = raw_doc.get('disabled')
                    if disabled_flag is True:
                        continue
                    existing_enabled_ids.add(stable_id)
            except Exception:  # pragma: no cover - tolerate storage access failures
                LOGGER.debug('Failed to enumerate existing songs for missing rehydrate', exc_info=True)

        processed = 0
        inserted = 0
        updated = 0
        errors = 0
        operations: List[Tuple[Mapping[str, object], Mapping[str, object]]] = []
        now = datetime.now(UTC)

        def _flush_operations() -> None:
            nonlocal operations, inserted, updated, errors
            if not operations:
                return
            op_payloads = [
                UpdateOne(filter_doc, update_doc, upsert=True)
                for filter_doc, update_doc in operations
            ]
            bulk_callable = getattr(song_store, 'bulk_write', None)
            if callable(bulk_callable):
                try:
                    bulk_result = bulk_callable(op_payloads, ordered=False)
                except Exception:  # pragma: no cover - fall back on sequential execution
                    LOGGER.warning('Bulk rehydrate write failed; falling back to sequential mode', exc_info=True)
                else:
                    batch_inserted = int(getattr(bulk_result, 'upserted_count', 0) or getattr(bulk_result, 'inserted_count', 0) or 0)
                    batch_modified = int(getattr(bulk_result, 'modified_count', 0) or 0)
                    inserted += batch_inserted
                    updated += batch_modified
                    operations = []
                    return
            # Sequential fallback path
            for filter_doc, update_doc in operations:
                try:
                    result = song_store.update_one(filter_doc, update_doc, upsert=True)
                except Exception:  # pragma: no cover - tolerate transient write issues
                    LOGGER.warning(
                        'Failed to rehydrate song %s from manifest',
                        filter_doc.get('scanner_stable_id'),
                        exc_info=True,
                    )
                    errors += 1
                    continue
                if getattr(result, 'upserted_id', None) is not None:
                    inserted += 1
                elif getattr(result, 'matched_count', 0) and getattr(result, 'modified_count', 0):
                    updated += 1
            operations = []

        for raw_entry in cursor:
            if not isinstance(raw_entry, dict):
                continue
            entry = dict(raw_entry)
            stable_id = entry.get('id') or entry.get('_id')
            if not isinstance(stable_id, str) or not stable_id:
                continue
            if mode == 'missing' and stable_id in existing_enabled_ids:
                continue
            entry.pop('_id', None)
            raw_difficulties = entry.get('difficulties') if isinstance(entry.get('difficulties'), dict) else {}
            normalized_difficulties = {
                legacy: {'valid': bool(raw_difficulties.get(legacy))}
                for legacy in ('easy', 'normal', 'hard', 'oni', 'ura')
            }
            playable_count = sum(1 for payload in normalized_difficulties.values() if payload['valid'])
            category_value = entry.get('category')
            if not isinstance(category_value, str) or not category_value.strip():
                category_value = DEFAULT_CATEGORY_TITLE
            else:
                category_value = category_value.strip()
            title_value = str(entry.get('title') or '')
            subtitle_value = str(entry.get('subtitle') or '')
            raw_paths = entry.get('paths') if isinstance(entry.get('paths'), dict) else {}
            paths_payload: Dict[str, object] = {}
            for key, value in raw_paths.items():
                if value is None:
                    continue
                paths_payload[str(key)] = value
            enabled = not bool(entry.get('disabled', False))
            sha1_value = entry.get('sha1') if isinstance(entry.get('sha1'), str) else None
            hash_value = sha1_value or stable_id
            sanitized_entry = {key: value for key, value in entry.items() if key != '_id'}
            update_payload: Dict[str, object] = {
                'scanner_stable_id': stable_id,
                'title': title_value,
                'title_lc': title_value.casefold() if title_value else '',
                'subtitle': subtitle_value,
                'category': category_value,
                'category_id': 0,
                'type': str(entry.get('source_type') or 'tja'),
                'paths': paths_payload,
                'managed_by_scanner': True,
                'enabled': enabled,
                'disabled': not enabled,
                'is_playable': playable_count > 0,
                'valid_chart_count': playable_count,
                'valid_charts': playable_count,
                'difficulties': normalized_difficulties,
                'preview_available': bool(entry.get('preview_available')),
                'sha1': sha1_value,
                'hash': hash_value,
                'fingerprint': hash_value,
                'mtime': entry.get('mtime'),
                'duration_ms': int(entry.get('duration_ms') or 0),
                'titleNormalized': _normalise_title_key(title_value) if title_value else '',
                'scanner_manifest_snapshot': sanitized_entry,
                'scanner_rehydrated_at': now,
                'scanner_rehydrated_placeholder': True,
                'updated_at': now,
            }
            set_on_insert: Dict[str, object] = {
                'group_key': f'manifest:{stable_id}',
                'charts': [],
                'courses': {legacy: None for legacy in COURSE_LEGACY_MAP.values()},
                'import_issues': [],
                'managed_by_scanner': True,
            }
            update_document = {
                '$set': update_payload,
                '$setOnInsert': set_on_insert,
            }
            operations.append(({'scanner_stable_id': stable_id}, update_document))
            processed += 1
            if len(operations) >= 500:
                _flush_operations()

        _flush_operations()

        total_upserts = inserted + updated
        if total_upserts:
            self._metrics.increment('songs_upserted_total', total_upserts)
        if inserted:
            self._metrics.increment('songs_inserted_total', inserted)

        if songs_count_before is not None:
            summary.setdefault('songs_count_before', songs_count_before)

        songs_count_after = self._count_enabled_songs()
        if songs_count_after is not None:
            summary['songs_count_after'] = songs_count_after

        summary['found'] = processed
        summary['inserted'] = inserted
        summary['updated'] = updated
        summary['rehydrated'] = total_upserts
        summary['manifest_documents'] = manifest_documents
        summary['errors'] = summary.get('errors', 0) + errors
        summary.setdefault('disabled', 0)
        summary.setdefault('skipped', 0)

        consistent = (
            songs_count_after is None
            or manifest_documents == 0
            or (songs_count_after >= manifest_documents and manifest_documents > 0)
        )

        return {
            'processed': processed,
            'rehydrated': total_upserts,
            'inserted': inserted,
            'updated': updated,
            'errors': errors,
            'songs_count_after': songs_count_after,
            'consistent': consistent,
        }

    def _ensure_manifest_indexes(self) -> None:
        store = self._manifest_store
        if store is None:
            return
        try:
            store.create_index('title_lc', name='songs_manifest_title_lc')
        except Exception:  # pragma: no cover - index creation is best-effort
            LOGGER.debug('Failed to ensure songs manifest title index', exc_info=True)
        try:
            store.create_index('category', name='songs_manifest_category')
        except Exception:  # pragma: no cover - index creation is best-effort
            LOGGER.debug('Failed to ensure songs manifest category index', exc_info=True)
        try:
            store.create_index('id', unique=True, name='songs_manifest_id_unique')
        except Exception:  # pragma: no cover - tolerate index errors
            LOGGER.debug('Failed to ensure songs manifest id index', exc_info=True)

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

class _ScanLogCounter(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.warn_count = 0
        self.error_count = 0
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - simple counter
        with self._lock:
            if record.levelno >= logging.ERROR:
                self.error_count += 1
            elif record.levelno >= logging.WARNING:
                self.warn_count += 1


class _ScanMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters = {
            'songs_upserted_total': 0,
            'songs_upserted_new_total': 0,
            'songs_updated_legacy_total': 0,
            'songs_inserted_total': 0,
            'songs_seeded_legacy_total': 0,
            'invalid_group_key_total': 0,
            'duplicate_key_retries_total': 0,
            'charts_synced_total': 0,
            'tja_dojo_parsed_total': 0,
            'tja_notes_total': 0,
            'tja_unknown_directives_total': 0,
            'tja_skipped_charts_total': 0,
            'tja_mapped_course_total': 0,
            'tja_skipped_no_course_total': 0,
            'tja_skipped_unknown_course_total': 0,
        }
        self._last_logged = 0.0

    def increment(self, name: str, amount: int = 1) -> None:
        if name not in self._counters:
            return
        with self._lock:
            self._counters[name] += amount
            self._maybe_log_locked()

    def _maybe_log_locked(self) -> None:
        now = time.time()
        if now - self._last_logged < 1.0:
            return
        if all(value == 0 for value in self._counters.values()):
            self._last_logged = now
            return
        snapshot = {key: self._counters[key] for key in sorted(self._counters)}
        message = ", ".join(f"{key}={value}" for key, value in snapshot.items())
        LOGGER.info("scanner counters: %s", message)
        self._last_logged = now

    def flush(self) -> None:
        with self._lock:
            self._last_logged = 0.0
            self._maybe_log_locked()

