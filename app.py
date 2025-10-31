#!/usr/bin/env python3

import base64
import bcrypt
import hashlib
import importlib
import importlib.util
import json
import logging
import mimetypes
import os
import re
import sys
import threading
import signal
import time
import unicodedata
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, Optional, Sequence, cast
from urllib.parse import unquote, urlparse
from collections import defaultdict
from pathlib import Path, PurePosixPath

# -- カスタム --
from datetime import datetime, timedelta

import flask

# ----

from functools import lru_cache, wraps
from flask import (
    Flask,
    g,
    jsonify,
    render_template,
    request,
    abort,
    redirect,
    session,
    flash,
    make_response,
    send_from_directory,
    Response,
    current_app,
)
from flask_caching import Cache
from flask_compress import Compress
from flask_session import Session
from cachelib.file import FileSystemCache
from flask_wtf.csrf import CSRFProtect, generate_csrf, CSRFError
from ffmpy import FFmpeg
from redis import Redis

if TYPE_CHECKING:
    from pymongo import MongoClient

from songs_scanner import DEFAULT_CATEGORY_TITLE, SongScanner, empty_scan_summary
from tower_chart_selection import select_best_chart
from tower_chart_normalization import normalize_measures_relative
from modes_manifest import build_modes_manifest, DEFAULT_CACHE_TTL
from desktop_config import DESKTOP_CONFIG_ENV, resolve_songs_dir_from_config
from server.paths import (
    app_dir,
    data_dir,
    public_dir,
    songs_dir,
    is_desktop,
)
from tools.init_db_schema import init_db_schema
from storage.factory import StorageBundle, create_storage_bundle
from storage.interfaces import (
    LeaderLock as LeaderLockInterface,
    ManifestStore as ManifestStoreInterface,
    SongStore as SongStoreInterface,
)


LOGGER = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _mongo_return_document_after() -> int:
    try:
        from pymongo import ReturnDocument as _ReturnDocument
    except Exception as exc:
        raise RuntimeError("pymongo is not available; Mongo features require pymongo") from exc
    return cast(int, getattr(_ReturnDocument, "AFTER", 1))


def resource_path(*parts: str) -> str:
    if not parts:
        raise ValueError("resource_path requires at least one component")

    def _normalise(items: Iterable[str]) -> tuple[str, ...]:
        components: list[str] = []
        for item in items:
            if not item:
                continue
            components.extend(Path(item).parts)
        return tuple(components)

    relative_parts = _normalise(parts)
    if not relative_parts:
        raise ValueError("resource_path resolved to empty path")

    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    project_root = Path(__file__).resolve().parent
    candidates = [base.joinpath(*relative_parts), project_root.joinpath(*relative_parts)]

    if relative_parts[0] == "web" and len(relative_parts) > 1:
        candidates.append(project_root.joinpath(*relative_parts[1:]))

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    return str(candidates[0])


TEMPLATES_DIR = os.getenv("TAIKO_TEMPLATES_DIR") or resource_path("web", "templates")
if is_desktop():
    STATIC_DIR = str(public_dir())
else:
    _static_candidate = os.getenv("TAIKO_STATIC_DIR") or resource_path("web", "static")
    if not Path(_static_candidate).exists():
        _static_candidate = resource_path("public")
    STATIC_DIR = _static_candidate


def _frontend_payload_status(root: Path) -> tuple[bool, list[str]]:
    """Return a tuple describing whether the frontend bundle is usable."""

    issues: list[str] = []
    try:
        root_exists = root.exists()
    except Exception:
        root_exists = False
    if not root_exists:
        issues.append(f"missing directory: {root}")
        return False, issues

    if (root / "index.html").is_file():
        return True, issues

    views_dir = root / "src" / "views"
    if not views_dir.exists():
        issues.append(f"missing directory: {views_dir}")
        return False, issues

    html_exists = any(child.suffix.lower() == ".html" for child in views_dir.glob("*.html"))
    if not html_exists:
        issues.append(f"missing *.html files in {views_dir}")
        return False, issues

    return True, issues


def _resolve_frontend_dir() -> tuple[Path, tuple[Path, ...]]:
    if is_desktop():
        frontend_public_dir = public_dir()
        return frontend_public_dir, (frontend_public_dir,)
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    resource_candidates = [
        Path(resource_path("taiko-web-backend", "_internal", "public")),
        Path(resource_path("taiko_web_backend", "_internal", "public")),
        Path(resource_path("client", "build")),
        Path(resource_path("web", "frontend")),
        Path(resource_path("public")),
    ]
    raw_candidates = [
        base / "taiko_web_backend" / "_internal" / "public",
        base / "taiko-web-backend" / "_internal" / "public",
        *resource_candidates,
    ]
    candidates: list[Path] = []
    seen: set[str] = set()
    for raw_candidate in raw_candidates:
        try:
            resolved = Path(raw_candidate).resolve()
        except Exception:
            resolved = Path(raw_candidate)
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(resolved)

    if not candidates:
        raise RuntimeError("No frontend directory candidates available")

    failure_reasons: list[str] = []
    for candidate in candidates:
        try:
            status, issues = _frontend_payload_status(candidate)
        except TypeError:
            continue
        if status:
            return candidate, tuple(candidates)
        description = ", ".join(issues) if issues else "incomplete payload"
        failure_reasons.append(f"{candidate}: {description}")

    LOGGER.error(
        "frontend_dir=missing; details=%s",
        failure_reasons or [str(candidate) for candidate in candidates],
    )
    return candidates[0], tuple(candidates)


FRONTEND_DIR, FRONTEND_DIR_CANDIDATES = _resolve_frontend_dir()
_FRONTEND_WARNING_EMITTED = False

PUBLIC_DIR_PATH = public_dir()


def JSONResponse(
    *,
    content: Any,
    status_code: int = 200,
    headers: Optional[Mapping[str, str]] = None,
    media_type: str = "application/json",
) -> Response:
    response = jsonify(content)
    response.status_code = status_code
    # Ensure the negotiated content type is explicit for downstream health checks.
    response.headers["Content-Type"] = media_type
    if headers:
        for key, value in headers.items():
            response.headers[key] = value
    return response


RUN_PROFILE = os.getenv("PROFILE") or os.getenv("RUN_PROFILE", "web")
if is_desktop():
    RUN_PROFILE = "desktop"


_startup_scan_started_at: Optional[float] = None
_startup_scan_logged = False


class _ResourceStore:
    def __init__(self, key: str):
        self._key = key
        self._thread_local = threading.local()

    def _thread_store(self) -> dict:
        store = getattr(self._thread_local, "store", None)
        if store is None:
            store = {}
            self._thread_local.store = store
        return store

    def _app_store(self) -> dict:
        if not flask.has_app_context():
            return {}
        app = flask.current_app
        resources = app.extensions.setdefault("taiko_resources", {})
        return resources

    def _store(self) -> dict:
        if flask.has_app_context():
            return self._app_store()
        return self._thread_store()

    def get(self):
        return self._store().get(self._key)

    def set(self, value) -> None:
        self._store()[self._key] = value

    def pop(self):
        store = self._store()
        return store.pop(self._key, None)


class _LazyResourceProxy:
    _MISSING = object()

    def __init__(self, factory: Callable[[], object]):
        self._factory = factory
        self._instance = self._MISSING
        self._lock = threading.Lock()

    def _get_instance(self):
        instance = self._instance
        if instance is self._MISSING:
            with self._lock:
                instance = self._instance
                if instance is self._MISSING:
                    instance = self._factory()
                    self._instance = instance
        return instance

    def __getattr__(self, item):
        return getattr(self._get_instance(), item)

    def __getitem__(self, key):
        return self._get_instance()[key]

    def __bool__(self) -> bool:
        return bool(self._get_instance())

    def __repr__(self) -> str:
        return f"<LazyResourceProxy factory={self._factory!r}>"


DESKTOP_MONGO_UNAVAILABLE_MESSAGE = (
    'Mongo-backed features are not available in the desktop profile.'
)


def _desktop_mongo_unavailable_response(*, api: bool) -> Optional[Response]:
    if RUN_PROFILE != 'desktop':
        return None
    if not flask.has_request_context():
        return None
    request_path = getattr(request, 'path', '') or ''
    normalized_path = request_path.rstrip('/') or '/'
    if normalized_path.endswith('/api/modes') or normalized_path.endswith('/api/categories'):
        return None
    LOGGER.debug(
        'desktop profile requested mongo-backed feature path=%s api=%s',
        request.path,
        api,
    )
    if api:
        payload = jsonify(
            {
                'status': 'error',
                'message': 'desktop_profile_feature_unavailable',
            }
        )
        payload.status_code = 503
        return payload
    response = make_response(DESKTOP_MONGO_UNAVAILABLE_MESSAGE, 503)
    response.mimetype = 'text/plain'
    return response


class MongoDispatcher:
    def __init__(self, client_factory: Callable[[], "MongoClient"], db_name: str):
        self._client_factory = client_factory
        self._db_name = db_name
        self._store = _ResourceStore("mongo_client")
        self._lock = threading.Lock()

    def _refresh_client(self, holder):
        if holder is not None:
            _, client = holder
            try:
                client.close()
            except Exception:
                LOGGER.debug("failed to close stale MongoClient", exc_info=True)

    def get_client(self) -> "MongoClient":
        holder = self._store.get()
        pid = os.getpid()
        if holder is None or holder[0] != pid:
            with self._lock:
                holder = self._store.get()
                if holder is None or holder[0] != pid:
                    self._refresh_client(holder)
                    client = self._client_factory()
                    holder = (pid, client)
                    self._store.set(holder)
        return holder[1]

    def get_database(self):
        return self.get_client()[self._db_name]

    def clear(self) -> None:
        holder = self._store.pop()
        if holder is None:
            return
        _, client = holder
        try:
            client.close()
        except Exception:
            LOGGER.debug("failed to close MongoClient during clear", exc_info=True)


class RedisDispatcher:
    def __init__(self, redis_factory: Callable[[], Redis]):
        self._redis_factory = redis_factory
        self._store = _ResourceStore("redis_client")
        self._lock = threading.Lock()

    def _refresh_client(self, holder):
        if holder is not None:
            _, client = holder
            close = getattr(client, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    LOGGER.debug("failed to close stale Redis client", exc_info=True)

    def get_client(self) -> Redis:
        holder = self._store.get()
        pid = os.getpid()
        if holder is None or holder[0] != pid:
            with self._lock:
                holder = self._store.get()
                if holder is None or holder[0] != pid:
                    self._refresh_client(holder)
                    client = self._redis_factory()
                    holder = (pid, client)
                    self._store.set(holder)
        return holder[1]

    def clear(self) -> None:
        holder = self._store.pop()
        if holder is None:
            return
        _, client = holder
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                LOGGER.debug("failed to close Redis client during clear", exc_info=True)


class SongScannerProvider:
    def __init__(self, factory: Callable[[], SongScanner]):
        self._factory = factory
        self._store = _ResourceStore("song_scanner")
        self._lock = threading.Lock()

    def get(self) -> SongScanner:
        holder = self._store.get()
        pid = os.getpid()
        if holder is None or holder[0] != pid:
            with self._lock:
                holder = self._store.get()
                if holder is None or holder[0] != pid:
                    scanner = self._factory()
                    holder = (pid, scanner)
                    self._store.set(holder)
        return holder[1]

    def clear(self) -> None:
        holder = self._store.pop()
        if holder is None:
            return
        _, scanner = holder
        closer = getattr(scanner, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                LOGGER.debug("failed to close SongScanner during clear", exc_info=True)


_mongo_dispatcher: Optional[MongoDispatcher] = None
_redis_dispatcher: Optional[RedisDispatcher] = None
_song_scanner_provider: Optional[SongScannerProvider] = None

_storage_bundle: Optional[StorageBundle] = None
SONG_STORE: Optional[SongStoreInterface] = None
MANIFEST_STORE: Optional[ManifestStoreInterface] = None
LEADER_LOCK: Optional[LeaderLockInterface] = None


def setup_stdout_logging() -> None:
    root = logging.getLogger()
    server_software = os.getenv("SERVER_SOFTWARE", "")
    is_gunicorn = "gunicorn" in server_software.lower()

    if not is_gunicorn:
        for h in list(root.handlers):
            root.removeHandler(h)
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        ))
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    else:
        # <<< вот это ключевое под gunicorn >>>
        gunicorn_logger = logging.getLogger("gunicorn.error")
        if gunicorn_logger.handlers:
            root.handlers = gunicorn_logger.handlers
            root.setLevel(gunicorn_logger.level)
        else:
            # запасной план: если вдруг нет хэндлеров у gunicorn
            handler = logging.StreamHandler(stream=sys.stdout)
            handler.setFormatter(logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                "%Y-%m-%d %H:%M:%S",
            ))
            root.addHandler(handler)
            root.setLevel(logging.INFO)

    for name in ("taiko.scanner", "taiko.scanner.summary"):
        lg = logging.getLogger(name)
        lg.propagate = True
        if lg.level == logging.NOTSET:
            lg.setLevel(logging.INFO)

client = None
db = None
basedir = '/'
SCAN_ON_START = 'auto'
ENABLE_SONG_WATCHER = True
SCAN_IGNORE_GLOBS: list[str] = []
ADMIN_SCAN_TOKEN = ''
SONGS_BASEURL_VALUE = ''
COERCE_UNKNOWN_COURSE = None
SONGS_DIR_PATH = songs_dir() if RUN_PROFILE == 'desktop' else Path('.')
song_scanner: Optional[SongScanner] = None
_song_watcher_handle = None


def _resolve_baseurl(value):
    if not value:
        return '/songs/'
    if value.startswith('http://') or value.startswith('https://') or value.startswith('/'):
        return value if value.endswith('/') else value + '/'
    resolved = basedir + value
    return resolved if resolved.endswith('/') else resolved + '/'


_FEATURE_MODES_MANIFEST_ENV = "FEATURE_MODES_MANIFEST"
_MODES_MANIFEST_CACHE: dict[str, object] = {"expires_at": 0.0, "payload": None}


def _coerce_int(value: object, default: int = 0) -> int:
    try:
        number = int(round(float(value)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return number


def _get_song_store() -> Optional[SongStoreInterface]:
    override_store = None
    if not isinstance(db, _LazyResourceProxy):
        override_store = getattr(db, 'songs', None)
    if override_store is not None:
        return cast(SongStoreInterface, override_store)
    return SONG_STORE


def _require_song_store() -> SongStoreInterface:
    store = _get_song_store()
    if store is None:
        raise RuntimeError('Song store is not configured')
    return store


def _get_manifest_store() -> Optional[ManifestStoreInterface]:
    return MANIFEST_STORE


def _load_song_document_for_identifier(
    identifier: str,
    *,
    projection: Optional[Mapping[str, Any]] = None,
    song_store: Optional[SongStoreInterface] = None,
) -> Optional[Mapping[str, Any]]:
    store = song_store or _require_song_store()
    document: Optional[Mapping[str, Any]] = None
    get_by_id = getattr(store, 'get_by_id', None)
    if callable(get_by_id):
        try:
            document = get_by_id(identifier)
        except Exception:
            app.logger.debug(
                'song lookup via get_by_id failed id=%s store=%s',
                identifier,
                type(store).__name__,
                exc_info=app.logger.isEnabledFor(logging.DEBUG),
            )
            document = None
        if document:
            return dict(document)

    lookup_keys = ('song_id', 'scanner_stable_id')
    for key in lookup_keys:
        filter_doc = {key: identifier}
        try:
            document = store.find_one(filter_doc, projection=projection)
        except Exception:
            app.logger.debug(
                'song lookup via find_one failed filter=%s store=%s',
                filter_doc,
                type(store).__name__,
                exc_info=app.logger.isEnabledFor(logging.DEBUG),
            )
            document = None
        if document:
            return dict(document)
    return None


def _normalize_document_identifier(value: object) -> Optional[str]:
    if isinstance(value, str):
        token = value.strip()
        return token or None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        text = str(int(value)) if isinstance(value, (int, float)) else str(value)
        token = text.strip()
        return token or None
    return None


def _normalize_category_title(value: object) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token:
            return token
    return ''


def _coerce_category_id(value: object) -> Optional[int]:
    if isinstance(value, bool):  # guard against True/False being treated as 1/0
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return int(token)
        except ValueError:
            return None
    return None


def _merge_category_documents(target: dict, source: Mapping[str, object]) -> None:
    source_id = _coerce_category_id(source.get('id') or source.get('category_id'))
    if source_id is not None and _coerce_category_id(target.get('id')) is None:
        target['id'] = source_id

    title = _normalize_category_title(source.get('title') or source.get('category') or source.get('name'))
    if title and not _normalize_category_title(target.get('title')):
        target['title'] = title

    aliases = source.get('aliases')
    if aliases and not target.get('aliases') and isinstance(aliases, list):
        target['aliases'] = aliases

    title_lang = source.get('title_lang') or source.get('titleLang')
    if title_lang and not target.get('title_lang') and isinstance(title_lang, Mapping):
        target['title_lang'] = title_lang

    song_skin = source.get('song_skin') or source.get('songSkin')
    if song_skin and not target.get('song_skin') and isinstance(song_skin, Mapping):
        target['song_skin'] = song_skin

    items = source.get('items')
    if items and not target.get('items') and isinstance(items, list):
        target['items'] = items


def _collect_desktop_categories() -> list[dict[str, object]]:
    store = _get_song_store()
    if store is None:
        return [
            {'id': 0, 'title': DEFAULT_CATEGORY_TITLE, 'song_skin': None},
        ]

    manifest_categories: dict[str, str] = {}
    manifest_store = _get_manifest_store()
    if manifest_store is not None:
        manifest_projection = {'_id': 1, 'category': 1}
        try:
            cursor = manifest_store.find({'_id': {'$ne': '__meta__'}}, projection=manifest_projection)
        except TypeError:
            cursor = manifest_store.find({'_id': {'$ne': '__meta__'}}, manifest_projection)
        except Exception:
            app.logger.debug('Failed to enumerate manifest categories', exc_info=True)
            cursor = []
        for manifest_doc in cursor:
            if not isinstance(manifest_doc, Mapping):
                continue
            manifest_id = manifest_doc.get('_id')
            if not isinstance(manifest_id, str) or not manifest_id:
                continue
            manifest_title = _normalize_category_title(manifest_doc.get('category'))
            if manifest_title:
                manifest_categories[manifest_id] = manifest_title

    projection = {
        'category_id': 1,
        'category': 1,
        'title': 1,
        'name': 1,
        'song_skin': 1,
        'songSkin': 1,
        'aliases': 1,
        'title_lang': 1,
        'titleLang': 1,
        'items': 1,
        'id': 1,
        'meta': 1,
        'scanner_stable_id': 1,
        'song_id': 1,
    }

    try:
        cursor = store.find({}, projection=projection)
    except TypeError:
        cursor = store.find({}, projection)
    except Exception:
        app.logger.debug('Failed to enumerate categories from song store', exc_info=True)
        cursor = []

    categories: dict[tuple[object, str], dict[str, object]] = {}

    for raw_doc in cursor:
        if not isinstance(raw_doc, Mapping):
            continue
        metadata = raw_doc.get('meta') if isinstance(raw_doc.get('meta'), Mapping) else {}
        stable_id = raw_doc.get('scanner_stable_id')
        if not isinstance(stable_id, str) or not stable_id:
            fallback = raw_doc.get('id') or raw_doc.get('song_id')
            stable_id = fallback if isinstance(fallback, str) else ''
        category_title = _normalize_category_title(
            raw_doc.get('category')
            or metadata.get('category')
            or metadata.get('category_title')
            or (manifest_categories.get(stable_id) if stable_id else '')
            or raw_doc.get('title')
            or raw_doc.get('name')
        )
        if not category_title:
            continue
        category_id = _coerce_category_id(
            raw_doc.get('category_id')
            or metadata.get('category_id')
            or raw_doc.get('id')
        )
        key: tuple[object, str]
        if category_id is not None:
            key = ('id', category_id)
        else:
            key = ('title', category_title.casefold())

        existing = categories.get(key)
        if existing is None:
            base_entry: dict[str, object] = {
                'id': category_id if category_id is not None else None,
                'title': category_title,
            }
            song_skin = raw_doc.get('song_skin') or raw_doc.get('songSkin')
            if isinstance(song_skin, Mapping):
                base_entry['song_skin'] = song_skin
            else:
                base_entry['song_skin'] = None
            aliases = raw_doc.get('aliases')
            if isinstance(aliases, list):
                base_entry['aliases'] = aliases
            title_lang = raw_doc.get('title_lang') or raw_doc.get('titleLang')
            if isinstance(title_lang, Mapping):
                base_entry['title_lang'] = title_lang
            items = raw_doc.get('items')
            if isinstance(items, list):
                base_entry['items'] = items
            categories[key] = base_entry
        else:
            _merge_category_documents(existing, raw_doc)
            if isinstance(metadata, Mapping):
                _merge_category_documents(existing, metadata)

    for manifest_title in manifest_categories.values():
        normalized_title = _normalize_category_title(manifest_title)
        if not normalized_title:
            continue
        key = ('title', normalized_title.casefold())
        if key not in categories:
            categories[key] = {
                'id': None,
                'title': manifest_title,
                'song_skin': None,
            }

    if not categories:
        return [
            {'id': 0, 'title': DEFAULT_CATEGORY_TITLE, 'song_skin': None},
        ]

    have_default = any(
        isinstance(entry.get('id'), int) and entry.get('id') == 0
        or _normalize_category_title(entry.get('title')) == DEFAULT_CATEGORY_TITLE
        for entry in categories.values()
    )
    if not have_default:
        categories[('id', 0)] = {
            'id': 0,
            'title': DEFAULT_CATEGORY_TITLE,
            'song_skin': None,
        }

    ordered = sorted(
        categories.values(),
        key=lambda entry: (
            0 if isinstance(entry.get('id'), int) else 1,
            entry.get('id') if isinstance(entry.get('id'), int) else entry.get('title', '').casefold(),
        ),
    )

    return ordered


def _normalize_categories_payload(categories: Iterable[object]) -> list[dict[str, object]]:
    if categories is None:
        return []

    if isinstance(categories, Mapping):
        source_iterable: Iterable[object] = categories.values()
    else:
        source_iterable = categories

    try:
        iterator = iter(source_iterable)
    except TypeError:
        return []

    normalized: list[dict[str, object]] = []

    for raw_entry in iterator:
        if not isinstance(raw_entry, Mapping):
            continue

        entry = dict(raw_entry)
        metadata = entry.get('meta')
        if isinstance(metadata, Mapping):
            _merge_category_documents(entry, metadata)
        entry.pop('meta', None)

        category_id = _coerce_category_id(entry.get('id'))
        if category_id is None:
            category_id = _coerce_category_id(entry.get('category_id'))

        title_value = _normalize_category_title(
            entry.get('title')
            or entry.get('category')
            or entry.get('name')
        )
        if not title_value:
            fallback = entry.get('slug') or entry.get('scanner_stable_id') or entry.get('id')
            if isinstance(fallback, str):
                token = fallback.strip()
                if token:
                    title_value = token
        if not title_value:
            continue

        count_value = entry.get('count')
        if count_value is None:
            normalized_count = 0
        else:
            normalized_count = _coerce_int(count_value, 0)
        if normalized_count <= 0:
            items_value = entry.get('items')
            if isinstance(items_value, Sequence) and not isinstance(items_value, (str, bytes)):
                normalized_count = sum(1 for item in items_value if item is not None)

        aliases_value = entry.get('aliases')
        if not isinstance(aliases_value, list):
            aliases_value = []

        title_lang_value = entry.get('title_lang') or entry.get('titleLang')
        if not isinstance(title_lang_value, Mapping):
            title_lang_value = {}

        song_skin_value = entry.get('song_skin') or entry.get('songSkin')
        if not isinstance(song_skin_value, Mapping):
            song_skin_value = None

        normalized_entry: dict[str, object] = {
            'title': title_value,
            'aliases': aliases_value,
            'title_lang': title_lang_value,
            'song_skin': song_skin_value,
            'count': normalized_count,
        }
        if category_id is not None:
            normalized_entry['id'] = category_id

        normalized.append(normalized_entry)

    return normalized


def _load_categories_documents_for_profile() -> tuple[Optional[Response], list[dict[str, object]]]:
    if RUN_PROFILE == 'desktop':
        documents = list(_collect_desktop_categories())
        return None, documents

    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable, []

    projection = {'_id': False}
    documents: list[dict[str, object]] = []
    try:
        try:
            cursor = db.categories.find({}, projection)
        except TypeError:
            cursor = db.categories.find({}, {'_id': False})
        for doc in cursor:
            if isinstance(doc, Mapping):
                documents.append(dict(doc))
    except Exception:
        documents = []

    return None, documents


def _load_manifest_meta() -> Optional[dict]:
    store = _get_manifest_store()
    if store is None:
        return None
    try:
        meta = store.find_one({'_id': '__meta__'})
        if isinstance(meta, Mapping):
            return dict(meta)
    except Exception:
        app.logger.debug('Failed to load songs manifest meta', exc_info=True)
    return None


def _normalize_if_none_match(header_value: Optional[str]) -> Optional[str]:
    if not header_value:
        return None
    token = header_value.strip()
    if token.startswith('W/'):
        token = token[2:].strip()
    if len(token) >= 2 and token[0] == token[-1] == '"':
        token = token[1:-1]
    return token or None


def _normalize_difficulties(entry, assume_valid=False):
    src = entry.get('difficulties') if isinstance(entry, dict) else None
    if not isinstance(src, dict):
        src = {}
    out = {}
    for name in ('easy', 'normal', 'hard', 'oni', 'ura'):
        val = src.get(name)
        if isinstance(val, dict):
            d = dict(val)
            d['valid'] = bool(val.get('valid', True))
            out[name] = d
        elif val is True:
            out[name] = {'valid': True}
        elif isinstance(val, (int, float)) and not isinstance(val, bool):
            out[name] = {'stars': int(val), 'valid': True}
    if not out and assume_valid:
        out['oni'] = {'valid': True}
    return out


def _apply_catalog_cache_headers(response: 'flask.Response', *, etag: Optional[str], cache_control: str, vary: str) -> None:
    if etag:
        response.headers['ETag'] = etag
    response.headers['Cache-Control'] = cache_control
    vary_tokens = []
    existing_vary = response.headers.get('Vary')
    if existing_vary:
        vary_tokens.extend(token.strip() for token in existing_vary.split(',') if token.strip())
    vary_tokens.extend(token.strip() for token in vary.split(',') if token.strip())
    normalised: list[str] = []
    seen: set[str] = set()
    for token in vary_tokens:
        lowered = token.lower()
        if lowered == 'cookie':
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        normalised.append(token)
    if normalised:
        response.headers['Vary'] = ', '.join(normalised)
    elif 'Vary' in response.headers:
        del response.headers['Vary']


def _ensure_chart_duration(chart_data: dict) -> None:
    if not isinstance(chart_data, dict):
        return
    measures = chart_data.get("measures")
    if not isinstance(measures, list):
        chart_data["duration_ms"] = max(0, _coerce_int(chart_data.get("duration_ms"), 0))
        return

    max_time = 0
    for measure in measures:
        if not isinstance(measure, dict):
            continue
        start_ms = _coerce_int(measure.get("start_ms"), 0)
        duration_ms = _coerce_int(measure.get("duration_ms"), 0)
        measure_end = start_ms + max(0, duration_ms)
        if measure_end > max_time:
            max_time = measure_end

        notes = measure.get("notes") if isinstance(measure.get("notes"), list) else []
        for note in notes:
            if not isinstance(note, dict):
                continue
            offset_value = note.get("at") if "at" in note else note.get("offset")
            absolute = start_ms + max(0, _coerce_int(offset_value, 0))
            if absolute > max_time:
                max_time = absolute

        longs = measure.get("longs") if isinstance(measure.get("longs"), list) else []
        for long_note in longs:
            if not isinstance(long_note, dict):
                continue
            offset = _coerce_int(long_note.get("at"), 0)
            absolute = start_ms + max(0, offset)
            end_at_value = long_note.get("end_at")
            if end_at_value is not None:
                end_absolute = start_ms + max(offset, _coerce_int(end_at_value, offset))
            else:
                length = _coerce_int(long_note.get("len_ms"), 0)
                end_absolute = absolute + max(0, length)
            if end_absolute > max_time:
                max_time = end_absolute

    duration_value = _coerce_int(chart_data.get("duration_ms"), 0)
    chart_data["duration_ms"] = max(duration_value, max_time, 0)


def _parse_bool_env(value: str) -> bool:
    token = value.strip().lower()
    return token not in {"0", "false", "no", "off"}


def _resolve_catalog_assume_valid() -> bool:
    env_value = os.environ.get("CATALOG_ASSUME_VALID")
    if env_value is not None:
        try:
            return _parse_bool_env(str(env_value))
        except AttributeError:
            return bool(env_value)
    config_value = getattr(config, "CATALOG_ASSUME_VALID", None)
    if config_value is not None:
        if isinstance(config_value, str):
            try:
                return _parse_bool_env(config_value)
            except AttributeError:
                return bool(config_value)
        try:
            return bool(int(config_value))
        except (TypeError, ValueError):
            return bool(config_value)
    return False


def _normalize_catalog_source_token(value: object) -> Optional[str]:
    if value is None:
        return None
    token = str(value).strip().lower()
    if not token:
        return None
    mapping = {
        "mongo": "mongo",
        "mongodb": "mongo",
        "db": "mongo",
        "filesystem": "filesystem",
        "fs": "filesystem",
        "file": "filesystem",
        "file_system": "filesystem",
        "sqlite": "sqlite",
        "sql": "sqlite",
    }
    return mapping.get(token)


def _host_value_is_configured(value: object) -> bool:
    if isinstance(value, str):
        tokens = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        tokens = [str(item).strip() for item in value if str(item).strip()]
    else:
        return False
    if not tokens:
        return False
    default_hosts = {"127.0.0.1:27017", "localhost:27017"}
    return any(token not in default_hosts for token in tokens)


def _has_valid_mongo_dsn(*, config_module: object) -> bool:
    string_candidates = [
        os.environ.get("CATALOG_MONGO_DSN"),
        os.environ.get("MONGO_DSN"),
        os.environ.get("TAIKO_WEB_MONGO_URI"),
        os.environ.get("MONGO_URI"),
    ]
    config_mongo = getattr(config_module, "MONGO", None)
    if isinstance(config_mongo, Mapping):
        string_candidates.extend([
            config_mongo.get("uri"),
            config_mongo.get("dsn"),
        ])
        host_candidate = config_mongo.get("host")
    else:
        host_candidate = None
    for candidate in string_candidates:
        if isinstance(candidate, str) and candidate.strip():
            return True
    host_env_names = ("TAIKO_WEB_MONGO_HOST", "MONGO_HOST", "MONGO_HOSTS")
    if any(isinstance(os.environ.get(name), str) and os.environ.get(name).strip() for name in host_env_names):
        return True
    if _host_value_is_configured(host_candidate):
        return True
    return False


def _resolve_catalog_source(*, run_profile: str, config_module: object) -> str:
    explicit_env_raw = os.environ.get("CATALOG_SOURCE")
    explicit_env = _normalize_catalog_source_token(explicit_env_raw)
    if explicit_env:
        return explicit_env
    if explicit_env_raw and explicit_env is None:
        LOGGER.warning("Unknown CATALOG_SOURCE=%s; defaulting to mongo", explicit_env_raw)
    legacy_env = os.environ.get("USE_MONGO_CATALOG")
    if legacy_env is not None:
        try:
            return "mongo" if _parse_bool_env(str(legacy_env)) else "filesystem"
        except AttributeError:
            return "mongo" if legacy_env else "filesystem"
    config_override = _normalize_catalog_source_token(
        getattr(config_module, "CATALOG_SOURCE", None)
    )
    if config_override:
        return config_override
    legacy_config = getattr(config_module, "USE_MONGO_CATALOG", None)
    if legacy_config is not None:
        return "mongo" if bool(legacy_config) else "filesystem"
    if run_profile == "desktop":
        return "sqlite"
    return "mongo"


def is_modes_manifest_enabled() -> bool:
    env_value = os.environ.get(_FEATURE_MODES_MANIFEST_ENV)
    if env_value is not None:
        try:
            return _parse_bool_env(env_value)
        except AttributeError:
            return bool(env_value)
    config_value = getattr(config, "FEATURE_MODES_MANIFEST", None)
    if config_value is not None:
        return bool(config_value)
    return True


def _load_config_module():
    """Load configuration module from several possible locations."""

    module_name = os.environ.get("TAIKO_WEB_CONFIG_MODULE")
    search_order = []
    if module_name:
        search_order.append(module_name)
    search_order.extend(["config.config", "config"])

    for name in search_order:
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError:
            continue

    path_candidates = [
        Path(os.environ.get("TAIKO_WEB_CONFIG_PATH", "config.py")),
        Path("config/config.py"),
    ]
    for config_path in path_candidates:
        if not config_path.exists():
            continue
        spec = importlib.util.spec_from_file_location("config", config_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
            return module

    raise FileNotFoundError('No such file or directory: \'config.py\'. Copy the example config file config.example.py to config.py')


config = _load_config_module()

CATALOG_SOURCE = _resolve_catalog_source(run_profile=RUN_PROFILE, config_module=config)
try:
    setattr(config, "CATALOG_SOURCE", CATALOG_SOURCE)
except Exception:
    LOGGER.debug("Failed to set config.CATALOG_SOURCE", exc_info=True)
CATALOG_ASSUME_VALID = _resolve_catalog_assume_valid()
CATALOG_ASSUME_VALID_INT = 1 if CATALOG_ASSUME_VALID else 0

mimetypes.add_type("audio/ogg", ".ogg")
mimetypes.add_type("audio/mpeg", ".mp3")

def take_config(name, required=False):
    if hasattr(config, name):
        return getattr(config, name)
    if required:
        raise ValueError('Required option is not defined in the config.py file: {}'.format(name))
    return None

compress = Compress()
session_manager = Session()


def create_app():
    global client, db, basedir, SCAN_ON_START, ENABLE_SONG_WATCHER, SCAN_IGNORE_GLOBS
    global ADMIN_SCAN_TOKEN, SONGS_BASEURL_VALUE, COERCE_UNKNOWN_COURSE, SONGS_DIR_PATH
    global song_scanner, _song_watcher_handle
    global _mongo_dispatcher, _redis_dispatcher, _song_scanner_provider
    global _startup_scan_started_at, _startup_scan_logged, _FRONTEND_WARNING_EMITTED

    setup_stdout_logging()

    _startup_scan_started_at = time.monotonic()
    _startup_scan_logged = False

    app_instance = Flask(
        __name__,
        template_folder=TEMPLATES_DIR,
        static_folder=STATIC_DIR,
        static_url_path="/static",
    )
    app_instance.logger.info(
        "run_profile=%s catalog_source=%s",
        RUN_PROFILE,
        CATALOG_SOURCE,
    )
    try:
        frontend_dir_resolved = FRONTEND_DIR.resolve()
    except Exception:
        frontend_dir_resolved = FRONTEND_DIR
    frontend_ready, frontend_issues = _frontend_payload_status(frontend_dir_resolved)
    if frontend_ready:
        app_instance.logger.info("frontend_dir=%s", frontend_dir_resolved)
    else:
        candidate_strings = [str(path) for path in FRONTEND_DIR_CANDIDATES]
        details = frontend_issues or ["frontend payload incomplete"]
        app_instance.logger.warning(
            "frontend_dir=missing candidates=%s issues=%s",
            candidate_strings,
            details,
        )
        _FRONTEND_WARNING_EMITTED = True
    app_instance.config['RUN_PROFILE'] = RUN_PROFILE
    app_instance.config.setdefault('COMPRESS_MIN_SIZE', 1024)
    compress.init_app(app_instance)

    mongo_config = take_config('MONGO') or {}

    mongo_uri_candidates = [
        os.environ.get("TAIKO_WEB_MONGO_URI"),
        os.environ.get("MONGO_URI"),
        os.environ.get("MONGO_URL"),
        mongo_config.get('uri'),
        mongo_config.get('url'),
    ]
    mongo_uri = next((value for value in mongo_uri_candidates if isinstance(value, str) and value.strip()), None)

    mongo_host_candidates: list[object] = []
    for env_name in ("TAIKO_WEB_MONGO_HOST", "MONGO_HOST", "MONGO_HOSTS"):
        env_value = os.environ.get(env_name)
        if env_value:
            mongo_host_candidates.append(env_value)
            break
    mongo_host_candidates.append(mongo_config.get('host'))
    mongo_host_candidates.append(mongo_config.get('hosts'))

    mongo_host: object | None = next(
        (value for value in mongo_host_candidates if value),
        None,
    )

    if not mongo_uri and not mongo_host:
        mongo_hosts = ['mongo:27017']
    else:
        mongo_hosts = mongo_host or ['127.0.0.1:27017']

    def _create_mongo_client() -> "MongoClient":
        from pymongo import MongoClient

        if mongo_uri:
            return MongoClient(mongo_uri)
        return MongoClient(host=mongo_hosts)

    app_instance.config['MONGO_URI'] = mongo_uri
    app_instance.config['MONGO_HOSTS'] = mongo_hosts
    app_instance.config['MONGO_CLIENT'] = None
    app_instance.config['MONGO_CLIENT_FACTORY'] = _create_mongo_client
    app_instance.config['REDIS_CLIENT'] = None
    app_instance.config['REDIS_CLIENT_FACTORY'] = None

    basedir_value = os.environ.get('BASEDIR') or take_config('BASEDIR') or '/'

    app_instance.secret_key = take_config('SECRET_KEY') or 'change-me'

    session_backend = 'redis'
    session_redis = None
    cache_config: Mapping[str, object]
    desktop_data_dir: Optional[Path] = None
    session_initialized = False

    if RUN_PROFILE == 'desktop':
        desktop_app_dir = app_dir()
        desktop_songs_dir = songs_dir()
        desktop_data_dir = data_dir()

        desktop_app_dir.mkdir(parents=True, exist_ok=True)
        desktop_data_dir.mkdir(parents=True, exist_ok=True)

        sessions_directory = desktop_data_dir / 'sessions'
        sessions_directory.mkdir(parents=True, exist_ok=True)

        db_file = desktop_data_dir / 'taiko.db'
        app_instance.config['APP_DIR'] = str(desktop_app_dir)
        app_instance.config['DATA_DIR'] = str(desktop_data_dir)
        app_instance.config['SQLITE_PATH'] = str(db_file)
        app_instance.config['SQLITE_DB_PATH'] = str(db_file)

        try:
            desktop_songs_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            app_instance.logger.warning('Failed to ensure songs directory at startup', exc_info=True)

        lifetime_seconds = int(
            os.getenv(
                'SESSION_TTL_SECONDS',
                str(int(timedelta(days=7).total_seconds())),
            )
        )
        session_cache = FileSystemCache(
            str(sessions_directory),
            threshold=5000,
            default_timeout=lifetime_seconds,
            mode=0o700,
        )
        app_instance.config['SESSION_TYPE'] = 'cachelib'
        app_instance.config['SESSION_CACHELIB'] = session_cache
        app_instance.config['PERMANENT_SESSION_LIFETIME'] = lifetime_seconds
        cache_config = {'CACHE_TYPE': 'NullCache'}
        session_backend = 'cachelib'
        Session(app_instance)
        session_initialized = True
    else:
        redis_config = dict(take_config('REDIS', required=True))

        redis_host = (
            os.environ.get("TAIKO_WEB_REDIS_HOST")
            or os.environ.get("REDIS_HOST")
            or redis_config.get('CACHE_REDIS_HOST')
            or 'redis'
        )
        redis_port = int(
            os.environ.get("TAIKO_WEB_REDIS_PORT")
            or os.environ.get("REDIS_PORT", redis_config.get('CACHE_REDIS_PORT', 6379))
        )
        redis_password_env = (
            os.environ.get("TAIKO_WEB_REDIS_PASSWORD")
            if os.environ.get("TAIKO_WEB_REDIS_PASSWORD") is not None
            else os.environ.get("REDIS_PASSWORD")
        )
        redis_password = (
            redis_password_env
            if redis_password_env is not None
            else redis_config.get('CACHE_REDIS_PASSWORD')
        )
        redis_db = int(
            os.environ.get("TAIKO_WEB_REDIS_DB")
            or os.environ.get("REDIS_DB", redis_config.get('CACHE_REDIS_DB', 0))
        )

        redis_config['CACHE_REDIS_HOST'] = redis_host
        redis_config['CACHE_REDIS_PORT'] = redis_port
        redis_config['CACHE_REDIS_PASSWORD'] = redis_password
        redis_config['CACHE_REDIS_DB'] = redis_db

        session_redis = Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            decode_responses=False,
        )

        session_logger = logging.getLogger('taiko.session')
        try:
            session_redis.ping()
        except Exception as exc:
            session_logger.error('SESSION_REDIS ping=FAIL: %r', exc)
        else:
            pool = getattr(session_redis, 'connection_pool', None)
            connection_kwargs = getattr(pool, 'connection_kwargs', {}) if pool is not None else {}
            session_logger.info(
                'SESSION_REDIS ping=ok host=%s port=%s db=%s',
                connection_kwargs.get('host', redis_host),
                connection_kwargs.get('port', redis_port),
                connection_kwargs.get('db', redis_db),
            )

        app_instance.config.update(
            SESSION_TYPE='redis',
            SESSION_REDIS=session_redis,
            SESSION_KEY_PREFIX=os.getenv('SESSION_KEY_PREFIX', 'sess:'),
            PERMANENT_SESSION_LIFETIME=int(os.getenv('SESSION_TTL_SECONDS', '1209600')),
        )
        cache_config = redis_config
    app_instance.config['SESSION_BACKEND'] = session_backend
    db_name = os.environ.get("TAIKO_WEB_MONGO_DB") or mongo_config.get('database') or 'taiko'

    mongo_database_factory: Callable[[], object]
    redis_factory: Optional[Callable[[], object]]

    if RUN_PROFILE == 'desktop':
        _mongo_dispatcher = None
        _redis_dispatcher = None

        def _desktop_mongo_database():
            return None

        mongo_database_factory = _desktop_mongo_database
        redis_factory = None
    else:
        def _create_redis_client() -> Redis:
            return Redis(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                db=redis_db,
                decode_responses=False,
            )

        _mongo_dispatcher = MongoDispatcher(_create_mongo_client, db_name)
        _redis_dispatcher = RedisDispatcher(_create_redis_client)
        mongo_database_factory = _mongo_dispatcher.get_database
        redis_factory = _redis_dispatcher.get_client
        app_instance.config['REDIS_CLIENT'] = session_redis
        app_instance.config['REDIS_CLIENT_FACTORY'] = _create_redis_client

    global _storage_bundle, SONG_STORE, MANIFEST_STORE, LEADER_LOCK
    _storage_bundle = create_storage_bundle(
        run_profile=RUN_PROFILE,
        mongo_database_factory=mongo_database_factory,
        redis_client_factory=redis_factory,
        data_directory=desktop_data_dir,
    )
    LOGGER.info('storage bundle initialized')
    SONG_STORE = _storage_bundle.song_store
    MANIFEST_STORE = _storage_bundle.manifest_store
    LEADER_LOCK = _storage_bundle.leader_lock
    if RUN_PROFILE == 'desktop':
        sqlite_path = getattr(SONG_STORE, 'path', None)
        if sqlite_path:
            resolved_sqlite = Path(sqlite_path).resolve()
            app_instance.config['SQLITE_PATH'] = str(resolved_sqlite)
            app_instance.config['SQLITE_DB_PATH'] = str(resolved_sqlite)

    app_instance.cache = Cache(app_instance, config=cache_config)
    if not session_initialized:
        session_manager.init_app(app_instance)
    #csrf = CSRFProtect(app)

    db_name = os.environ.get("TAIKO_WEB_MONGO_DB") or mongo_config.get('database') or 'taiko'
    if RUN_PROFILE != 'desktop' and os.getenv('TAIKO_INIT_INDEXES') == '1':
        init_db_schema(_mongo_dispatcher.get_database())

    basedir = basedir_value
    if _mongo_dispatcher is not None:
        client = _LazyResourceProxy(_mongo_dispatcher.get_client)
        db = _LazyResourceProxy(_mongo_dispatcher.get_database)
    else:
        client = _LazyResourceProxy(lambda: None)
        db = _LazyResourceProxy(lambda: None)

    if RUN_PROFILE == 'desktop':
        songs_dir_value = str(songs_dir())
        config_songs_dir = None
        config_path = None
    else:
        config_songs_dir, config_path = resolve_songs_dir_from_config(logger=app_instance.logger)
        if config_path and not os.environ.get(DESKTOP_CONFIG_ENV):
            os.environ[DESKTOP_CONFIG_ENV] = str(Path(config_path).resolve())

        songs_dir_candidates = [
            os.environ.get('TAIKO_SONGS_DIR'),
            os.environ.get('SONGS_DIR'),
            str(config_songs_dir) if config_songs_dir else None,
            take_config('SONGS_DIR'),
        ]
        songs_dir_value = next((value for value in songs_dir_candidates if value), None)
        if songs_dir_value is None:
            songs_dir_value = str(Path.home() / 'Music' / 'TaikoSongs')
    SONGS_DIR_PATH = Path(songs_dir_value).expanduser().resolve()
    app_instance.logger.info(
        'profile=%s catalog_source=%s songs_dir=%s',
        RUN_PROFILE,
        CATALOG_SOURCE,
        SONGS_DIR_PATH,
    )
    app_instance.logger.info('Songs dir: %s', SONGS_DIR_PATH)
    if not SONGS_DIR_PATH.exists():
        app_instance.logger.warning('Songs directory %s does not exist; library will start empty', SONGS_DIR_PATH)

    def _normalise_scan_mode(value) -> str:
        if value is None:
            return 'auto'
        if isinstance(value, bool):
            return 'force' if value else 'skip'
        text = str(value).strip().lower()
        if not text:
            return 'auto'
        if text in {'auto', 'force', 'skip'}:
            return text
        if text in {'1', 'true', 'yes', 'on', 'full'}:
            return 'force'
        if text in {'0', 'false', 'no', 'off', 'none'}:
            return 'skip'
        return 'auto'

    SCAN_ON_START = _normalise_scan_mode(take_config('SCAN_ON_START'))
    ENABLE_SONG_WATCHER_DEFAULT = True

    def _coerce_bool(value, default):
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if not text:
            return default
        return text not in {'0', 'false', 'no', 'off'}

    ENABLE_SONG_WATCHER = _coerce_bool(
        os.environ.get('ENABLE_SONG_WATCHER'),
        _coerce_bool(take_config('ENABLE_SONG_WATCHER'), ENABLE_SONG_WATCHER_DEFAULT),
    )
    scan_env = os.environ.get('SCAN_ON_START')
    if scan_env is not None:
        SCAN_ON_START = _normalise_scan_mode(scan_env)
    SCAN_IGNORE_GLOBS = take_config('SCAN_IGNORE_GLOBS') or ['**/.DS_Store', '**/Thumbs.db']
    ADMIN_SCAN_TOKEN = os.environ.get('ADMIN_SCAN_TOKEN') or take_config('ADMIN_SCAN_TOKEN') or 'change-me'
    SONGS_BASEURL_VALUE = _resolve_baseurl(os.environ.get('SONGS_BASEURL') or take_config('SONGS_BASEURL'))
    COERCE_UNKNOWN_COURSE = os.environ.get('COERCE_UNKNOWN_COURSE') or take_config('COERCE_UNKNOWN_COURSE')

    def _create_song_scanner() -> SongScanner:
        database = _mongo_dispatcher.get_database() if _mongo_dispatcher is not None else None
        return SongScanner(
            db=database,
            songs_dir=SONGS_DIR_PATH,
            songs_baseurl=SONGS_BASEURL_VALUE,
            ignore_globs=SCAN_IGNORE_GLOBS,
            coerce_unknown_course=COERCE_UNKNOWN_COURSE,
            redis_client=app_instance.config.get('SESSION_REDIS'),
            song_store=SONG_STORE,
            manifest_store=MANIFEST_STORE,
            leader_lock=LEADER_LOCK,
        )

    _song_scanner_provider = SongScannerProvider(_create_song_scanner)
    song_scanner = _LazyResourceProxy(_song_scanner_provider.get)

    logging.getLogger('taiko.scanner.summary').info('scan:boot marker')
    logging.getLogger('taiko.scanner').info('scanner:boot marker')

    _song_watcher_handle = None

    try:
        routes_snapshot = [getattr(rule, 'rule', str(rule)) for rule in app_instance.url_map.iter_rules()]
        if RUN_PROFILE == 'desktop' and CATALOG_SOURCE == 'sqlite':
            app_instance.logger.info('Routes: %s, api_songs=enabled(sqlite)', routes_snapshot)
        else:
            app_instance.logger.info('Routes: %s', routes_snapshot)
        app_instance.logger.info('Routes count: %s', len(routes_snapshot))
    except Exception as exc:  # pragma: no cover - diagnostic helper
        app_instance.logger.warning('Failed to list routes: %s', exc)

    return app_instance


app = create_app()


def _maybe_log_startup_duration(*, fast_path: bool) -> None:
    global _startup_scan_started_at, _startup_scan_logged
    if _startup_scan_started_at is None or _startup_scan_logged:
        return
    duration = time.monotonic() - _startup_scan_started_at
    logger = app.logger if 'app' in globals() else LOGGER
    logger.info('Song scan startup_duration_seconds=%.3f fast_path=%s', duration, fast_path)
    _startup_scan_logged = True


@app.route('/healthz')
def healthz():
    if not is_desktop():
        # Ровно три ключа в контракте: status, mongo, profile.
        status_ok = {'status': 'ok', 'mongo': 'ok', 'profile': 'web'}
        status_fail_mongo = {'status': 'fail', 'mongo': 'fail', 'profile': 'web'}
        status_fail_redis = {'status': 'fail', 'mongo': 'ok', 'profile': 'web'}

        client = current_app.config.get('MONGO_CLIENT')
        if client is None:
            factory = current_app.config.get('MONGO_CLIENT_FACTORY')
            if callable(factory):
                try:
                    client = factory()
                    current_app.config['MONGO_CLIENT'] = client
                except Exception:
                    client = None

        try:
            if client is None:
                raise RuntimeError('mongo client unavailable')
            client.admin.command('ping')
        except Exception:
            current_app.logger.exception('mongo ping failed')
            return jsonify(status_fail_mongo), 503

        redis_client = current_app.config.get('SESSION_REDIS')
        if redis_client is not None:
            try:
                redis_client.ping()
            except Exception:
                current_app.logger.exception('redis ping failed')
                return jsonify(status_fail_redis), 503

        return jsonify(status_ok), 200

    payload = {'status': 'ok', 'profile': 'desktop'}
    sqlite_path = current_app.config.get('SQLITE_PATH') or current_app.config.get('SQLITE_DB_PATH')
    if sqlite_path:
        payload['db_path'] = str(Path(sqlite_path).resolve())
    return jsonify(payload), 200


@app.route('/favicon.ico', methods=['GET', 'HEAD'])
def favicon_asset():
    favicon_path = public_dir() / 'assets' / 'img' / 'favicon.png'
    if favicon_path.is_file():
        return send_from_directory(
            str(favicon_path.parent),
            favicon_path.name,
            mimetype='image/png',
        )

    response = Response(status=200)
    response.data = b''
    response.headers['Content-Type'] = 'image/x-icon'
    return response


@app.route('/admin/shutdown', methods=['POST'])
def route_admin_shutdown():
    app.logger.info('Shutdown requested via /admin/shutdown')

    def _trigger_shutdown() -> None:
        time.sleep(0.1)
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception:
            app.logger.exception('Failed to signal shutdown; forcing exit')
            os._exit(0)

    threading.Thread(target=_trigger_shutdown, daemon=True).start()
    return jsonify({'status': 'shutting_down'}), 202


class HashException(Exception):
    pass


def api_error(message):
    return jsonify({'status': 'error', 'message': message})


def generate_hash(id, form):
    md5 = hashlib.md5()
    if form['type'] == 'tja':
        urls = ['%s%s/main.tja' % (take_config('SONGS_BASEURL', required=True), id)]
    else:
        urls = []
        for diff in ['easy', 'normal', 'hard', 'oni', 'ura']:
            if form['course_' + diff]:
                urls.append('%s%s/%s.osu' % (take_config('SONGS_BASEURL', required=True), id, diff))

    for url in urls:
        if url.startswith("http://") or url.startswith("https://"):
            resp = requests.get(url)
            if resp.status_code != 200:
                raise HashException('Invalid response from %s (status code %s)' % (resp.url, resp.status_code))
            md5.update(resp.content)
        else:
            if url.startswith(basedir):
                url = url[len(basedir):]
            path = os.path.normpath(os.path.join("public", url))
            if not os.path.isfile(path):
                raise HashException("File not found: %s" % (os.path.abspath(path)))
            with open(path, "rb") as file:
                md5.update(file.read())

    return base64.b64encode(md5.digest())[:-2].decode('utf-8')


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('username'):
            return api_error('not_logged_in')
        return f(*args, **kwargs)
    return decorated_function


def admin_required(level):
    def decorated_function(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            unavailable = _desktop_mongo_unavailable_response(api=False)
            if unavailable is not None:
                return unavailable
            if not session.get('username'):
                return abort(403)

            user = db.users.find_one({'username': session.get('username')})
            if not isinstance(user, Mapping) or user.get('user_level', 0) < level:
                return abort(403)

            return f(*args, **kwargs)
        return wrapper
    return decorated_function


@app.errorhandler(CSRFError)
def handle_csrf_error(e):
    return api_error('invalid_csrf')


@app.before_request
def before_request_func():
    if RUN_PROFILE == 'desktop':
        return None
    if session.get('session_id'):
        if not db.users.find_one({'session_id': session.get('session_id')}):
            session.clear()


def get_config(credentials=False):
    config_out = {
        'basedir': basedir,
        'songs_baseurl': SONGS_BASEURL_VALUE,
        'assets_baseurl': take_config('ASSETS_BASEURL', required=True),
        'email': take_config('EMAIL'),
        'accounts': take_config('ACCOUNTS'),
        'custom_js': take_config('CUSTOM_JS'),
        'plugins': take_config('PLUGINS') and [x for x in take_config('PLUGINS') if x['url']],
        'preview_type': take_config('PREVIEW_TYPE') or 'mp3',
        'multiplayer_url': take_config('MULTIPLAYER_URL'),
    }
    catalog_assume_valid_flag = bool(CATALOG_ASSUME_VALID)
    config_out['catalog_assume_valid'] = catalog_assume_valid_flag
    config_out['catalogAssumeValid'] = catalog_assume_valid_flag
    relative_urls = ['songs_baseurl', 'assets_baseurl']
    for name in relative_urls:
        if not config_out[name].startswith("/") and not config_out[name].startswith("http://") and not config_out[name].startswith("https://"):
            config_out[name] = basedir + config_out[name]
    if credentials:
        google_credentials = take_config('GOOGLE_CREDENTIALS')
        min_level = google_credentials['min_level'] or 0
        if not session.get('username'):
            user_level = 0
        else:
            user = db.users.find_one({'username': session.get('username')})
            user_level = user['user_level']
        if user_level >= min_level:
            config_out['google_credentials'] = google_credentials
        else:
            config_out['google_credentials'] = {
                'gdrive_enabled': False
            }

    if not config_out.get('songs_baseurl'):
        config_out['songs_baseurl'] = ''.join([request.host_url, 'songs']) + '/'
    if not config_out.get('assets_baseurl'):
        config_out['assets_baseurl'] = ''.join([request.host_url, 'assets']) + '/'

    config_out['_version'] = get_version()
    return config_out

def get_version():
    version = {'commit': None, 'commit_short': '', 'version': None, 'url': take_config('URL')}
    if os.path.isfile('version.json'):
        try:
            ver = json.load(open('version.json', 'r'))
        except ValueError:
            print('Invalid version.json file')
            return version

        for key in version.keys():
            if ver.get(key):
                version[key] = ver.get(key)

    return version

def get_db_don(user):
    don_body_fill = user['don_body_fill'] if 'don_body_fill' in user else get_default_don('body_fill')
    don_face_fill = user['don_face_fill'] if 'don_face_fill' in user else get_default_don('face_fill')
    return {'body_fill': don_body_fill, 'face_fill': don_face_fill}

def get_default_don(part=None):
    if part == None:
        return {
            'body_fill': get_default_don('body_fill'),
            'face_fill': get_default_don('face_fill')
        }
    elif part == 'body_fill':
        return '#5fb7c1'
    elif part == 'face_fill':
        return '#ff5724'

def is_hex(input):
    try:
        int(input, 16)
        return True
    except ValueError:
        return False


def _render_legacy_index() -> Response:
    version = get_version()
    now = datetime.now()
    return render_template(
        'index.html',
        version=version,
        config=get_config(),
        year=now.year,
        month=now.month,
        day=now.day,
    )


def _serve_frontend_asset(spa_path: str) -> Response:
    global _FRONTEND_WARNING_EMITTED
    root = FRONTEND_DIR
    try:
        root_resolved = root.resolve()
    except Exception:
        root_resolved = root

    if root_resolved.exists():
        relative_token = Path(spa_path).as_posix().lstrip('/')
        if relative_token:
            candidate = root_resolved.joinpath(relative_token).resolve()
            try:
                candidate.relative_to(root_resolved)
            except ValueError:
                abort(404)
            if candidate.is_file():
                return send_from_directory(str(root_resolved), relative_token)
        index_path = root_resolved / 'index.html'
        if index_path.is_file():
            return send_from_directory(str(root_resolved), 'index.html')

    if not _FRONTEND_WARNING_EMITTED:
        LOGGER.warning(
            'Frontend directory %s missing or incomplete; falling back to legacy template',
            root,
        )
        _FRONTEND_WARNING_EMITTED = True
    return _render_legacy_index()


@app.route(basedir)
def route_index():
    return _serve_frontend_asset('')


@app.route(basedir + 'api/csrftoken')
def route_csrftoken():
    return jsonify({'status': 'ok', 'token': generate_csrf()})


@app.route(basedir + 'admin')
@admin_required(level=50)
def route_admin():
    return redirect(basedir + 'admin/songs')


@app.route(basedir + 'admin/songs')
@admin_required(level=50)
def route_admin_songs():
    song_store = _require_song_store()
    songs = sorted(list(song_store.find({})), key=lambda x: x['id'])
    categories = db.categories.find({})
    user = db.users.find_one({'username': session['username']})
    return render_template('admin_songs.html', songs=songs, admin=user, categories=list(categories), config=get_config())


@app.route(basedir + 'admin/songs/<int:id>')
@admin_required(level=50)
def route_admin_songs_id(id):
    song = _require_song_store().find_one({'id': id})
    if not song:
        return abort(404)

    categories = list(db.categories.find({}))
    song_skins = list(db.song_skins.find({}))
    makers = list(db.makers.find({}))
    user = db.users.find_one({'username': session['username']})

    return render_template('admin_song_detail.html',
        song=song, categories=categories, song_skins=song_skins, makers=makers, admin=user, config=get_config())


def _current_song_id_ceiling(*, include_counter: bool = True) -> int:
    current = 0
    if include_counter:
        counter = getattr(db, 'counters', None)
        if counter is not None:
            try:
                counter_doc = counter.find_one({'_id': 'songs'})
            except Exception:
                counter_doc = None
            if counter_doc and isinstance(counter_doc.get('seq'), int):
                current = max(current, counter_doc['seq'])
    seq = getattr(db, 'seq', None)
    if seq is not None:
        try:
            seq_doc = seq.find_one({'name': 'songs'})
        except Exception:
            seq_doc = None
        if seq_doc and isinstance(seq_doc.get('value'), int):
            current = max(current, seq_doc['value'])
    try:
        highest_song = _require_song_store().find_one(sort=[('id', -1)])
    except Exception:
        highest_song = None
    if highest_song and isinstance(highest_song.get('id'), int):
        current = max(current, highest_song['id'])
    return current


def _peek_next_song_id():
    return _current_song_id_ceiling() + 1


def _get_next_song_id():
    counter = getattr(db, 'counters', None)
    if counter is not None:
        floor = _current_song_id_ceiling()
        try:
            counter.update_one(
                {'_id': 'songs'},
                {
                    '$setOnInsert': {'seq': floor},
                    '$max': {'seq': floor},
                },
                upsert=True,
            )
        except Exception as exc:
            LOGGER.warning(
                'Failed to ensure songs counter floor at %d: %s',
                floor,
                exc,
                exc_info=True,
            )
        last_failure = None
        for attempt in range(3):
            try:
                doc = counter.find_one_and_update(
                    {'_id': 'songs'},
                    {'$inc': {'seq': 1}},
                    upsert=True,
                    return_document=_mongo_return_document_after(),
                )
            except Exception as exc:
                last_failure = exc
                delay = 0.05 * (attempt + 1)
                LOGGER.warning(
                    'Failed to increment songs counter (attempt %d/3); retrying in %.2fs: %s',
                    attempt + 1,
                    delay,
                    exc,
                    exc_info=True,
                )
                time.sleep(delay)
                continue
            if isinstance(doc, dict) and isinstance(doc.get('seq'), int):
                seq_value = doc['seq']
                ceiling = _current_song_id_ceiling(include_counter=False)
                if seq_value <= ceiling:
                    last_failure = RuntimeError(
                        f'songs counter returned stale value {seq_value} <= ceiling {ceiling}'
                    )
                    LOGGER.warning(
                        'Songs counter returned %d which is not above the ceiling %d; repairing and retrying',
                        seq_value,
                        ceiling,
                    )
                    try:
                        counter.update_one(
                            {'_id': 'songs'},
                            {
                                '$setOnInsert': {'seq': ceiling},
                                '$max': {'seq': ceiling},
                            },
                            upsert=True,
                        )
                    except Exception as clamp_exc:
                        last_failure = clamp_exc
                        LOGGER.warning(
                            'Failed to clamp songs counter to %d: %s',
                            ceiling,
                            clamp_exc,
                            exc_info=True,
                        )
                        break
                    floor = max(floor, ceiling)
                    continue
                return seq_value
        if last_failure is not None:
            LOGGER.warning('Falling back to legacy song id allocation after counter failures')
    seq = getattr(db, 'seq', None)
    if seq is not None:
        seq_doc = seq.find_one({'name': 'songs'})
        seq_value = seq_doc['value'] if seq_doc else 0
        highest_song = _require_song_store().find_one(sort=[('id', -1)])
        if highest_song and highest_song['id'] > seq_value:
            seq_value = highest_song['id']
        next_value = seq_value + 1
        seq.update_one({'name': 'songs'}, {'$set': {'value': next_value}}, upsert=True)
        return next_value
    highest_song = _require_song_store().find_one(sort=[('id', -1)])
    if highest_song and highest_song['id']:
        return highest_song['id'] + 1
    return 1


@app.route(basedir + 'admin/songs/new')
@admin_required(level=100)
def route_admin_songs_new():
    categories = list(db.categories.find({}))
    song_skins = list(db.song_skins.find({}))
    makers = list(db.makers.find({}))
    seq_new = _peek_next_song_id()

    return render_template('admin_song_new.html', categories=categories, song_skins=song_skins, makers=makers, config=get_config(), id=seq_new)


@app.route(basedir + 'admin/songs/new', methods=['POST'])
@admin_required(level=100)
def route_admin_songs_new_post():
    output = {'title_lang': {}, 'subtitle_lang': {}, 'courses': {}}
    output['enabled'] = True if request.form.get('enabled') else False
    output['title'] = request.form.get('title') or None
    output['subtitle'] = request.form.get('subtitle') or None
    for lang in ['ja', 'en', 'cn', 'tw', 'ko']:
        output['title_lang'][lang] = request.form.get('title_%s' % lang) or None
        output['subtitle_lang'][lang] = request.form.get('subtitle_%s' % lang) or None

    for course in ['easy', 'normal', 'hard', 'oni', 'ura']:
        if request.form.get('course_%s' % course):
            output['courses'][course] = {'stars': int(request.form.get('course_%s' % course)),
                                         'branch': True if request.form.get('branch_%s' % course) else False}
        else:
            output['courses'][course] = None
    
    output['category_id'] = int(request.form.get('category_id')) or None
    output['type'] = request.form.get('type')
    output['music_type'] = request.form.get('music_type')
    output['offset'] = float(request.form.get('offset')) or None
    output['skin_id'] = int(request.form.get('skin_id')) or None
    output['preview'] = float(request.form.get('preview')) or None
    output['volume'] = float(request.form.get('volume')) or None
    output['maker_id'] = int(request.form.get('maker_id')) or None
    output['lyrics'] = True if request.form.get('lyrics') else False
    output['hash'] = request.form.get('hash')
    
    seq_new = _get_next_song_id()
    
    hash_error = False
    if request.form.get('gen_hash'):
        try:
            output['hash'] = generate_hash(seq_new, request.form)
        except HashException as e:
            hash_error = True
            flash('An error occurred: %s' % str(e), 'error')
    
    output['id'] = seq_new
    output['order'] = seq_new
    
    _require_song_store().insert_one(output)
    if not hash_error:
        flash('Song created.')
    
    return redirect(basedir + 'admin/songs/%s' % str(seq_new))


@app.route(basedir + 'admin/songs/<int:id>', methods=['POST'])
@admin_required(level=50)
def route_admin_songs_id_post(id):
    song = _require_song_store().find_one({'id': id})
    if not song:
        return abort(404)

    user = db.users.find_one({'username': session['username']})
    user_level = user['user_level']

    output = {'title_lang': {}, 'subtitle_lang': {}, 'courses': {}}
    if user_level >= 100:
        output['enabled'] = True if request.form.get('enabled') else False

    output['title'] = request.form.get('title') or None
    output['subtitle'] = request.form.get('subtitle') or None
    for lang in ['ja', 'en', 'cn', 'tw', 'ko']:
        output['title_lang'][lang] = request.form.get('title_%s' % lang) or None
        output['subtitle_lang'][lang] = request.form.get('subtitle_%s' % lang) or None

    for course in ['easy', 'normal', 'hard', 'oni', 'ura']:
        if request.form.get('course_%s' % course):
            output['courses'][course] = {'stars': int(request.form.get('course_%s' % course)),
                                         'branch': True if request.form.get('branch_%s' % course) else False}
        else:
            output['courses'][course] = None
    
    output['category_id'] = int(request.form.get('category_id')) or None
    output['type'] = request.form.get('type')
    output['music_type'] = request.form.get('music_type')
    output['offset'] = float(request.form.get('offset')) or None
    output['skin_id'] = int(request.form.get('skin_id')) or None
    output['preview'] = float(request.form.get('preview')) or None
    output['volume'] = float(request.form.get('volume')) or None
    output['maker_id'] = int(request.form.get('maker_id')) or None
    output['lyrics'] = True if request.form.get('lyrics') else False
    output['hash'] = request.form.get('hash')
    
    hash_error = False
    if request.form.get('gen_hash'):
        try:
            output['hash'] = generate_hash(id, request.form)
        except HashException as e:
            hash_error = True
            flash('An error occurred: %s' % str(e), 'error')
    
    _require_song_store().update_one({'id': id}, {'$set': output})
    if not hash_error:
        flash('Changes saved.')
    
    return redirect(basedir + 'admin/songs/%s' % id)


@app.route(basedir + 'admin/songs/<int:id>/delete', methods=['POST'])
@admin_required(level=100)
def route_admin_songs_id_delete(id):
    song = _require_song_store().find_one({'id': id})
    if not song:
        return abort(404)

    _require_song_store().delete_one({'id': id})
    flash('Song deleted.')
    return redirect(basedir + 'admin/songs')


@app.route(basedir + 'admin/users')
@admin_required(level=50)
def route_admin_users():
    user = db.users.find_one({'username': session.get('username')})
    max_level = user['user_level'] - 1
    return render_template('admin_users.html', config=get_config(), max_level=max_level, username='', level='')


@app.route(basedir + 'admin/users', methods=['POST'])
@admin_required(level=50)
def route_admin_users_post():
    admin_name = session.get('username')
    admin = db.users.find_one({'username': admin_name})
    max_level = admin['user_level'] - 1
    
    username = request.form.get('username')
    try:
        level = int(request.form.get('level')) or 0
    except ValueError:
        level = 0
    
    user = db.users.find_one({'username_lower': username.lower()})
    if not user:
        flash('Error: User was not found.')
    elif admin['username'] == user['username']:
        flash('Error: You cannot modify your own level.')
    else:
        user_level = user['user_level']
        if level < 0 or level > max_level:
            flash('Error: Invalid level.')
        elif user_level > max_level:
            flash('Error: This user has higher level than you.')
        else:
            output = {'user_level': level}
            db.users.update_one({'username': user['username']}, {'$set': output})
            flash('User updated.')
    
    return render_template('admin_users.html', config=get_config(), max_level=max_level, username=username, level=level)


@app.route(basedir + 'api/preview')
@app.cache.cached(timeout=15, query_string=True)
def route_api_preview():
    song_id = request.args.get('id', None)
    if not song_id or not re.match('^[0-9]{1,9}$', song_id):
        abort(400)

    song_id = int(song_id)
    song = _require_song_store().find_one({'id': song_id})
    if not song:
        abort(400)

    song_type = song['type']
    song_ext = song['music_type'] if song['music_type'] else "mp3"
    prev_path = make_preview(song_id, song_type, song_ext, song['preview'])
    if not prev_path:
        return redirect(get_config()['songs_baseurl'] + '%s/main.%s' % (song_id, song_ext))

    return redirect(get_config()['songs_baseurl'] + '%s/preview.mp3' % song_id)


def _serialize_catalog_entry(
    entry: Mapping[str, Any], *, manifest_entry: Optional[Mapping[str, Any]] = None
) -> Optional[dict[str, Any]]:
    sources: list[Mapping[str, Any]] = []
    if isinstance(entry, Mapping):
        sources.append(entry)
    if isinstance(manifest_entry, Mapping):
        sources.append(manifest_entry)

    def _first(key: str, default: object = None) -> object:
        for source in sources:
            if key in source:
                value = source.get(key)
                if value is not None:
                    return value
        return default

    def _normalize_identifier(value: object) -> Optional[str]:
        if isinstance(value, str):
            token = value.strip()
            return token or None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                return str(int(value))
            except (TypeError, ValueError):
                return None
        return None

    primary_id = _normalize_identifier(_first('song_id'))
    if not primary_id:
        primary_id = _normalize_identifier(_first('scanner_stable_id'))
    if not primary_id:
        app.logger.error('Failed to serialize song entry without identifier sources=%s', sources)
        raise RuntimeError('song entry missing identifier')

    title_value = _first('title', primary_id)
    subtitle_value = _first('subtitle', '')
    category_value = _first('category', '')
    category_id_value = _first('category_id', 0)
    duration_value = _first('duration_ms', 0)
    preview_available = bool(_first('preview_available', False))
    source_type_value = _first('source_type', 'tja') or 'tja'
    is_playable_value = _first('is_playable', False)
    paths_value = _first('paths', {})

    combined_entry: dict[str, Any] = {}
    for source in sources[::-1]:
        combined_entry.update(source)

    try:
        duration_ms = int(duration_value) if duration_value is not None else 0
    except (TypeError, ValueError):
        duration_ms = 0

    playable_flag = bool(is_playable_value)
    if not playable_flag and CATALOG_ASSUME_VALID:
        playable_flag = True

    if isinstance(paths_value, Mapping):
        filtered_paths = {
            key: value
            for key in ('tja_url', 'audio_url', 'dir_url')
            if (value := paths_value.get(key))
        }
    else:
        filtered_paths = {}

    item: dict[str, Any] = {
        'id': primary_id,
        'title': title_value if isinstance(title_value, str) else str(title_value),
        'subtitle': subtitle_value if isinstance(subtitle_value, str) else '',
        'category': category_value if isinstance(category_value, str) else '',
        'category_id': _coerce_int(category_id_value, 0),
        'duration_ms': duration_ms,
        'preview_available': preview_available,
        'source_type': source_type_value if isinstance(source_type_value, str) and source_type_value else 'tja',
        'paths': filtered_paths,
        'is_playable': bool(playable_flag),
        'difficulties': _normalize_difficulties(combined_entry, assume_valid=CATALOG_ASSUME_VALID),
    }
    return item


def _load_mongo_catalog_entries(
    *,
    limit: Optional[int],
    skip: int,
    category_value: str,
    search_value: str,
) -> list[dict[str, Any]]:
    songs_collection = getattr(db, 'songs', None)
    if songs_collection is None:
        return []

    filters: dict[str, Any] = {'is_hidden': {'$ne': True}, 'is_playable': True}
    if category_value:
        filters['category'] = category_value
    if search_value:
        filters['title_lc'] = {'$regex': f'^{re.escape(search_value)}'}

    projection = {
        '_id': 0,
        'id': 1,
        'scanner_stable_id': 1,
        'title': 1,
        'subtitle': 1,
        'category': 1,
        'category_id': 1,
        'duration_ms': 1,
        'preview_available': 1,
        'source_type': 1,
        'paths': 1,
        'is_playable': 1,
        'difficulties': 1,
    }

    try:
        cursor = songs_collection.find(filters, projection).sort([
            ('title', 1),
            ('scanner_stable_id', 1),
            ('id', 1),
        ])
        if skip:
            cursor = cursor.skip(skip)
        if isinstance(limit, int):
            cursor = cursor.limit(limit)
        raw_payload = [dict(doc) for doc in cursor if isinstance(doc, Mapping)]
    except Exception:
        app.logger.exception('Failed to query songs catalog')
        return []

    stable_ids = [
        entry.get('scanner_stable_id')
        for entry in raw_payload
        if isinstance(entry.get('scanner_stable_id'), str)
    ]
    manifest_map = _load_manifest_entries_for_ids(stable_ids)

    payload: list[dict[str, Any]] = []
    for entry in raw_payload:
        stable_id = entry.get('scanner_stable_id')
        manifest_entry = manifest_map.get(stable_id) if isinstance(stable_id, str) else None
        try:
            item = _serialize_catalog_entry(entry, manifest_entry=manifest_entry)
        except RuntimeError:
            app.logger.exception(
                'Failed to serialize mongo song entry id=%s',
                entry.get('song_id') or entry.get('scanner_stable_id') or '<unknown>',
            )
            raise
        if item is not None:
            payload.append(item)
    return payload


def _load_filesystem_catalog_entries(
    *,
    limit: Optional[int],
    skip: int,
    category_value: str,
    search_value: str,
) -> list[dict[str, Any]]:
    store = _get_song_store()
    if store is None:
        return []

    projection = {
        '_id': 0,
        'id': 1,
        'song_id': 1,
        'scanner_stable_id': 1,
        'title': 1,
        'subtitle': 1,
        'category': 1,
        'category_id': 1,
        'duration_ms': 1,
        'preview_available': 1,
        'source_type': 1,
        'paths': 1,
        'is_playable': 1,
        'is_hidden': 1,
        'disabled': 1,
        'difficulties': 1,
    }

    try:
        cursor = store.find({}, projection=projection)
    except Exception:
        app.logger.exception('Failed to load sqlite songs catalog')
        return []

    docs: list[dict[str, Any]] = []
    stable_ids: list[str] = []
    for raw_doc in cursor:
        if not isinstance(raw_doc, Mapping):
            continue
        doc = dict(raw_doc)
        stable_id = (
            doc.get('scanner_stable_id')
            or doc.get('id')
            or doc.get('song_id')
        )
        if not isinstance(stable_id, str) or not stable_id:
            continue
        doc['scanner_stable_id'] = stable_id
        docs.append(doc)
        stable_ids.append(stable_id)

    manifest_map = _load_manifest_entries_for_ids(stable_ids)

    filtered: list[tuple[dict[str, Any], Mapping[str, Any]]] = []
    for doc in docs:
        manifest_entry_raw = manifest_map.get(doc['scanner_stable_id'])
        manifest_entry = manifest_entry_raw if isinstance(manifest_entry_raw, Mapping) else {}
        if doc.get('is_hidden') is True:
            continue
        if doc.get('disabled') is True:
            continue
        if manifest_entry.get('is_hidden') is True:
            continue
        if manifest_entry.get('disabled') is True:
            continue
        playable_flag = bool(doc.get('is_playable')) or bool(manifest_entry.get('is_playable'))
        if not playable_flag and not CATALOG_ASSUME_VALID:
            continue
        category_candidate = doc.get('category')
        if not isinstance(category_candidate, str) or not category_candidate.strip():
            category_candidate = manifest_entry.get('category') if isinstance(manifest_entry.get('category'), str) else ''
        if category_value and category_candidate != category_value:
            continue
        if search_value:
            title_candidate = doc.get('title')
            if not isinstance(title_candidate, str) or not title_candidate.strip():
                manifest_title = manifest_entry.get('title')
                title_candidate = manifest_title if isinstance(manifest_title, str) else ''
            if not title_candidate.strip().lower().startswith(search_value):
                continue
        filtered.append((doc, manifest_entry))

    filtered.sort(
        key=lambda pair: (
            str((pair[0].get('title') or pair[1].get('title') or '')).casefold(),
            str(pair[0].get('scanner_stable_id') or pair[1].get('id') or ''),
        )
    )

    start_index = max(skip, 0)
    if isinstance(limit, int):
        slice_pairs = filtered[start_index:start_index + limit]
    else:
        slice_pairs = filtered[start_index:]

    payload: list[dict[str, Any]] = []
    for doc, manifest_entry in slice_pairs:
        try:
            item = _serialize_catalog_entry(doc, manifest_entry=manifest_entry)
        except RuntimeError:
            app.logger.exception(
                'Failed to serialize sqlite song entry id=%s',
                doc.get('song_id') or doc.get('scanner_stable_id') or '<unknown>',
            )
            raise
        if item is not None:
            payload.append(item)
    return payload


@app.route(basedir + 'api/songs')
def route_api_songs():
    app.logger.debug('api_songs: catalog_source=%s', CATALOG_SOURCE)
    cache_control = 'public, max-age=86400, stale-while-revalidate=600'
    vary_header = 'If-None-Match, Accept-Encoding'

    meta = _load_manifest_meta()
    etag: Optional[str] = None
    if isinstance(meta, dict):
        for key in ('manifestChecksum', 'manifest_checksum', 'checksum'):
            candidate = meta.get(key)
            if isinstance(candidate, str) and candidate.strip():
                etag = candidate.strip()
                break

    quoted_etag = f'"{etag}"' if etag else None

    request_etag = _normalize_if_none_match(request.headers.get('If-None-Match'))
    if etag and request_etag == etag:
        response = make_response('', 304)
        _apply_catalog_cache_headers(response, etag=quoted_etag, cache_control=cache_control, vary=vary_header)
        return response

    limit_param = request.args.get('limit', type=int)
    page_param = request.args.get('page', type=int)
    use_pagination = limit_param is not None or page_param is not None

    if use_pagination:
        limit_value = limit_param if isinstance(limit_param, int) else 200
        limit_value = max(1, min(limit_value, 200))
        page_value = page_param if isinstance(page_param, int) else 1
        page_value = max(page_value, 1)
        skip_value = (page_value - 1) * limit_value
    else:
        limit_value = None
        skip_value = 0

    category_param = request.args.get('category', '')
    category_value = category_param.strip() if isinstance(category_param, str) else ''

    search_param = request.args.get('q', '')
    if isinstance(search_param, str):
        search_value = search_param.strip().lower()
    else:
        search_value = ''

    use_sqlite_catalog = RUN_PROFILE == 'desktop' or CATALOG_SOURCE in {'filesystem', 'sqlite'}
    if use_sqlite_catalog:
        try:
            payload = _load_filesystem_catalog_entries(
                limit=limit_value,
                skip=skip_value,
                category_value=category_value,
                search_value=search_value,
            )
        except RuntimeError:
            app.logger.exception('sqlite catalog serialization error')
            abort(500)
        except Exception as exc:
            app.logger.warning('sqlite catalog error: %s', exc, exc_info=app.logger.isEnabledFor(logging.DEBUG))
            payload = []
    elif CATALOG_SOURCE == 'mongo':
        unavailable = _desktop_mongo_unavailable_response(api=True)
        if unavailable is not None:
            return unavailable
        try:
            payload = _load_mongo_catalog_entries(
                limit=limit_value,
                skip=skip_value,
                category_value=category_value,
                search_value=search_value,
            )
        except RuntimeError:
            app.logger.exception('mongo catalog serialization error')
            abort(500)
    else:
        app.logger.warning('Unknown catalog_source=%s for /api/songs', CATALOG_SOURCE)
        payload = []

    normalized_payload: list[Any]
    if payload is None:
        normalized_payload = []
    elif isinstance(payload, dict):
        normalized_payload = []
        for key in ('items', 'songs', 'data'):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                normalized_payload = candidate
                break
            if isinstance(candidate, (tuple, set)):
                normalized_payload = list(candidate)
                break
        else:
            app.logger.warning('Unexpected /api/songs dict payload without items/songs/data; normalizing to []')
    elif isinstance(payload, list):
        normalized_payload = payload
    elif isinstance(payload, (tuple, set)):
        normalized_payload = list(payload)
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        normalized_payload = list(payload)
    elif isinstance(payload, Iterable) and not isinstance(payload, (str, bytes, bytearray)):
        normalized_payload = list(payload)
    else:
        app.logger.warning('Unexpected /api/songs payload type %s; normalizing to []', type(payload).__name__)
        normalized_payload = []

    response = JSONResponse(content=normalized_payload, media_type='application/json')
    _apply_catalog_cache_headers(response, etag=quoted_etag, cache_control=cache_control, vary=vary_header)
    return response


@app.route(basedir + 'api/song/<song_id>')
def route_api_song_detail(song_id: str):
    if not isinstance(song_id, str):
        abort(400)
    stable_id = song_id.strip()
    if not stable_id:
        abort(400)

    meta_only = False
    meta_param = request.args.get('meta')
    if isinstance(meta_param, str) and meta_param.strip():
        token = meta_param.strip().lower()
        meta_only = token not in {'0', 'false', 'no', 'off'}
    notes_param = request.args.get('notes')
    if isinstance(notes_param, str) and notes_param.strip():
        token = notes_param.strip().lower()
        if token in {'none', '0', 'false', 'no'}:
            meta_only = True
        elif token in {'full', 'all', 'yes', 'true', '1'}:
            meta_only = False

    projection = dict(_SONG_DETAIL_PROJECTION)
    if meta_only:
        projection['charts.chart_data'] = False

    try:
        song_doc = _load_song_document_for_identifier(stable_id, projection=projection)
    except Exception:
        app.logger.exception('Failed to load song detail for %s', stable_id)
        abort(500)

    if not isinstance(song_doc, dict):
        return jsonify({'error': 'chart_not_found'}), 404

    valid_count: Optional[int] = None
    for key in ('valid_chart_count', 'valid_charts'):
        candidate = song_doc.get(key)
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            valid_count = int(candidate)
            break
        if isinstance(candidate, str):
            try:
                valid_count = int(candidate)
                break
            except ValueError:
                continue

    charts_field = song_doc.get('charts') if isinstance(song_doc.get('charts'), list) else []
    has_chart_entry = any(isinstance(entry, dict) for entry in charts_field)
    if valid_count is not None:
        if valid_count <= 0:
            return jsonify({'error': 'chart_not_found'}), 404
    elif not has_chart_entry:
        return jsonify({'error': 'chart_not_found'}), 404

    manifest_map = _load_manifest_entries_for_ids([stable_id])
    manifest_entry = manifest_map.get(stable_id)

    include_notes = not meta_only
    try:
        payload = _serialize_song_detail(song_doc, include_notes=include_notes, manifest_entry=manifest_entry)
    except RuntimeError:
        app.logger.exception('Failed to serialize song detail for %s', stable_id)
        abort(500)

    etag_source = None
    if isinstance(payload.get('sha1'), str) and payload['sha1']:
        etag_source = payload['sha1']
    elif isinstance(song_doc.get('hash'), str) and song_doc['hash']:
        etag_source = song_doc['hash']
    elif isinstance(song_doc.get('fingerprint'), str) and song_doc['fingerprint']:
        etag_source = song_doc['fingerprint']
    else:
        etag_source = hashlib.sha1(stable_id.encode('utf-8')).hexdigest()

    cache_control = 'public, max-age=86400, stale-while-revalidate=600'
    vary_header = 'If-None-Match, Accept-Encoding'

    request_etag = _normalize_if_none_match(request.headers.get('If-None-Match'))
    quoted_etag = f'"{etag_source}"' if etag_source else None
    if etag_source and request_etag == etag_source:
        response = make_response('', 304)
        response.headers['Cache-Control'] = cache_control
        response.headers['Vary'] = vary_header
        if quoted_etag:
            response.headers['ETag'] = quoted_etag
        return response

    response = JSONResponse(content=payload, media_type='application/json')
    if quoted_etag:
        response.headers['ETag'] = quoted_etag
    response.headers['Cache-Control'] = cache_control
    response.headers['Vary'] = vary_header
    return response


@app.route(basedir + 'api/songs/details')
def route_api_song_details() -> 'flask.Response':
    raw_ids = request.args.get('ids')
    if not isinstance(raw_ids, str) or not raw_ids.strip():
        abort(400)
    tokens = [token.strip() for token in raw_ids.split(',')]
    ordered_ids: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if not token:
            continue
        if token in seen:
            continue
        ordered_ids.append(token)
        seen.add(token)
        if len(ordered_ids) >= 50:
            break
    if not ordered_ids:
        return jsonify([])

    include_notes = True
    meta_param = request.args.get('meta')
    if isinstance(meta_param, str) and meta_param.strip():
        include_notes = meta_param.strip().lower() in {'0', 'false', 'no', 'off'}
    notes_param = request.args.get('notes')
    if isinstance(notes_param, str) and notes_param.strip():
        token = notes_param.strip().lower()
        if token in {'none', '0', 'false', 'no'}:
            include_notes = False
        elif token in {'full', 'all', 'yes', 'true', '1'}:
            include_notes = True

    projection = dict(_SONG_DETAIL_PROJECTION)

    try:
        cursor = _require_song_store().find({'scanner_stable_id': {'$in': ordered_ids}}, projection)
        docs = [dict(raw_doc) for raw_doc in cursor if isinstance(raw_doc, dict)]
    except Exception as exc:
        app.logger.exception('Failed to load batch song details')
        reason = getattr(exc, 'details', None)
        if not isinstance(reason, str) or not reason:
            reason = str(exc) or 'database query failed'
        payload = {'error': 'songs_details_failed', 'reason': reason}
        response = make_response(jsonify(payload), 400)
        return response

    if not include_notes:
        for doc in docs:
            charts_payload = doc.get('charts')
            if not isinstance(charts_payload, list):
                continue
            for chart_doc in charts_payload:
                if isinstance(chart_doc, dict):
                    chart_doc.pop('chart_data', None)

    found_docs: dict[str, dict] = {}
    manifest_lookup_ids: list[str] = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        primary_identifier = _normalize_document_identifier(doc.get('song_id'))
        if primary_identifier:
            found_docs[primary_identifier] = doc
        stable_identifier = _normalize_document_identifier(doc.get('scanner_stable_id') or doc.get('id'))
        if stable_identifier:
            if stable_identifier not in found_docs:
                found_docs[stable_identifier] = doc
            manifest_lookup_ids.append(stable_identifier)

    manifest_map = _load_manifest_entries_for_ids(manifest_lookup_ids)

    results: list[dict] = []
    for stable_id in ordered_ids:
        song_doc = found_docs.get(stable_id)
        if not isinstance(song_doc, dict):
            continue
        manifest_key = _normalize_document_identifier(song_doc.get('scanner_stable_id') or song_doc.get('id'))
        manifest_entry = manifest_map.get(manifest_key) if manifest_key else None
        try:
            payload = _serialize_song_detail(song_doc, include_notes=include_notes, manifest_entry=manifest_entry)
        except RuntimeError:
            app.logger.exception('Failed to serialize song detail in batch for %s', stable_id)
            abort(500)
        results.append(payload)

    return jsonify(results)


@app.route(basedir + 'api/modes')
def route_api_modes():
    if not is_modes_manifest_enabled():
        return jsonify({'status': 'disabled'})

    now = time.time()
    cached_payload = _MODES_MANIFEST_CACHE.get('payload')
    expires_at = float(_MODES_MANIFEST_CACHE.get('expires_at', 0.0))
    if cached_payload and expires_at > now:
        return jsonify(cached_payload)

    error_response, categories = _load_categories_documents_for_profile()
    if error_response is not None:
        return error_response

    manifest = build_modes_manifest(categories, cache_ttl=DEFAULT_CACHE_TTL)
    ttl_value = manifest.get('cache_ttl', DEFAULT_CACHE_TTL)
    try:
        ttl_seconds = int(ttl_value)
    except (TypeError, ValueError):
        ttl_seconds = DEFAULT_CACHE_TTL
    if ttl_seconds <= 0:
        ttl_seconds = DEFAULT_CACHE_TTL
    _MODES_MANIFEST_CACHE['payload'] = manifest
    _MODES_MANIFEST_CACHE['expires_at'] = now + ttl_seconds
    LOGGER.info('modes-manifest: count=%d', len(manifest.get('modes', [])))
    return jsonify(manifest)


@app.route(basedir + 'api/tower/chart')
@app.cache.cached(timeout=15, query_string=True)
def route_api_tower_chart():
    title = request.args.get('title', '').strip()
    if not title:
        return jsonify({'status': 'error', 'message': 'missing_title'}), 400
    course_param = request.args.get('course', '').strip().casefold() or 'oni'
    mode_param_raw = request.args.get('mode', '').strip()
    mode_param = mode_param_raw.casefold()
    if mode_param in {'dan', 'dojo'}:
        mode_param = 'dandojo'

    projection = {'_id': False, 'charts': True, 'title': True, 'titleNormalized': True}
    song = _require_song_store().find_one({'title': {'$regex': f'^{re.escape(title)}$', '$options': 'i'}}, projection)
    if song is None:
        normalised_title = title.casefold()
        song = _require_song_store().find_one({'titleNormalized': normalised_title}, projection)
    if song is None:
        return jsonify({'status': 'error', 'message': 'not_found'}), 404

    charts = song.get('charts') if isinstance(song.get('charts'), list) else []
    prefer_modes = (mode_param,) if mode_param else ("tower", "dandojo")
    best_chart = select_best_chart(charts, course_param, prefer_modes=prefer_modes)

    if best_chart is None:
        return jsonify({'status': 'error', 'message': 'chart_not_found'}), 404

    chart_data_source = best_chart.get('chart_data')
    if isinstance(chart_data_source, dict):
        chart_data = dict(chart_data_source)
    else:
        chart_data = {
            'course': best_chart.get('canonical_course') or best_chart.get('course'),
            'total_notes': best_chart.get('total_notes', 0),
            'measures': best_chart.get('measures', []),
        }
    measures = chart_data.get('measures')
    if not isinstance(measures, list):
        measures = []
    duration_value = chart_data.get('duration_ms')
    try:
        duration_ms = int(duration_value)
    except (TypeError, ValueError):
        duration_ms = 0
    if duration_ms < 0:
        duration_ms = 0
    chart_data['duration_ms'] = duration_ms
    normalized_measures = normalize_measures_relative(measures)
    chart_data['measures'] = normalized_measures
    _ensure_chart_duration(chart_data)

    total_notes = chart_data.get('total_notes')
    if not isinstance(total_notes, int):
        try:
            total_notes = int(total_notes)
        except (TypeError, ValueError):
            total_notes = sum(len(m.get('notes', [])) for m in normalized_measures)
    course_label = best_chart.get('display_course') or course_param
    duration_value = chart_data.get('duration_ms')
    try:
        duration_int = int(duration_value)
    except (TypeError, ValueError):
        duration_int = duration_ms

    response = {
        'status': 'ok',
        'title': song.get('title'),
        'mode': best_chart.get('mode'),
        'display_course': best_chart.get('display_course'),
        'chart_data': chart_data,
    }
    LOGGER.info('tower-chart: title=%s course=%s notes=%d dur_ms=%d', title, course_label, total_notes, duration_int)
    LOGGER.info('chart-final: title=%s course|rank=%s notes=%d dur_ms=%d', song.get('title'), course_label, total_notes, duration_int)
    return jsonify(response)


@app.route(basedir + 'api/dan/chart')
@app.cache.cached(timeout=15, query_string=True)
def route_api_dan_chart():
    if not is_modes_manifest_enabled():
        return jsonify({'status': 'disabled'}), 404

    title = request.args.get('title', '').strip()
    if not title:
        return jsonify({'status': 'error', 'message': 'missing_title'}), 400
    rank_raw = request.args.get('rank', '').strip()
    if not rank_raw:
        return jsonify({'status': 'error', 'message': 'missing_rank'}), 400
    mode_param_raw = request.args.get('mode', '').strip()
    mode_param = mode_param_raw.casefold() or 'dandojo'
    if mode_param in {'dan', 'dojo'}:
        mode_param = 'dandojo'
    rank_param = rank_raw.casefold()

    projection = {'_id': False, 'charts': True, 'title': True, 'titleNormalized': True}
    song = _require_song_store().find_one({'title': {'$regex': f'^{re.escape(title)}$', '$options': 'i'}}, projection)
    if song is None:
        normalised_title = title.casefold()
        song = _require_song_store().find_one({'titleNormalized': normalised_title}, projection)
    if song is None:
        return jsonify({'status': 'error', 'message': 'not_found'}), 404

    charts = song.get('charts') if isinstance(song.get('charts'), list) else []
    prefer_modes = (mode_param,) if mode_param else ('dandojo',)
    best_chart = select_best_chart(charts, rank_param, prefer_modes=prefer_modes)

    if best_chart is None:
        return jsonify({'status': 'error', 'message': 'chart_not_found'}), 404

    chart_data_source = best_chart.get('chart_data')
    if isinstance(chart_data_source, dict):
        chart_data = dict(chart_data_source)
    else:
        chart_data = {
            'course': best_chart.get('canonical_course') or best_chart.get('course'),
            'total_notes': best_chart.get('total_notes', 0),
            'measures': best_chart.get('measures', []),
        }

    measures = chart_data.get('measures')
    if not isinstance(measures, list):
        measures = []
    duration_value = chart_data.get('duration_ms')
    try:
        duration_ms = int(duration_value)
    except (TypeError, ValueError):
        duration_ms = 0
    if duration_ms < 0:
        duration_ms = 0
    chart_data['duration_ms'] = duration_ms
    normalized_measures = normalize_measures_relative(measures)
    chart_data['measures'] = normalized_measures
    _ensure_chart_duration(chart_data)

    total_notes = chart_data.get('total_notes')
    if not isinstance(total_notes, int):
        try:
            total_notes = int(total_notes)
        except (TypeError, ValueError):
            total_notes = sum(len(m.get('notes', [])) for m in normalized_measures)

    rank_value = best_chart.get('rank')
    if isinstance(rank_value, (int, float)):
        rank_label = str(rank_value)
    elif isinstance(rank_value, str) and rank_value.strip():
        rank_label = rank_value
    else:
        rank_label = best_chart.get('display_course') or rank_param
    duration_value = chart_data.get('duration_ms')
    try:
        duration_int = int(duration_value)
    except (TypeError, ValueError):
        duration_int = duration_ms

    response = {
        'status': 'ok',
        'title': song.get('title'),
        'mode': best_chart.get('mode'),
        'display_course': best_chart.get('display_course'),
        'rank': best_chart.get('rank'),
        'chart_data': chart_data,
    }

    LOGGER.info('dan-chart: title=%s rank=%s notes=%d dur_ms=%d', title, rank_label, total_notes, duration_int)
    LOGGER.info('chart-final: title=%s course|rank=%s notes=%d dur_ms=%d', song.get('title'), rank_label, total_notes, duration_int)
    return jsonify(response)


@app.route(basedir + 'api/categories')
@app.cache.cached(timeout=15)
def route_api_categories():
    error_response, documents = _load_categories_documents_for_profile()
    if error_response is not None:
        return error_response

    categories = _normalize_categories_payload(documents)
    return jsonify(categories)


@app.route(basedir + 'import/report')
def route_import_report():
    unavailable = _desktop_mongo_unavailable_response(api=False)
    if unavailable is not None:
        return unavailable
    state_collection = getattr(db, 'song_scanner_state', None)
    if state_collection is None:
        abort(404)

    try:
        cursor = state_collection.find({}, {'_id': False})
    except Exception:
        app.logger.exception('Failed to load song scanner state for report')
        cursor = []

    grouped: defaultdict[str, list] = defaultdict(list)
    for doc in cursor:
        if not isinstance(doc, dict):
            continue
        key = doc.get('group_key') or doc.get('tja_path') or 'ungrouped'
        grouped[str(key)].append(doc)

    report_groups = []
    for key in sorted(grouped.keys()):
        docs = grouped[key]
        song_id = None
        title = None
        normalized_title = None
        audio_url = None
        issues: set[str] = set()
        diagnostics: set[str] = set()
        total_valid = 0
        total_charts = 0
        records = []

        for doc in docs:
            if song_id is None and isinstance(doc.get('song_id'), int):
                song_id = doc['song_id']
            record = doc.get('record') if isinstance(doc.get('record'), dict) else {}
            if not title and isinstance(record.get('title'), str):
                title = record['title']
            if not normalized_title and isinstance(record.get('normalized_title'), str):
                normalized_title = record['normalized_title']
            if not audio_url and isinstance(record.get('audio_url'), str) and record['audio_url']:
                audio_url = record['audio_url']

            record_issues = set(record.get('import_issues', []) or [])
            issues.update(record_issues)
            diagnostics.update(set(record.get('diagnostics', []) or []))

            charts_raw = record.get('charts', []) or []
            chart_entries = []
            for chart in charts_raw:
                if not isinstance(chart, dict):
                    continue
                chart_entry = {
                    'course': chart.get('course') or 'Unknown',
                    'level': chart.get('level'),
                    'valid': bool(chart.get('valid')),
                    'issues': list(chart.get('issues', []) or []),
                    'coerced': bool(chart.get('coerced')),
                    'tja_path': doc.get('tja_path'),
                }
                chart_entries.append(chart_entry)
                if chart_entry['valid']:
                    total_valid += 1
                total_charts += 1

            records.append({
                'tja_path': doc.get('tja_path'),
                'relative_dir': record.get('relative_dir'),
                'title': record.get('title'),
                'genre': record.get('genre'),
                'category_title': record.get('category_title'),
                'audio_url': record.get('audio_url'),
                'import_issues': sorted(record_issues),
                'diagnostics': sorted(set(record.get('diagnostics', []) or [])),
                'valid_charts': sum(1 for chart in chart_entries if chart['valid']),
                'charts': chart_entries,
            })

        group_entry = {
            'group_key': key,
            'song_id': song_id,
            'title': title,
            'normalized_title': normalized_title,
            'audio_url': audio_url,
            'issues': sorted(issues),
            'diagnostics': sorted(diagnostics),
            'valid_chart_count': total_valid,
            'total_charts': total_charts,
            'records': records,
        }
        report_groups.append(group_entry)

    generated_at = datetime.utcnow()
    summary = {
        'groups': len(report_groups),
        'records': sum(len(group['records']) for group in report_groups),
        'groups_with_issues': sum(1 for group in report_groups if group['issues']),
        'total_charts': sum(group['total_charts'] for group in report_groups),
        'valid_charts': sum(group['valid_chart_count'] for group in report_groups),
    }

    response_format = request.args.get('format', 'html').lower()
    if response_format == 'json':
        payload = {
            'generated_at': generated_at.isoformat() + 'Z',
            'summary': summary,
            'groups': report_groups,
        }
        return jsonify(payload)

    return render_template(
        'import_report.html',
        groups=report_groups,
        summary=summary,
        generated_at=generated_at,
    )


def invalidate_category_cache():
    try:
        app.cache.delete_memoized(route_api_categories)
    except Exception:
        pass


def perform_song_scan(*, full: bool = False):
    mode_label = 'full' if full else 'incremental'
    start_perf = time.perf_counter()
    try:
        summary = song_scanner.scan(full=full)
    except Exception:  # pragma: no cover - defensive runtime guard
        elapsed = max(time.perf_counter() - start_perf, 0.0)
        active_summary = getattr(song_scanner, '_active_summary', None)
        files_count = None
        if isinstance(active_summary, dict):
            files_count = active_summary.get('files_count')
        logger = app.logger if 'app' in globals() else LOGGER
        logger.error(
            'Song scan failed: mode=%s base_dir=%s files_count=%s duration=%.3fs',
            mode_label,
            SONGS_DIR_PATH,
            files_count if files_count is not None else 'unknown',
            elapsed,
            exc_info=True,
        )
        summary = empty_scan_summary(reason='scan_failed')
        summary['duration_seconds'] = round(elapsed, 3)
    summary_dict = summary if isinstance(summary, dict) else {}
    if not summary_dict:
        fallback = empty_scan_summary(reason='scan_empty')
        summary_dict.update(fallback)
        summary_dict['duration_seconds'] = round(max(time.perf_counter() - start_perf, 0.0), 3)
        summary = summary_dict
    leader = summary_dict.get('leader') is True
    fast_path = summary_dict.get('fast_path') is True
    if leader and not fast_path:
        invalidate_category_cache()
    if leader:
        app.logger.info("Song scan finished: %s", summary)
    else:
        app.logger.info("Song scan skipped (no leader): %s", summary)
    metrics_snapshot = summary_dict.get('metrics') if isinstance(summary_dict, dict) else {}
    metrics_map = metrics_snapshot if isinstance(metrics_snapshot, Mapping) else {}
    tja_valid = _coerce_int(metrics_map.get('tja_valid_total'), 0)
    files_count = _coerce_int(summary_dict.get('files_count'), 0)
    errors_count = _coerce_int(summary_dict.get('errors'), 0)
    duration_value = summary_dict.get('duration_seconds', 0.0)
    try:
        duration_ms = int(round(float(duration_value) * 1000))
    except (TypeError, ValueError):
        duration_ms = 0
    app.logger.info(
        'Song scan counters: files=%d tja_valid=%d errors=%d scan_ms=%d',
        files_count,
        tja_valid,
        errors_count,
        duration_ms,
    )
    _maybe_log_startup_duration(fast_path=fast_path)
    return summary


def _get_scan_token():
    header_token = request.headers.get('X-Scan-Token')
    if header_token:
        return header_token.strip()
    auth_header = request.headers.get('Authorization', '')
    if auth_header.lower().startswith('bearer '):
        return auth_header[7:].strip()
    request_json = request.get_json(silent=True) or {}
    if isinstance(request_json, dict) and request_json.get('token'):
        return str(request_json['token'])
    if request.form.get('token'):
        return request.form.get('token')
    return request.args.get('token')


def _should_run_full_scan(request_json):
    if isinstance(request_json, dict):
        mode = request_json.get('mode')
        if isinstance(mode, str) and mode.lower() in {'full', 'complete', 'all'}:
            return True
        if str(request_json.get('full', '')).lower() in {'1', 'true', 'yes'}:
            return True
    for source in (request.form, request.args):
        if source.get('mode', '').lower() in {'full', 'complete', 'all'}:
            return True
        if source.get('full', '').lower() in {'1', 'true', 'yes'}:
            return True
    return False


@app.route(basedir + 'api/admin/scan', methods=['POST'])
def route_admin_scan():
    token = _get_scan_token()
    if ADMIN_SCAN_TOKEN and token != ADMIN_SCAN_TOKEN:
        app.logger.warning('Unauthorized scan attempt')
        return abort(403)

    payload = request.get_json(silent=True) or {}
    summary = perform_song_scan(full=_should_run_full_scan(payload))
    return jsonify({'status': 'ok', 'summary': summary})

@app.route(basedir + 'api/config')
@app.cache.cached(timeout=15)
def route_api_config():
    config = get_config(credentials=True)
    return jsonify(config)


@app.route(basedir + 'api/register', methods=['POST'])
def route_api_register():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    data = request.get_json()
    if not schema.validate(data, schema.register):
        return abort(400)

    if session.get('username'):
        session.clear()

    username = data.get('username', '')
    if len(username) < 3 or len(username) > 20 or not re.match('^[a-zA-Z0-9_]{3,20}$', username):
        return api_error('invalid_username')

    if db.users.find_one({'username_lower': username.lower()}):
        return api_error('username_in_use')

    password = data.get('password', '').encode('utf-8')
    if not 6 <= len(password) <= 5000:
        return api_error('invalid_password')

    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password, salt)
    don = get_default_don()
    
    session_id = os.urandom(24).hex()
    db.users.insert_one({
        'username': username,
        'username_lower': username.lower(),
        'password': hashed,
        'display_name': username,
        'don': don,
        'user_level': 1,
        'session_id': session_id
    })

    session['session_id'] = session_id
    session['username'] = username
    session.permanent = True
    return jsonify({'status': 'ok', 'username': username, 'display_name': username, 'don': don})


@app.route(basedir + 'api/login', methods=['POST'])
def route_api_login():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    data = request.get_json()
    if not schema.validate(data, schema.login):
        return abort(400)

    if session.get('username'):
        session.clear()

    username = data.get('username', '')
    result = db.users.find_one({'username_lower': username.lower()})
    if not result:
        return api_error('invalid_username_password')

    password = data.get('password', '').encode('utf-8')
    if not bcrypt.checkpw(password, result['password']):
        return api_error('invalid_username_password')
    
    don = get_db_don(result)
    
    session['session_id'] = result['session_id']
    session['username'] = result['username']
    session.permanent = True if data.get('remember') else False

    return jsonify({'status': 'ok', 'username': result['username'], 'display_name': result['display_name'], 'don': don})


@app.route(basedir + 'api/logout', methods=['POST'])
@login_required
def route_api_logout():
    session.clear()
    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/account/display_name', methods=['POST'])
@login_required
def route_api_account_display_name():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    data = request.get_json()
    if not schema.validate(data, schema.update_display_name):
        return abort(400)

    display_name = data.get('display_name', '').strip()
    if not display_name:
        display_name = session.get('username')
    elif len(display_name) > 25:
        return api_error('invalid_display_name')
    
    db.users.update_one({'username': session.get('username')}, {
        '$set': {'display_name': display_name}
    })

    return jsonify({'status': 'ok', 'display_name': display_name})


@app.route(basedir + 'api/account/don', methods=['POST'])
@login_required
def route_api_account_don():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    data = request.get_json()
    if not schema.validate(data, schema.update_don):
        return abort(400)
    
    don_body_fill = data.get('body_fill', '').strip()
    don_face_fill = data.get('face_fill', '').strip()
    if len(don_body_fill) != 7 or\
        not don_body_fill.startswith("#")\
        or not is_hex(don_body_fill[1:])\
        or len(don_face_fill) != 7\
        or not don_face_fill.startswith("#")\
        or not is_hex(don_face_fill[1:]):
        return api_error('invalid_don')
    
    db.users.update_one({'username': session.get('username')}, {'$set': {
        'don_body_fill': don_body_fill,
        'don_face_fill': don_face_fill,
    }})
    
    return jsonify({'status': 'ok', 'don': {'body_fill': don_body_fill, 'face_fill': don_face_fill}})


@app.route(basedir + 'api/account/password', methods=['POST'])
@login_required
def route_api_account_password():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    data = request.get_json()
    if not schema.validate(data, schema.update_password):
        return abort(400)

    user = db.users.find_one({'username': session.get('username')})
    current_password = data.get('current_password', '').encode('utf-8')
    if not bcrypt.checkpw(current_password, user['password']):
        return api_error('current_password_invalid')
    
    new_password = data.get('new_password', '').encode('utf-8')
    if not 6 <= len(new_password) <= 5000:
        return api_error('invalid_new_password')
    
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(new_password, salt)
    session_id = os.urandom(24).hex()

    db.users.update_one({'username': session.get('username')}, {
        '$set': {'password': hashed, 'session_id': session_id}
    })

    session['session_id'] = session_id
    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/account/remove', methods=['POST'])
@login_required
def route_api_account_remove():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    data = request.get_json()
    if not schema.validate(data, schema.delete_account):
        return abort(400)

    user = db.users.find_one({'username': session.get('username')})
    password = data.get('password', '').encode('utf-8')
    if not bcrypt.checkpw(password, user['password']):
        return api_error('verify_password_invalid')

    db.scores.delete_many({'username': session.get('username')})
    db.users.delete_one({'username': session.get('username')})

    session.clear()
    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/scores/save', methods=['POST'])
@login_required
def route_api_scores_save():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    data = request.get_json()
    if not schema.validate(data, schema.scores_save):
        return abort(400)

    username = session.get('username')
    if data.get('is_import'):
        db.scores.delete_many({'username': username})

    scores = data.get('scores', [])
    for score in scores:
        db.scores.update_one({'username': username, 'hash': score['hash']},
        {'$set': {
            'username': username,
            'hash': score['hash'],
            'score': score['score']
        }}, upsert=True)

    return jsonify({'status': 'ok'})


@app.route(basedir + 'api/scores/get')
@login_required
def route_api_scores_get():
    unavailable = _desktop_mongo_unavailable_response(api=True)
    if unavailable is not None:
        return unavailable
    username = session.get('username')

    scores = []
    for score in db.scores.find({'username': username}):
        scores.append({
            'hash': score['hash'],
            'score': score['score']
        })

    user = db.users.find_one({'username': username})
    don = get_db_don(user)
    return jsonify({'status': 'ok', 'scores': scores, 'username': user['username'], 'display_name': user['display_name'], 'don': don})


@app.route(basedir + 'privacy')
def route_api_privacy():
    last_modified = time.strftime('%d %B %Y', time.gmtime(os.path.getmtime('templates/privacy.txt')))
    integration = take_config('GOOGLE_CREDENTIALS')['gdrive_enabled'] if take_config('GOOGLE_CREDENTIALS') else False
    
    response = make_response(render_template('privacy.txt', last_modified=last_modified, config=get_config(), integration=integration))
    response.headers['Content-type'] = 'text/plain; charset=utf-8'
    return response


def make_preview(song_id, song_type, song_ext, preview):
    song_path = 'public/songs/%s/main.%s' % (song_id, song_ext)
    prev_path = 'public/songs/%s/preview.mp3' % song_id

    if os.path.isfile(song_path) and not os.path.isfile(prev_path):
        if not preview or preview <= 0:
            print('Skipping #%s due to no preview' % song_id)
            return False

        print('Making preview.mp3 for song #%s' % song_id)
        ff = FFmpeg(inputs={song_path: '-ss %s' % preview},
                    outputs={prev_path: '-codec:a libmp3lame -ar 32000 -b:a 92k -y -loglevel panic'})
        ff.run()

    return prev_path

error_pages = take_config('ERROR_PAGES') or {}

def create_error_page(code, url):
    if url.startswith("http://") or url.startswith("https://"):
        resp = requests.get(url)
        if resp.status_code == 200:
            app.register_error_handler(code, lambda e: (resp.content, code))
    else:
        if url.startswith(basedir):
            url = url[len(basedir):]
        path = os.path.normpath(os.path.join("public", url))
        if os.path.isfile(path):
            app.register_error_handler(code, lambda e: (send_from_directory(".", path), code))

for code in error_pages:
    if error_pages[code]:
        create_error_page(code, error_pages[code])

def cache_wrap(res_from, secs):
    res = flask.make_response(res_from)

    if os.environ.get("FLASK_ENV") == "production":
        res.headers["Cache-Control"] = f"public, max-age={secs}, s-maxage={secs}"
        res.headers["CDN-Cache-Control"] = f"max-age={secs}"
    else:
        res.headers["Cache-Control"] = "no-cache"

    return res

if RUN_PROFILE != 'desktop':

    @app.route(basedir + "src/<path:ref>")
    def send_src(ref):
        return cache_wrap(flask.send_from_directory(str(PUBLIC_DIR_PATH / 'src'), ref), 3600)

    @app.route(basedir + "assets/<path:ref>")
    def send_assets(ref):
        return cache_wrap(flask.send_from_directory(str(PUBLIC_DIR_PATH / 'assets'), ref), 3600)

    @app.route(basedir + "songs/<path:ref>")
    def send_songs(ref):
        if not SONGS_DIR_PATH.exists():
            app.logger.warning('Songs directory %s missing while serving %s', SONGS_DIR_PATH, ref)
            abort(404)
        return cache_wrap(flask.send_from_directory(str(SONGS_DIR_PATH), ref), 604800)

    @app.route('/<path:spa_path>')
    def route_frontend_spa(spa_path: str):
        return _serve_frontend_asset(spa_path)

else:

    DESKTOP_PUBLIC_DIR = public_dir()
    DESKTOP_VIEWS_DIR = DESKTOP_PUBLIC_DIR / "src" / "views"
    DESKTOP_SONGS_DIR = songs_dir()
    DESKTOP_SONGS_DIR.mkdir(parents=True, exist_ok=True)

    def _desktop_normalize_identifier(value: object) -> Optional[str]:
        if isinstance(value, str):
            token = value.strip()
            return token or None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            text = str(value).strip()
            return text or None
        return None

    def _desktop_fetch_song_document(
        identifier: str,
        *,
        song_store: Optional[SongStoreInterface] = None,
        projection: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        store = song_store or _require_song_store()
        return _load_song_document_for_identifier(
            identifier,
            projection=projection,
            song_store=store,
        )

    def _desktop_load_manifest_entry(
        song_doc: Mapping[str, Any],
        *,
        manifest_store: Optional[ManifestStoreInterface] = None,
    ) -> Optional[Mapping[str, Any]]:
        store = manifest_store if manifest_store is not None else _get_manifest_store()
        if store is None:
            return None
        stable_identifier = _desktop_normalize_identifier(song_doc.get("scanner_stable_id"))
        if not stable_identifier:
            return None
        try:
            entry = store.get(stable_identifier)
        except Exception:
            app.logger.debug(
                "desktop manifest lookup failed id=%s", stable_identifier, exc_info=app.logger.isEnabledFor(logging.DEBUG)
            )
            return None
        return dict(entry) if isinstance(entry, Mapping) else None

    def _desktop_normalize_posix_path(value: object) -> Optional[PurePosixPath]:
        if isinstance(value, PurePosixPath):
            return value
        if isinstance(value, str):
            token = value.strip().replace("\\", "/")
            if not token:
                return None
            try:
                return PurePosixPath(token)
            except ValueError:
                return None
        return None

    def _desktop_extract_relative_path_from_url(value: object) -> Optional[PurePosixPath]:
        if not isinstance(value, str):
            return None
        token = value.strip()
        if not token:
            return None
        parsed = urlparse(token)
        path_candidate = parsed.path if parsed.scheme or parsed.netloc else token
        normalized = _desktop_normalize_posix_path(path_candidate)
        if normalized is None:
            return None
        parts = list(normalized.parts)
        if not parts:
            return None
        if parts and parts[0] == "/":
            parts = parts[1:]
        if not parts:
            return None
        if "songs" in parts:
            try:
                index = parts.index("songs")
            except ValueError:
                index = -1
            if index >= 0:
                parts = parts[index + 1 :]
        if not parts:
            return None
        return PurePosixPath(*parts)

    def _desktop_find_first_tja_in_directory(
        songs_root: Path, relative_dir: PurePosixPath
    ) -> Optional[PurePosixPath]:
        try:
            target = (songs_root / relative_dir.as_posix()).resolve()
        except Exception:
            return None
        try:
            target.relative_to(songs_root)
        except ValueError:
            return None
        if not target.is_dir():
            return None
        try:
            entries = [child for child in target.iterdir() if child.is_file() and child.suffix.lower() == ".tja"]
        except FileNotFoundError:
            return None
        if not entries:
            return None
        main_candidates = [entry for entry in entries if entry.name.lower() == "main.tja"]
        if main_candidates:
            selected = min(main_candidates, key=lambda item: item.name.lower())
        else:
            selected = min(entries, key=lambda item: item.name.lower())
        try:
            relative_path = selected.relative_to(songs_root)
        except ValueError:
            return None
        return PurePosixPath(relative_path.as_posix())

    def _desktop_resolve_main_tja_relative_path(
        song_doc: Mapping[str, Any],
        manifest_entry: Optional[Mapping[str, Any]],
        songs_root: Path,
    ) -> PurePosixPath:
        candidate_files: list[PurePosixPath] = []
        candidate_dirs: list[PurePosixPath] = []

        def _register_file_candidate(value: object) -> None:
            normalized = _desktop_normalize_posix_path(value)
            if normalized is None:
                return
            if normalized not in candidate_files:
                candidate_files.append(normalized)
            parent = normalized.parent if normalized.parent != PurePosixPath("") else PurePosixPath(".")
            if parent not in candidate_dirs:
                candidate_dirs.append(parent)

        def _register_dir_candidate(value: object) -> None:
            normalized = _desktop_extract_relative_path_from_url(value)
            if normalized is None:
                return
            directory = normalized if normalized != PurePosixPath("") else PurePosixPath(".")
            if directory not in candidate_dirs:
                candidate_dirs.append(directory)

        def _register_identifier_directory(value: object) -> None:
            identifier = _desktop_normalize_identifier(value)
            if not identifier:
                return
            directory = PurePosixPath(identifier)
            if directory not in candidate_dirs:
                candidate_dirs.append(directory)
            default_main = directory / "main.tja"
            if default_main not in candidate_files:
                candidate_files.append(default_main)

        if isinstance(manifest_entry, Mapping):
            _register_file_candidate(manifest_entry.get("file_path"))
            paths_map = manifest_entry.get("paths") if isinstance(manifest_entry.get("paths"), Mapping) else None
            if paths_map:
                _register_file_candidate(paths_map.get("tja_url"))
                _register_dir_candidate(paths_map.get("dir_url"))

        paths_doc = song_doc.get("paths") if isinstance(song_doc.get("paths"), Mapping) else None
        if paths_doc:
            _register_file_candidate(paths_doc.get("tja_url"))
            _register_dir_candidate(paths_doc.get("dir_url"))

        _register_identifier_directory(song_doc.get("song_id"))
        _register_identifier_directory(song_doc.get("scanner_stable_id"))

        for candidate in candidate_files:
            try:
                absolute = (songs_root / candidate.as_posix()).resolve()
            except Exception:
                continue
            try:
                absolute.relative_to(songs_root)
            except ValueError:
                continue
            if absolute.is_file():
                return candidate

        visited_dirs: set[PurePosixPath] = set()
        for directory in candidate_dirs:
            if directory in visited_dirs:
                continue
            visited_dirs.add(directory)
            relative = _desktop_find_first_tja_in_directory(songs_root, directory)
            if relative is not None:
                return relative

        raise FileNotFoundError("main TJA file not found for song")

    def resolve_main_tja_path(
        song_identifier: str,
        *,
        song_store: Optional[SongStoreInterface] = None,
        manifest_store: Optional[ManifestStoreInterface] = None,
        songs_dir: Optional[Path] = None,
    ) -> Path:
        normalized_id = _desktop_normalize_identifier(song_identifier)
        if not normalized_id:
            raise FileNotFoundError("song identifier is missing")
        store = song_store or _require_song_store()
        songs_root = Path(songs_dir) if songs_dir is not None else DESKTOP_SONGS_DIR
        document = _desktop_fetch_song_document(normalized_id, song_store=store)
        if document is None:
            raise FileNotFoundError("song not found")
        manifest_entry = _desktop_load_manifest_entry(document, manifest_store=manifest_store)
        relative = _desktop_resolve_main_tja_relative_path(document, manifest_entry, songs_root)
        absolute = (songs_root / relative.as_posix()).resolve()
        try:
            absolute.relative_to(songs_root)
        except ValueError as exc:
            raise FileNotFoundError("resolved path escapes songs directory") from exc
        if not absolute.is_file():
            raise FileNotFoundError("main TJA file missing")
        return absolute

    def resolve_song_file_path(
        song_identifier: str,
        requested_name: str,
        *,
        song_store: Optional[SongStoreInterface] = None,
        manifest_store: Optional[ManifestStoreInterface] = None,
        songs_dir: Optional[Path] = None,
    ) -> Path:
        normalized_id = _desktop_normalize_identifier(song_identifier)
        if not normalized_id:
            raise FileNotFoundError("song identifier is missing")
        name_token = requested_name.strip()
        if not name_token:
            raise FileNotFoundError("requested asset name is missing")
        store = song_store or _require_song_store()
        songs_root = Path(songs_dir) if songs_dir is not None else DESKTOP_SONGS_DIR
        document = _desktop_fetch_song_document(normalized_id, song_store=store)
        if document is None:
            raise FileNotFoundError("song not found")
        manifest_entry = _desktop_load_manifest_entry(document, manifest_store=manifest_store)
        relative_main = _desktop_resolve_main_tja_relative_path(document, manifest_entry, songs_root)
        if name_token.lower() == "main.tja":
            relative = relative_main
        else:
            component = _desktop_normalize_posix_path(name_token)
            if component is None or component.is_absolute():
                raise FileNotFoundError("invalid asset path")
            relative = relative_main.parent.joinpath(component)
        absolute = (songs_root / relative.as_posix()).resolve()
        try:
            absolute.relative_to(songs_root)
        except ValueError as exc:
            raise FileNotFoundError("resolved path escapes songs directory") from exc
        if not absolute.is_file():
            raise FileNotFoundError("requested asset missing")
        return absolute

    @app.route("/")
    def desktop_root_loader():
        return send_from_directory(str(DESKTOP_VIEWS_DIR), "loader.html")

    @app.route("/<name>.html")
    def desktop_html_page(name: str):
        views_path = DESKTOP_VIEWS_DIR / f"{name}.html"
        if views_path.is_file():
            return send_from_directory(str(DESKTOP_VIEWS_DIR), f"{name}.html")
        root_path = DESKTOP_PUBLIC_DIR / f"{name}.html"
        if root_path.is_file():
            return send_from_directory(str(DESKTOP_PUBLIC_DIR), f"{name}.html")
        abort(404)

    @app.route("/assets/<path:filename>")
    def desktop_public_assets(filename: str):
        return cache_wrap(
            send_from_directory(str(DESKTOP_PUBLIC_DIR / "assets"), filename),
            3600,
        )

    @app.route("/src/<path:filename>")
    def desktop_public_src(filename: str):
        return cache_wrap(
            send_from_directory(str(DESKTOP_PUBLIC_DIR / "src"), filename),
            3600,
        )

    @app.route("/songs/<path:filename>")
    def desktop_song_files(filename: str):
        if not isinstance(filename, str) or not filename:
            abort(404)
        parts = filename.split("/", 1)
        raw_song_id = parts[0].strip()
        if not raw_song_id:
            abort(404)
        if len(parts) < 2 or not parts[1].strip():
            abort(404)
        try:
            asset_path = resolve_song_file_path(raw_song_id, parts[1])
        except FileNotFoundError:
            abort(404)
        except Exception:  # pragma: no cover - defensive logging
            app.logger.exception("Failed to resolve song asset id=%s name=%s", raw_song_id, parts[1])
            abort(500)
        return cache_wrap(flask.send_file(asset_path), 604800)


@app.route(basedir + "manifest.json")
def send_manifest():
    return cache_wrap(flask.send_from_directory(str(PUBLIC_DIR_PATH), "manifest.json"), 3600)


def _start_song_directory_watcher():
    global _song_watcher_handle
    if _song_watcher_handle is not None:
        return
    if not ENABLE_SONG_WATCHER:
        app.logger.info('Song directory watcher disabled')
        return
    if not song_scanner.watchdog_supported:
        app.logger.info('watchdog not available; live song updates disabled')
        return
    if not SONGS_DIR_PATH.exists():
        app.logger.warning('Songs directory %s missing; live song updates disabled', SONGS_DIR_PATH)
        return
    if not hasattr(song_scanner, 'has_leader_lock'):
        app.logger.info('Song directory watcher not started: leader lock unavailable')
        return
    if not song_scanner.has_leader_lock():
        app.logger.info('Song directory watcher not started: scanner is not leader')
        return

    def _run_scan():
        with app.app_context():
            try:
                perform_song_scan(full=False)
            except Exception:
                app.logger.exception('Live song scan failed')

    try:
        handle = song_scanner.start_watcher(callback=_run_scan, debounce_seconds=0.75)
        if handle:
            key_value = getattr(song_scanner, 'leader_lock_key', 'taiko:scanner:leader')
            app.logger.info('Song directory watcher started (pid=%s, key=%s)', os.getpid(), key_value)
            _song_watcher_handle = handle
    except KeyboardInterrupt:
        raise
    except SystemExit as exc:
        app.logger.error('Failed to start song directory watcher (exiting): %s', exc, exc_info=True)
    except Exception:
        app.logger.exception('Failed to start song directory watcher')


# Run an eager scan at startup when configured and immediately start the
# directory watcher if this process currently owns the scanner leader lock.
if SCAN_ON_START != 'skip':
    try:
        perform_song_scan(full=SCAN_ON_START == 'force')
    except Exception:
        app.logger.exception('Automatic song scan failed')
    else:
        if song_scanner.has_leader_lock():
            _start_song_directory_watcher()


# Flask 3 removed the ``before_serving`` decorator. Provide a compatible fallback
# that runs the hook before the first request is processed so that the song
# directory watcher still starts automatically.
if hasattr(app, "before_serving"):
    _song_watcher_hook = app.before_serving
else:
    def _song_watcher_hook(func):
        has_run = False
        lock = threading.Lock()

        @wraps(func)
        def _run_once():
            nonlocal has_run
            if has_run:
                return
            with lock:
                if has_run:
                    return
                has_run = True
            func()

        if hasattr(app, "before_first_request"):
            app.before_first_request(_run_once)
        else:
            app.before_request(_run_once)
        return func


@_song_watcher_hook
def _ensure_song_directory_watcher_started():
    _start_song_directory_watcher()

if __name__ == '__main__':
    import argparse, sys

    parser = argparse.ArgumentParser(description='Run the taiko-web development server.')
    parser.add_argument('port', type=int, metavar='PORT', nargs='?', default=34801, help='Port to listen on.')
    parser.add_argument('-b', '--bind-address', default='localhost', help='Bind server to address.')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode.')
    args = parser.parse_args()

    run_profile = RUN_PROFILE

    if run_profile == 'web':
        app.run(host=args.bind_address, port=args.port, debug=args.debug)
    elif run_profile == 'desktop':
        LOGGER.info('RUN_PROFILE=desktop stub: exiting without starting the server.')
        sys.exit(0)
    else:
        LOGGER.error('Unknown run_profile=%s; defaulting to web', run_profile)
        app.run(host=args.bind_address, port=args.port, debug=args.debug)

_SONG_DETAIL_PROJECTION = {
    '_id': False,
    'id': True,
    'scanner_stable_id': True,
    'title': True,
    'titleJa': True,
    'subtitle': True,
    'subtitleJa': True,
    'category': True,
    'preview': True,
    'paths': True,
    'music_type': True,
    'type': True,
    'courses': True,
    'import_issues': True,
    'valid_chart_count': True,
    'charts': True,
    'hash': True,
    'fingerprint': True,
}


def _load_manifest_entries_for_ids(ids: list[str]) -> dict[str, dict]:
    store = _get_manifest_store()
    if store is None or not ids:
        return {}
    unique_ids = sorted({identifier for identifier in ids if isinstance(identifier, str) and identifier})
    if not unique_ids:
        return {}
    projection = {
        '_id': True,
        'duration_ms': True,
        'sha1': True,
        'preview_available': True,
        'paths': True,
    }
    try:
        cursor = store.find({'_id': {'$in': unique_ids}}, projection)
    except Exception:
        app.logger.debug('Failed to load manifest entries for %d ids', len(unique_ids), exc_info=True)
        return {}
    result: dict[str, dict] = {}
    for doc in cursor:
        if isinstance(doc, dict) and isinstance(doc.get('_id'), str):
            result[str(doc['_id'])] = doc
    return result


def _serialize_song_detail(song_doc: dict, *, include_notes: bool, manifest_entry: Optional[dict] = None) -> dict:
    charts_payload = song_doc.get('charts') if isinstance(song_doc.get('charts'), list) else []
    sanitized_charts: list[dict[str, object]] = []
    max_duration = 0
    for entry in charts_payload:
        if not isinstance(entry, dict):
            continue
        chart_data = entry.get('chart_data') if isinstance(entry.get('chart_data'), dict) else None
        if isinstance(chart_data, dict):
            _ensure_chart_duration(chart_data)
            duration_val = chart_data.get('duration_ms')
        else:
            duration_val = entry.get('duration_ms')
        try:
            duration_int = int(duration_val) if duration_val is not None else 0
        except (TypeError, ValueError):
            duration_int = 0
        if duration_int > max_duration:
            max_duration = duration_int
        sanitized_entry = {
            'course': entry.get('course'),
            'canonical_course': entry.get('canonical_course'),
            'mode': entry.get('mode'),
            'display_course': entry.get('display_course'),
            'level': entry.get('level'),
            'branch': entry.get('branch'),
            'valid': entry.get('valid'),
            'issues': entry.get('issues'),
            'total_notes': entry.get('total_notes'),
            'tja_path': entry.get('tja_path'),
            'rank': entry.get('rank'),
            'tja_url': entry.get('tja_url'),
        }
        if include_notes and isinstance(chart_data, dict):
            sanitized_entry['chart_data'] = chart_data
        sanitized_charts.append(sanitized_entry)

    courses_doc = song_doc.get('courses') if isinstance(song_doc.get('courses'), dict) else {}
    difficulties = {key: bool(courses_doc.get(key)) for key in ('easy', 'normal', 'hard', 'oni', 'ura')}

    manifest_duration = None
    manifest_sha1 = None
    manifest_preview_available = None
    manifest_paths = None
    if isinstance(manifest_entry, dict):
        manifest_duration = manifest_entry.get('duration_ms')
        manifest_sha1 = manifest_entry.get('sha1')
        manifest_preview_available = manifest_entry.get('preview_available')
        manifest_paths = manifest_entry.get('paths') if isinstance(manifest_entry.get('paths'), dict) else None
        try:
            duration_candidate = int(manifest_duration)
        except (TypeError, ValueError):
            duration_candidate = None
        if duration_candidate:
            max_duration = max(max_duration, duration_candidate)

    def _normalize_identifier(value: object) -> Optional[str]:
        if isinstance(value, str):
            token = value.strip()
            return token or None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                return str(int(value))
            except (TypeError, ValueError):
                return None
        return None

    primary_identifier = _normalize_identifier(song_doc.get('song_id'))
    if not primary_identifier:
        primary_identifier = _normalize_identifier(song_doc.get('scanner_stable_id'))
    if not primary_identifier:
        app.logger.error('Song detail serialization missing identifier document=%s', song_doc)
        raise RuntimeError('song detail missing identifier')

    legacy_identifier = _normalize_identifier(song_doc.get('id'))

    payload = {
        'id': primary_identifier,
        'legacy_id': legacy_identifier,
        'title': song_doc.get('title'),
        'titleJa': song_doc.get('titleJa'),
        'subtitle': song_doc.get('subtitle'),
        'subtitleJa': song_doc.get('subtitleJa'),
        'category': song_doc.get('category'),
        'preview': song_doc.get('preview'),
        'music_type': song_doc.get('music_type'),
        'type': song_doc.get('type') or 'tja',
        'source_type': song_doc.get('source_type') or song_doc.get('type') or 'tja',
        'paths': song_doc.get('paths'),
        'courses': courses_doc,
        'difficulties': difficulties,
        'charts': sanitized_charts,
        'import_issues': song_doc.get('import_issues', []),
        'valid_chart_count': song_doc.get('valid_chart_count', 0),
        'duration_ms': max_duration,
        'hash': song_doc.get('hash'),
        'fingerprint': song_doc.get('fingerprint'),
    }

    if manifest_paths and isinstance(payload.get('paths'), dict):
        merged_paths = dict(payload['paths'])
        merged_paths.update(manifest_paths)
        payload['paths'] = merged_paths
    elif manifest_paths:
        payload['paths'] = manifest_paths

    if manifest_sha1 is not None:
        payload['sha1'] = manifest_sha1
    elif isinstance(song_doc.get('sha1'), str) and song_doc.get('sha1'):
        payload['sha1'] = song_doc['sha1']
    if manifest_preview_available is not None:
        payload['preview_available'] = bool(manifest_preview_available)
    elif 'preview_available' not in payload:
        payload['preview_available'] = bool(song_doc.get('preview'))

    return payload
