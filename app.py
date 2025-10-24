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
import time
import unicodedata
from typing import Callable, Mapping, Optional, cast
from urllib.parse import unquote, urlparse
from collections import defaultdict
from pathlib import Path

# -- カスタム --
from datetime import datetime

import flask

# ----

from functools import wraps
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
)
from flask_caching import Cache
from flask_compress import Compress
from flask_session import Session
from flask_wtf.csrf import CSRFProtect, generate_csrf, CSRFError
from ffmpy import FFmpeg
from pymongo import MongoClient, ReturnDocument
from redis import Redis

from songs_scanner import SongScanner
from tower_chart_selection import select_best_chart
from tower_chart_normalization import normalize_measures_relative
from modes_manifest import build_modes_manifest, DEFAULT_CACHE_TTL
from tools.init_db_schema import init_db_schema
from storage.factory import StorageBundle, create_storage_bundle
from storage.interfaces import (
    LeaderLock as LeaderLockInterface,
    ManifestStore as ManifestStoreInterface,
    SongStore as SongStoreInterface,
)


LOGGER = logging.getLogger(__name__)


RUN_PROFILE = os.getenv("RUN_PROFILE", "web")


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


class MongoDispatcher:
    def __init__(self, client_factory: Callable[[], MongoClient], db_name: str):
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

    def get_client(self) -> MongoClient:
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
        for handler in list(root.handlers):
            root.removeHandler(handler)
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                "%Y-%m-%d %H:%M:%S",
            )
        )
        root.addHandler(handler)

    root.setLevel(logging.DEBUG)
    for name in ("taiko.scanner", "taiko.scanner.summary"):
        logging.getLogger(name).propagate = True


client = None
db = None
basedir = '/'
SCAN_ON_START = 'auto'
ENABLE_SONG_WATCHER = True
SCAN_IGNORE_GLOBS: list[str] = []
ADMIN_SCAN_TOKEN = ''
SONGS_BASEURL_VALUE = ''
COERCE_UNKNOWN_COURSE = None
SONGS_DIR_PATH = Path('.')
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
sess = Session()


def create_app():
    global client, db, basedir, SCAN_ON_START, ENABLE_SONG_WATCHER, SCAN_IGNORE_GLOBS
    global ADMIN_SCAN_TOKEN, SONGS_BASEURL_VALUE, COERCE_UNKNOWN_COURSE, SONGS_DIR_PATH
    global song_scanner, _song_watcher_handle
    global _mongo_dispatcher, _redis_dispatcher, _song_scanner_provider
    global _startup_scan_started_at, _startup_scan_logged

    setup_stdout_logging()

    _startup_scan_started_at = time.monotonic()
    _startup_scan_logged = False

    app_instance = Flask(__name__)
    app_instance.logger.info("run_profile=%s", RUN_PROFILE)
    app_instance.config.setdefault('COMPRESS_MIN_SIZE', 1024)
    compress.init_app(app_instance)

    mongo_config = take_config('MONGO') or {}
    mongo_uri = os.environ.get("TAIKO_WEB_MONGO_URI") or mongo_config.get('uri')
    mongo_host = os.environ.get("TAIKO_WEB_MONGO_HOST") or mongo_config.get('host')

    mongo_hosts = mongo_host or ['127.0.0.1:27017']

    def _create_mongo_client() -> MongoClient:
        if mongo_uri:
            return MongoClient(mongo_uri)
        return MongoClient(host=mongo_hosts)

    basedir_value = os.environ.get('BASEDIR') or take_config('BASEDIR') or '/'

    app_instance.secret_key = take_config('SECRET_KEY') or 'change-me'
    redis_config = dict(take_config('REDIS', required=True))

    redis_host = os.environ.get("TAIKO_WEB_REDIS_HOST") or redis_config.get('CACHE_REDIS_HOST') or 'redis'
    redis_port = int(os.environ.get("TAIKO_WEB_REDIS_PORT", redis_config.get('CACHE_REDIS_PORT', 6379)))
    redis_password_env = os.environ.get("TAIKO_WEB_REDIS_PASSWORD")
    redis_password = redis_password_env if redis_password_env is not None else redis_config.get('CACHE_REDIS_PASSWORD')
    redis_db = int(os.environ.get("TAIKO_WEB_REDIS_DB", redis_config.get('CACHE_REDIS_DB', 0)))

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
    db_name = os.environ.get("TAIKO_WEB_MONGO_DB") or mongo_config.get('database') or 'taiko'

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

    global _storage_bundle, SONG_STORE, MANIFEST_STORE, LEADER_LOCK
    _storage_bundle = create_storage_bundle(
        run_profile=RUN_PROFILE,
        mongo_database_factory=_mongo_dispatcher.get_database,
        redis_client_factory=_redis_dispatcher.get_client,
    )
    LOGGER.info('storage bundle initialized')
    SONG_STORE = _storage_bundle.song_store
    MANIFEST_STORE = _storage_bundle.manifest_store
    LEADER_LOCK = _storage_bundle.leader_lock

    app_instance.cache = Cache(app_instance, config=redis_config)
    sess.init_app(app_instance)
    #csrf = CSRFProtect(app)

    db_name = os.environ.get("TAIKO_WEB_MONGO_DB") or mongo_config.get('database') or 'taiko'
    if os.getenv('TAIKO_INIT_INDEXES') == '1':
        init_db_schema(_mongo_dispatcher.get_database())

    basedir = basedir_value
    client = _LazyResourceProxy(_mongo_dispatcher.get_client)
    db = _LazyResourceProxy(_mongo_dispatcher.get_database)

    SONGS_DIR_PATH = Path(
        os.environ.get('SONGS_DIR')
        or take_config('SONGS_DIR')
        or os.path.join(os.getcwd(), 'public', 'songs')
    ).expanduser().resolve()

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
        return SongScanner(
            db=_mongo_dispatcher.get_database(),
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
def route_healthcheck():
    status = {'status': 'ok'}
    try:
        client.admin.command('ping')
        status['mongo'] = 'ok'
    except Exception:
        status['status'] = 'error'
        status['mongo'] = 'error'
        return jsonify(status), 503
    try:
        redis_client = app.config.get('SESSION_REDIS')
        if redis_client:
            redis_client.ping()
        status['redis'] = 'ok'
    except Exception:
        status['status'] = 'error'
        status['redis'] = 'error'
        return jsonify(status), 503
    return jsonify(status)


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
            if not session.get('username'):
                return abort(403)
            
            user = db.users.find_one({'username': session.get('username')})
            if user['user_level'] < level:
                return abort(403)

            return f(*args, **kwargs)
        return wrapper
    return decorated_function


@app.errorhandler(CSRFError)
def handle_csrf_error(e):
    return api_error('invalid_csrf')


@app.before_request
def before_request_func():
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


@app.route(basedir)
def route_index():
    version = get_version()

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    return render_template('index.html', version=version, config=get_config(), year=year, month=month, day=day)


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
                    return_document=ReturnDocument.AFTER,
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


@app.route(basedir + 'api/songs')
def route_api_songs():
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

    songs_collection = getattr(db, 'songs', None)
    if songs_collection is None:
        response = make_response(jsonify([]))
        _apply_catalog_cache_headers(response, etag=quoted_etag, cache_control=cache_control, vary=vary_header)
        return response

    limit_param = request.args.get('limit', type=int)
    page_param = request.args.get('page', type=int)
    use_pagination = limit_param is not None or page_param is not None

    if use_pagination:
        limit = limit_param if isinstance(limit_param, int) else 200
        limit = max(1, min(limit, 200))
        page = page_param if isinstance(page_param, int) else 1
        page = max(page, 1)
        skip = (page - 1) * limit
    else:
        limit = None
        skip = 0

    filters: dict[str, object] = {'is_hidden': {'$ne': True}, 'is_playable': True}
    category_param = request.args.get('category', '')
    if isinstance(category_param, str):
        category_value = category_param.strip()
        if category_value:
            filters['category'] = category_value

    search_param = request.args.get('q', '')
    if isinstance(search_param, str):
        search_value = search_param.strip().lower()
        if search_value:
            filters['title_lc'] = {'$regex': f'^{re.escape(search_value)}'}

    projection = {'_id': 0}
    projection.update({
        'id': 1, 'scanner_stable_id': 1,
        'title': 1, 'subtitle': 1,
        'category': 1, 'category_id': 1,
        'duration_ms': 1, 'preview_available': 1,
        'source_type': 1,
        'paths': 1,
        'is_playable': 1,
        'difficulties': 1,
    })

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
        raw_payload = list(cursor)
    except Exception:
        app.logger.exception('Failed to query songs catalog')
        raw_payload = []

    payload: list[dict[str, object]] = []
    for entry in raw_payload:
        if not isinstance(entry, dict):
            continue
        item = dict(entry)
        item['id'] = item.pop('scanner_stable_id', item.get('id'))
        item['subtitle'] = item['subtitle'] if isinstance(item.get('subtitle'), str) else ''
        item['difficulties'] = _normalize_difficulties(entry, assume_valid=CATALOG_ASSUME_VALID)
        item['is_playable'] = bool(item.get('is_playable')) or bool(CATALOG_ASSUME_VALID)
        item['preview_available'] = bool(item.get('preview_available'))
        source_type_value = item.get('source_type')
        if not isinstance(source_type_value, str) or not source_type_value:
            item['source_type'] = 'tja'
        duration_value = item.get('duration_ms')
        try:
            item['duration_ms'] = int(duration_value) if duration_value is not None else 0
        except (TypeError, ValueError):
            item['duration_ms'] = 0
        item['category_id'] = _coerce_int(item.get('category_id'), 0)
        paths_value = item.get('paths')
        if isinstance(paths_value, dict):
            item['paths'] = {
                key: v
                for key in ('tja_url', 'audio_url', 'dir_url')
                if (v := paths_value.get(key))
            }
        else:
            item['paths'] = {}
        payload.append(item)

    response = make_response(jsonify(payload))
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
        song_doc = _require_song_store().find_one({'scanner_stable_id': stable_id}, projection)
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
    payload = _serialize_song_detail(song_doc, include_notes=include_notes, manifest_entry=manifest_entry)
    payload['id'] = stable_id

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

    response = make_response(jsonify(payload))
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
    for doc in docs:
        stable = doc.get('scanner_stable_id') or doc.get('id')
        if not isinstance(stable, str) or not stable:
            continue
        found_docs[stable] = doc

    manifest_map = _load_manifest_entries_for_ids(ordered_ids)

    results: list[dict] = []
    for stable_id in ordered_ids:
        song_doc = found_docs.get(stable_id)
        if not isinstance(song_doc, dict):
            continue
        payload = _serialize_song_detail(song_doc, include_notes=include_notes, manifest_entry=manifest_map.get(stable_id))
        payload['id'] = stable_id
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

    try:
        categories_cursor = db.categories.find({}, {'_id': False})
        categories = list(categories_cursor)
    except Exception:
        categories = []

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
    categories = list(db.categories.find({},{'_id': False}))
    return jsonify(categories)


@app.route(basedir + 'import/report')
def route_import_report():
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
    summary = song_scanner.scan(full=full)
    summary_dict = summary if isinstance(summary, dict) else {}
    leader = summary_dict.get('leader') is True
    fast_path = summary_dict.get('fast_path') is True
    if leader and not fast_path:
        invalidate_category_cache()
    if leader:
        app.logger.info("Song scan finished: %s", summary)
    else:
        app.logger.info("Song scan skipped (no leader): %s", summary)
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

@app.route(basedir + "src/<path:ref>")
def send_src(ref):
    return cache_wrap(flask.send_from_directory("public/src", ref), 3600)

@app.route(basedir + "assets/<path:ref>")
def send_assets(ref):
    return cache_wrap(flask.send_from_directory("public/assets", ref), 3600)

@app.route(basedir + "songs/<path:ref>")
def send_songs(ref):
    return cache_wrap(flask.send_from_directory(str(SONGS_DIR_PATH), ref), 604800)

@app.route(basedir + "manifest.json")
def send_manifest():
    return cache_wrap(flask.send_from_directory("public", "manifest.json"), 3600)


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

    payload = {
        'id': song_doc.get('scanner_stable_id') or song_doc.get('id'),
        'legacy_id': song_doc.get('id'),
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
