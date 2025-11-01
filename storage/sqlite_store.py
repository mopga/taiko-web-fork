"""SQLite-backed storage implementations for desktop profile."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence, Tuple, TypeVar


LOGGER = logging.getLogger(__name__)


SCHEMA_VERSION = 4

UPSERT_CHUNK_SIZE = 500


@dataclass(frozen=True)
class SQLiteSongUpsertItem:
    """Container for providing filter context during upserts."""

    payload: Mapping[str, Any]
    filter: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class DifficultyFilter:
    """Filtering constraint for a difficulty entry."""

    name: str
    require_valid: bool = True
    min_stars: Optional[int] = None
    max_stars: Optional[int] = None


@dataclass(frozen=True)
class SongFilter:
    """Supported filtering options for catalog queries."""

    is_playable: Optional[bool] = None
    genres: Sequence[str] | None = None
    artist: Optional[str] = None
    search: Optional[str] = None
    song_ids: Sequence[str] | None = None
    difficulties: Sequence[DifficultyFilter] = field(default_factory=tuple)

    def requires_difficulties(self) -> bool:
        return bool(self.difficulties)


@dataclass(frozen=True)
class SortField:
    """Sort specification for catalog queries."""

    field: str
    ascending: bool = True


@dataclass
class Page:
    """Paginated response returned by ``SQLiteSongStore.query``."""

    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


def _serialize_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _deserialize_json(payload: Optional[str]) -> Any:
    if payload is None:
        return None
    if payload == "":
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        LOGGER.warning("Failed to decode JSON payload", exc_info=True)
        return None


def _normalize_timestamp_value(value: Any, *, unit: str) -> tuple[Optional[int], bool]:
    if value is None:
        return None, False
    if isinstance(value, bool) and not isinstance(value, int):
        return int(value), True
    if isinstance(value, int):
        return value, False
    if isinstance(value, float):
        return int(round(value)), True
    if isinstance(value, datetime):
        candidate = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        multiplier = 1000 if unit == "milliseconds" else 1
        return int(round(candidate.timestamp() * multiplier)), True
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None, True
        iso_candidate = token
        if iso_candidate.endswith("Z"):
            iso_candidate = f"{iso_candidate[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(iso_candidate)
        except ValueError:
            try:
                numeric = float(token)
            except ValueError:
                return None, True
            return int(round(numeric)), True
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        multiplier = 1000 if unit == "milliseconds" else 1
        return int(round(parsed.timestamp() * multiplier)), True
    return None, True


def _apply_update_ops(document: dict[str, Any], update: Mapping[str, Any]) -> bool:
    modified = False
    if not update:
        return modified
    if any(key.startswith("$") for key in update.keys()):
        sets = update.get("$set")
        if isinstance(sets, Mapping):
            for key, value in sets.items():
                if document.get(key) != value:
                    document[key] = value
                    modified = True
        add_to_set = update.get("$addToSet")
        if isinstance(add_to_set, Mapping):
            for key, value in add_to_set.items():
                existing = document.setdefault(key, [])
                if isinstance(existing, list) and value not in existing:
                    existing.append(value)
                    modified = True
        pulls = update.get("$pull")
        if isinstance(pulls, Mapping):
            for key, condition in pulls.items():
                original = document.get(key)
                if not isinstance(original, list):
                    continue
                if isinstance(condition, Mapping) and "$nin" in condition:
                    keep = condition.get("$nin")
                    if isinstance(keep, Sequence) and not isinstance(keep, (str, bytes)):
                        filtered = [item for item in original if item in keep]
                    else:
                        filtered = [item for item in original if item == keep]
                else:
                    filtered = [item for item in original if item != condition]
                if filtered != original:
                    document[key] = filtered
                    modified = True
        return modified
    if document != update:
        document.clear()
        document.update(update)
        modified = True
    return modified


def _normalise_title_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("_", " ").replace("-", " ")).strip()


def _clean_title_value(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalised = _normalise_title_whitespace(value)
    if not normalised:
        return None
    return normalised


def _title_from_group_key(group_key: object) -> Optional[str]:
    if not isinstance(group_key, str) or not group_key.strip():
        return None
    parts = [part.strip() for part in group_key.split(":") if part.strip()]
    if not parts:
        return None
    if parts[0] == "missing" and len(parts) >= 3:
        candidate = parts[-2]
    else:
        candidate = parts[-1]
    return _clean_title_value(candidate)


def _recover_song_title(song: Mapping[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for field in ('title', 'title_en', 'title_ru', 'title_ja', 'title_kana', 'titleJa'):
        candidate = _clean_title_value(song.get(field))
        if candidate:
            return candidate, field
    titles_mapping = song.get('titles')
    if isinstance(titles_mapping, Mapping):
        for lang in ('en', 'ru', 'ja', 'kana'):
            candidate = _clean_title_value(titles_mapping.get(lang))
            if candidate:
                return candidate, f'titles.{lang}'
    title_lang = song.get('title_lang')
    if isinstance(title_lang, Mapping):
        for lang, value in title_lang.items():
            candidate = _clean_title_value(value)
            if candidate:
                return candidate, f'title_lang.{lang}'
    locale_doc = song.get('locale')
    if isinstance(locale_doc, Mapping):
        for lang, payload in locale_doc.items():
            if isinstance(payload, Mapping):
                candidate = _clean_title_value(payload.get('title'))
                if candidate:
                    return candidate, f'locale.{lang}'
    charts = song.get('charts')
    if isinstance(charts, Sequence):
        for chart in charts:
            if not isinstance(chart, Mapping):
                continue
            candidate = _clean_title_value(chart.get('title'))
            if candidate:
                return candidate, 'charts.title'
            meta = chart.get('meta')
            if isinstance(meta, Mapping):
                candidate = _clean_title_value(meta.get('title'))
                if candidate:
                    return candidate, 'charts.meta.title'
            chart_data = chart.get('chart_data')
            if isinstance(chart_data, Mapping):
                candidate = _clean_title_value(chart_data.get('title'))
                if candidate:
                    return candidate, 'charts.chart_data.title'
                meta_payload = chart_data.get('meta')
                if isinstance(meta_payload, Mapping):
                    candidate = _clean_title_value(meta_payload.get('title'))
                    if candidate:
                        return candidate, 'charts.chart_data.meta.title'
    group_key = song.get('group_key')
    candidate = _title_from_group_key(group_key)
    if candidate:
        return candidate, 'group_key'
    return None, None


T = TypeVar("T")


class SQLiteDatabase:
    """Light-weight helper for managing the SQLite connection."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            str(self.path),
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._pragmas = self._apply_pragmas()
        self._schema_version = self._ensure_schema()
        self._log_startup()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    @property
    def schema_version(self) -> int:
        return self._schema_version

    def execute(self, sql: str, parameters: Sequence[Any] | None = None):
        params = parameters or ()
        with self._lock:
            return self._connection.execute(sql, params)

    def executemany(self, sql: str, seq_of_parameters: Iterable[Sequence[Any]]):
        with self._lock:
            return self._connection.executemany(sql, seq_of_parameters)

    def cursor(self) -> sqlite3.Cursor:
        with self._lock:
            return self._connection.cursor()

    def _apply_pragmas(self) -> dict[str, Any]:
        pragma_statements = {
            "journal_mode": "PRAGMA journal_mode = WAL",
            "synchronous": "PRAGMA synchronous = NORMAL",
            "temp_store": "PRAGMA temp_store = MEMORY",
            "foreign_keys": "PRAGMA foreign_keys = ON",
            "cache_size": "PRAGMA cache_size = -20000",
        }
        applied: dict[str, Any] = {}

        apply_cursor = self._connection.cursor()
        try:
            for statement in pragma_statements.values():
                try:
                    apply_cursor.execute(statement)
                except sqlite3.DatabaseError:
                    LOGGER.warning(
                        "Failed to apply pragma: %s", statement, exc_info=True
                    )
        finally:
            apply_cursor.close()

        read_cursor = self._connection.cursor()
        try:
            for pragma_name in pragma_statements.keys():
                try:
                    read_cursor.execute(f"PRAGMA {pragma_name}")
                    row = read_cursor.fetchone()
                except sqlite3.DatabaseError:
                    LOGGER.warning(
                        "Failed to read pragma value: %s", pragma_name, exc_info=True
                    )
                    row = None
                applied[pragma_name] = row[0] if row else None
        finally:
            read_cursor.close()

        LOGGER.info("SQLite pragmas applied path=%s settings=%s", self.path, applied)
        return applied

    def _ensure_schema(self) -> int:
        with self._connection:  # autocommit boundary
            cursor = self._connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version(
                    version INTEGER NOT NULL PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                SELECT version FROM schema_version
                ORDER BY version DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            if row is None:
                self._create_schema_v2(cursor)
                cursor.execute(
                    "INSERT INTO schema_version(version, applied_at) VALUES(?, datetime('now'))",
                    (SCHEMA_VERSION,),
                )
                version = SCHEMA_VERSION
            else:
                version = int(row[0])
            if version < 2:
                LOGGER.info(
                    "Migrating SQLite schema from v%s to v2", version
                )
                self._migrate_schema_v1_to_v2(cursor)
                cursor.execute(
                    "INSERT INTO schema_version(version, applied_at) VALUES(?, datetime('now'))",
                    (2,),
                )
                version = 2
            if version < 3:
                LOGGER.info(
                    "Migrating SQLite schema from v%s to v3", version
                )
                self._migrate_schema_v2_to_v3(cursor)
                cursor.execute(
                    "INSERT INTO schema_version(version, applied_at) VALUES(?, datetime('now'))",
                    (3,),
                )
                version = 3
            if version < SCHEMA_VERSION:
                LOGGER.info(
                    "Migrating SQLite schema from v%s to v%d", version, SCHEMA_VERSION
                )
                self._migrate_schema_v3_to_v4(cursor)
                cursor.execute(
                    "INSERT INTO schema_version(version, applied_at) VALUES(?, datetime('now'))",
                    (SCHEMA_VERSION,),
                )
                version = SCHEMA_VERSION
            cursor.close()
        return version

    def _create_schema_v2(self, cursor: sqlite3.Cursor) -> None:
        cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS songs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scanner_stable_id TEXT NOT NULL,
                group_key TEXT NOT NULL,
                song_id TEXT NOT NULL,
                title TEXT NOT NULL,
                title_reading TEXT,
                artist TEXT,
                genre TEXT,
                bpm REAL,
                duration_ms INTEGER,
                is_playable INTEGER NOT NULL,
                difficulties_json TEXT NOT NULL,
                tags_json TEXT,
                meta_json TEXT,
                tja_path TEXT,
                assets_json TEXT,
                dir_path TEXT,
                tja_filename TEXT,
                updated_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS manifest(
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_songs_scanner_group
                ON songs(scanner_stable_id, group_key);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_songs_song_id
                ON songs(song_id);
            CREATE INDEX IF NOT EXISTS idx_songs_is_playable ON songs(is_playable);
            CREATE INDEX IF NOT EXISTS idx_songs_title ON songs(title);
            CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs(artist);
            CREATE INDEX IF NOT EXISTS idx_songs_genre ON songs(genre);
            CREATE INDEX IF NOT EXISTS idx_songs_updated_at ON songs(updated_at);
            """
        )

    def _migrate_schema_v1_to_v2(self, cursor: sqlite3.Cursor) -> None:
        cursor.executescript(
            """
            ALTER TABLE songs RENAME TO songs_v1;
            CREATE TABLE songs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scanner_stable_id TEXT NOT NULL,
                group_key TEXT NOT NULL,
                song_id TEXT NOT NULL,
                title TEXT NOT NULL,
                title_reading TEXT,
                artist TEXT,
                genre TEXT,
                bpm REAL,
                duration_ms INTEGER,
                is_playable INTEGER NOT NULL,
                difficulties_json TEXT NOT NULL,
                tags_json TEXT,
                meta_json TEXT,
                tja_path TEXT,
                assets_json TEXT,
                updated_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );
            INSERT INTO songs(
                scanner_stable_id,
                group_key,
                song_id,
                title,
                title_reading,
                artist,
                genre,
                bpm,
                duration_ms,
                is_playable,
                difficulties_json,
                tags_json,
                meta_json,
                tja_path,
                assets_json,
                dir_path,
                tja_filename,
                updated_at,
                created_at
            )
            SELECT
                song_id,
                song_id,
                song_id,
                title,
                title_reading,
                artist,
                genre,
                bpm,
                duration_ms,
                is_playable,
                difficulties_json,
                tags_json,
                meta_json,
                NULL AS tja_path,
                NULL AS assets_json,
                NULL AS dir_path,
                NULL AS tja_filename,
                updated_at,
                created_at
            FROM songs_v1;
            DROP TABLE songs_v1;
            CREATE UNIQUE INDEX idx_songs_scanner_group
                ON songs(scanner_stable_id, group_key);
            CREATE UNIQUE INDEX idx_songs_song_id
                ON songs(song_id);
            CREATE INDEX idx_songs_is_playable ON songs(is_playable);
            CREATE INDEX idx_songs_title ON songs(title);
            CREATE INDEX idx_songs_artist ON songs(artist);
            CREATE INDEX idx_songs_genre ON songs(genre);
            CREATE INDEX idx_songs_updated_at ON songs(updated_at);
            """
        )

    def _migrate_schema_v2_to_v3(self, cursor: sqlite3.Cursor) -> None:
        try:
            cursor.executescript(
                """
                ALTER TABLE songs ADD COLUMN tja_path TEXT;
                ALTER TABLE songs ADD COLUMN assets_json TEXT;
                """
            )
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise

    def _migrate_schema_v3_to_v4(self, cursor: sqlite3.Cursor) -> None:
        try:
            cursor.executescript(
                """
                ALTER TABLE songs ADD COLUMN dir_path TEXT;
                ALTER TABLE songs ADD COLUMN tja_filename TEXT;
                """
            )
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise

    def _log_startup(self) -> None:
        LOGGER.info(
            "SQLite storage initialised path=%s schema_version=%s pragmas=%s",
            self.path,
            self._schema_version,
            self._pragmas,
        )

    def execute_bulk(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        with self._lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.executemany(sql, rows)
                self._connection.commit()
            except Exception:
                LOGGER.exception(
                    "SQLiteDatabase execute_bulk failed sql=%s row_count=%d",
                    sql,
                    len(rows),
                )
                self._connection.rollback()
                raise
            finally:
                cursor.close()

    def execute_in_transaction(self, callback: Callable[[sqlite3.Cursor], T]) -> T:
        with self._lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                result = callback(cursor)
                self._connection.commit()
                return result
            except Exception:
                self._connection.rollback()
                raise
            finally:
                cursor.close()


class SQLiteInsertOneResult:
    """Lightweight result object returned from ``insert_one``."""

    def __init__(self, inserted_id: Any):
        self.inserted_id = inserted_id


class SQLiteUpdateResult:
    """Lightweight result object returned from update operations."""

    def __init__(self, matched: int, modified: int, upserted_id: Any | None = None):
        self.matched_count = matched
        self.modified_count = modified
        self.upserted_id = upserted_id


class SQLiteDeleteResult:
    """Lightweight result object returned from delete operations."""

    def __init__(self, deleted: int):
        self.deleted_count = deleted


class SQLiteSongStore:
    """SQLite-backed implementation of the songs data access layer."""

    def __init__(self, database: SQLiteDatabase):
        self._db = database
        self._title_recovered_callback: Callable[[str, Optional[str]], None] | None = None
        self._normalization_warned_fields: set[str] = set()
        self._normalization_failure_warned_fields: set[str] = set()
        self._key_mismatch_logged = False

    def set_title_recovered_callback(
        self, callback: Callable[[str, Optional[str]], None] | None
    ) -> None:
        self._title_recovered_callback = callback

    @property
    def path(self) -> Path:
        return self._db.path

    def upsert_many(
        self, songs: Iterable[Mapping[str, Any] | SQLiteSongUpsertItem]
    ) -> list[int]:
        prepared_rows: list[tuple[Any, ...]] = []
        for entry in songs:
            if isinstance(entry, SQLiteSongUpsertItem):
                payload = entry.payload
                filter_doc = entry.filter
            elif isinstance(entry, Mapping):
                payload = entry
                filter_doc = None
            else:
                raise TypeError(
                    f"Unsupported upsert entry type: {type(entry).__name__}"
                )
            prepared_rows.append(self._prepare_song_row(payload, filter_doc))
        if not prepared_rows:
            return []

        total_rows = len(prepared_rows)
        start = time.perf_counter()
        LOGGER.info(
            "SQLiteSongStore upsert_many start rows=%d chunk_size=%d",
            total_rows,
            UPSERT_CHUNK_SIZE,
        )
        query = (
            """
            INSERT INTO songs(
                scanner_stable_id, group_key, song_id, title, title_reading, artist, genre, bpm, duration_ms,
                is_playable, difficulties_json, tags_json, meta_json, tja_path, assets_json, dir_path, tja_filename,
                updated_at, created_at
            ) VALUES(
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(scanner_stable_id, group_key) DO UPDATE SET
                song_id=excluded.song_id,
                title=excluded.title,
                title_reading=excluded.title_reading,
                artist=excluded.artist,
                genre=excluded.genre,
                bpm=excluded.bpm,
                duration_ms=excluded.duration_ms,
                is_playable=excluded.is_playable,
                difficulties_json=excluded.difficulties_json,
                tags_json=excluded.tags_json,
                meta_json=excluded.meta_json,
                tja_path=excluded.tja_path,
                assets_json=excluded.assets_json,
                dir_path=excluded.dir_path,
                tja_filename=excluded.tja_filename,
                updated_at=excluded.updated_at,
                created_at=excluded.created_at
            RETURNING id
            """
        )

        processed = 0

        def _execute_chunks(cursor: sqlite3.Cursor) -> list[int]:
            nonlocal processed
            identifiers: list[int] = []
            for chunk_index, chunk_start in enumerate(
                range(0, total_rows, UPSERT_CHUNK_SIZE), start=1
            ):
                chunk_rows = prepared_rows[chunk_start : chunk_start + UPSERT_CHUNK_SIZE]
                chunk_begin = time.perf_counter()
                chunk_identifiers: list[int] = []
                for params in chunk_rows:
                    cursor.execute(query, params)
                    returning_row = cursor.fetchone()
                    identifier = self._resolve_upsert_identifier(cursor, returning_row, params)
                    chunk_identifiers.append(identifier)
                identifiers.extend(chunk_identifiers)
                processed += len(chunk_rows)
                chunk_duration_ms = (time.perf_counter() - chunk_begin) * 1000
                LOGGER.info(
                    "SQLiteSongStore upsert_many chunk=%d rows=%d duration_ms=%.2f processed=%d/%d",
                    chunk_index,
                    len(chunk_rows),
                    chunk_duration_ms,
                    processed,
                    total_rows,
                )
            return identifiers

        try:
            returned_ids = self._db.execute_in_transaction(_execute_chunks)
        except Exception:
            LOGGER.exception("SQLiteSongStore upsert_many failed")
            raise

        duration_ms = (time.perf_counter() - start) * 1000
        LOGGER.info(
            "SQLiteSongStore upsert_many complete rows=%d duration_ms=%.2f",
            total_rows,
            duration_ms,
        )
        return returned_ids

    def _resolve_upsert_identifier(
        self,
        cursor: sqlite3.Cursor,
        returning_row: Optional[Sequence[Any]],
        params: Sequence[Any],
    ) -> int:
        if returning_row:
            identifier = returning_row[0]
            if identifier is not None:
                return int(identifier)
        stable_id, group_key = params[0], params[1]
        cursor.execute(
            "SELECT id FROM songs WHERE scanner_stable_id = ? AND group_key = ? LIMIT 1",
            (stable_id, group_key),
        )
        fallback = cursor.fetchone()
        if fallback and fallback[0] is not None:
            return int(fallback[0])
        raise RuntimeError(
            "Failed to resolve song identifier after upsert for scanner_stable_id=%s group_key=%s"
            % (stable_id, group_key)
        )

    # ------------------------------------------------------------------
    # Compatibility helpers for the ``SongStore`` protocol
    # ------------------------------------------------------------------

    def find(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *,
        sort: Optional[Sequence[tuple[str, int]]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        **_: Any,
    ) -> Iterable[Mapping[str, Any]]:
        """Retrieve documents matching the supplied filter."""

        documents = list(
            self._query_documents(
                filter=filter,
                sort_spec=sort,
                limit=limit if limit is not None else -1,
                offset=skip or 0,
            )
        )
        if projection:
            documents = [self._apply_projection(doc, projection) for doc in documents]
        return documents

    def find_one(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        *,
        sort: Optional[Sequence[tuple[str, int]]] = None,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        documents = list(
            self._query_documents(
                filter=filter,
                sort_spec=sort,
                limit=1,
                offset=0,
            )
        )
        if not documents:
            return None
        document = documents[0]
        if projection:
            document = self._apply_projection(document, projection)
        return document

    def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
        **kwargs: Any,
    ) -> Optional[Mapping[str, Any]]:
        original = self.find_one(filter, **kwargs)
        self.update_one(filter, update, upsert=upsert)
        return original

    def insert_one(
        self,
        document: Mapping[str, Any],
        *args: Any,
        **kwargs: Any,
    ) -> SQLiteInsertOneResult:
        identifiers = self.upsert_many([document])
        inserted_id = identifiers[0] if identifiers else None
        return SQLiteInsertOneResult(inserted_id)

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
        **kwargs: Any,
    ) -> SQLiteUpdateResult:
        existing = self.find_one(filter)
        if existing is None:
            if not upsert:
                return SQLiteUpdateResult(0, 0)
            base: dict[str, Any] = {}
        else:
            base = dict(existing)

        modified = _apply_update_ops(base, update)
        if modified:
            self.upsert_many([SQLiteSongUpsertItem(base, filter)])
        elif existing is None and upsert:
            self.upsert_many([SQLiteSongUpsertItem(base, filter)])
        matched = 1 if existing is not None else 0
        upserted_id = None if existing is not None else base.get("song_id")
        return SQLiteUpdateResult(matched, 1 if modified else 0, upserted_id)

    def replace_one(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        *,
        upsert: bool = False,
        **kwargs: Any,
    ) -> SQLiteUpdateResult:
        existing = self.find_one(filter)
        if existing is None and not upsert:
            return SQLiteUpdateResult(0, 0)
        self.upsert_many([SQLiteSongUpsertItem(replacement, filter)])
        matched = 1 if existing is not None else 0
        upserted_id = None if existing is not None else replacement.get("song_id")
        return SQLiteUpdateResult(matched, 1, upserted_id)

    def update_many(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        **kwargs: Any,
    ) -> SQLiteUpdateResult:
        documents = list(self._query_documents(filter=filter))
        if not documents:
            return SQLiteUpdateResult(0, 0)
        modified = 0
        for document in documents:
            payload = dict(document)
            if _apply_update_ops(payload, update):
                stable_filter = {
                    "scanner_stable_id": document.get("scanner_stable_id"),
                    "group_key": document.get("group_key"),
                }
                self.upsert_many([SQLiteSongUpsertItem(payload, stable_filter)])
                modified += 1
        return SQLiteUpdateResult(len(documents), modified)

    def bulk_write(
        self,
        operations: Sequence[Any],
        **_: Any,
    ) -> None:
        for operation in operations:
            handler = getattr(operation, "_sqlite_apply", None)
            if callable(handler):
                handler(self)
                continue
            if hasattr(operation, "_filter") and hasattr(operation, "_doc"):
                self.replace_one(operation._filter, operation._doc, upsert=True)
                continue
            LOGGER.warning("Unsupported bulk_write operation: %s", type(operation).__name__)

    def delete_one(
        self,
        filter: Mapping[str, Any],
        **_: Any,
    ) -> SQLiteDeleteResult:
        document = self.find_one(filter)
        if not document:
            return SQLiteDeleteResult(0)
        identifier = document.get("song_id")
        if identifier is None:
            return SQLiteDeleteResult(0)
        with self._db.connection:
            self._db.execute("DELETE FROM songs WHERE song_id = ?", (identifier,))
        return SQLiteDeleteResult(1)

    def delete_many(
        self,
        filter: Mapping[str, Any],
        **_: Any,
    ) -> SQLiteDeleteResult:
        documents = list(self._query_documents(filter=filter))
        if not documents:
            return SQLiteDeleteResult(0)
        identifiers = [doc.get("song_id") for doc in documents if doc.get("song_id")]
        if not identifiers:
            return SQLiteDeleteResult(0)
        placeholders = ",".join(["?"] * len(identifiers))
        query = f"DELETE FROM songs WHERE song_id IN ({placeholders})"
        with self._db.connection:
            self._db.execute(query, identifiers)
        return SQLiteDeleteResult(len(identifiers))

    def count_documents(
        self,
        filter: Mapping[str, Any],
        **_: Any,
    ) -> int:
        song_filter = self._convert_filter_to_song_filter(filter)
        if song_filter is not None:
            return self.count(song_filter)
        return len(list(self._query_documents(filter=filter)))

    def create_index(self, *args: Any, **kwargs: Any) -> None:
        LOGGER.debug("SQLiteSongStore.create_index noop args=%s kwargs=%s", args, kwargs)

    def drop_index(self, *args: Any, **kwargs: Any) -> None:
        LOGGER.debug("SQLiteSongStore.drop_index noop args=%s kwargs=%s", args, kwargs)

    def list_indexes(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        return []

    def get_by_id(self, song_id: str) -> Optional[dict[str, Any]]:
        cursor = self._db.execute(
            "SELECT * FROM songs WHERE song_id = ?", (song_id,)
        )
        row = cursor.fetchone()
        return self._row_to_song(row) if row else None

    def find_by_id(self, song_id: str) -> Optional[Mapping[str, Any]]:
        """Retrieve a song document by identifier, mirroring Mongo semantics."""

        document = self.get_by_id(song_id)
        return dict(document) if document is not None else None

    def query(
        self,
        filter: SongFilter | None,
        sort: Sequence[SortField] | None,
        limit: int,
        offset: int,
    ) -> Page:
        song_filter = filter or SongFilter()
        order_clause = self._build_order_clause(sort)
        where_clause, parameters = self._build_where_clause(song_filter)
        if song_filter.requires_difficulties():
            cursor = self._db.execute(
                f"SELECT * FROM songs {where_clause} {order_clause}", parameters
            )
            rows = cursor.fetchall()
            filtered = [
                row for row in rows if self._matches_difficulties(row, song_filter.difficulties)
            ]
            total = len(filtered)
            sliced = self._slice_rows(filtered, limit, offset)
        else:
            total = self._count_via_sql(where_clause, parameters)
            query = f"SELECT * FROM songs {where_clause} {order_clause}"
            params = list(parameters)
            if limit >= 0:
                query += " LIMIT ?"
                params.append(limit)
            if offset > 0:
                query += " OFFSET ?"
                params.append(offset)
            cursor = self._db.execute(query, params)
            sliced = cursor.fetchall()
        items = [self._row_to_song(row) for row in sliced]
        return Page(items=items, total=total, limit=limit, offset=offset)

    def count(self, filter: SongFilter | None) -> int:
        song_filter = filter or SongFilter()
        where_clause, parameters = self._build_where_clause(song_filter)
        if song_filter.requires_difficulties():
            cursor = self._db.execute(
                f"SELECT song_id, difficulties_json FROM songs {where_clause}",
                parameters,
            )
            rows = cursor.fetchall()
            return sum(
                1
                for row in rows
                if self._matches_difficulties(row, song_filter.difficulties)
            )
        return self._count_via_sql(where_clause, parameters)

    def delete_obsolete(self, ids: set[str]) -> int:
        if not ids:
            with self._db.connection:
                cursor = self._db.execute("DELETE FROM songs")
                return cursor.rowcount
        placeholders = ",".join(["?"] * len(ids))
        query = f"DELETE FROM songs WHERE song_id NOT IN ({placeholders})"
        with self._db.connection:
            cursor = self._db.execute(query, tuple(ids))
            return cursor.rowcount

    def stats(self) -> dict[str, Any]:
        stats: dict[str, Any] = {}
        cursor = self._db.execute("SELECT COUNT(*) FROM songs")
        stats["total_songs"] = cursor.fetchone()[0]
        cursor = self._db.execute("SELECT COUNT(*) FROM songs WHERE is_playable = 1")
        stats["playable_songs"] = cursor.fetchone()[0]
        cursor = self._db.execute("SELECT MAX(updated_at) FROM songs")
        stats["latest_update"] = cursor.fetchone()[0]
        cursor = self._db.execute(
            "SELECT genre, COUNT(*) AS c FROM songs GROUP BY genre ORDER BY genre"
        )
        stats["by_genre"] = {
            (row[0] if row[0] is not None else ""): row[1] for row in cursor.fetchall()
        }
        return stats

    def _prepare_song_row(
        self, song: Mapping[str, Any], filter_doc: Mapping[str, Any] | None
    ) -> tuple[Any, ...]:
        filter_scanner = self._extract_filter_value(filter_doc, "scanner_stable_id")
        if filter_scanner is None:
            filter_scanner = self._extract_filter_value(filter_doc, "_id")
        payload_scanner = self._normalize_string(song.get("scanner_stable_id"))
        stable_id = filter_scanner or payload_scanner
        if not isinstance(stable_id, str) or not stable_id:
            raise ValueError("song payload missing scanner_stable_id")

        filter_group = self._extract_filter_value(filter_doc, "group_key")
        payload_group = self._normalize_string(song.get("group_key"))
        group_key = filter_group
        if not group_key and filter_scanner:
            group_key = self._lookup_existing_group_key(filter_scanner)
        if not group_key:
            group_key = payload_group
        if not group_key and stable_id:
            group_key = f"group::{stable_id}"
        if not isinstance(group_key, str) or not group_key:
            raise ValueError("song payload missing group_key")

        self._maybe_log_key_mismatch(
            filter_scanner, payload_scanner, filter_group, payload_group
        )

        raw_song_id = self._normalize_identifier(song.get("song_id"))
        if raw_song_id:
            song_id = raw_song_id
        else:
            raw_payload_id = self._normalize_identifier(song.get("id"))
            if raw_payload_id:
                song_id = raw_payload_id
            else:
                song_id = stable_id
        title = song.get("title")
        if not isinstance(title, str) or not title.strip():
            recovered_title, source = _recover_song_title(song)
            if recovered_title:
                log_level = logging.DEBUG if source == 'title_lang.ja' else logging.INFO
                LOGGER.log(
                    log_level,
                    "Recovered missing song title for scanner_stable_id=%s via %s",
                    stable_id,
                    source or "fallback",
                )
                if self._title_recovered_callback is not None:
                    try:
                        self._title_recovered_callback(stable_id, source)
                    except Exception:  # pragma: no cover - callbacks must not break storage
                        LOGGER.debug(
                            "Title recovery callback failed for scanner_stable_id=%s",
                            stable_id,
                            exc_info=True,
                        )
                title = recovered_title
            else:
                raise ValueError("song payload missing title")
        title_reading = song.get("title_reading")
        artist = song.get("artist")
        genre = song.get("genre")
        bpm = song.get("bpm")
        duration_ms = song.get("duration_ms")
        is_playable = 1 if song.get("is_playable", True) else 0
        difficulties = song.get("difficulties") or {}
        tags = song.get("tags")
        meta = song.get("meta")
        tja_path_value = self._normalize_asset_value(song.get("tja_path"))
        raw_assets = song.get("assets")
        sanitized_assets = self._sanitize_assets_payload(raw_assets) if isinstance(raw_assets, Mapping) else None
        if not tja_path_value and sanitized_assets:
            candidate = sanitized_assets.get("tja_main")
            if isinstance(candidate, str):
                tja_path_value = self._normalize_asset_value(candidate)
        dir_path_value = self._normalize_asset_value(song.get("dir_path"))
        tja_filename_value = self._normalize_asset_value(song.get("tja_filename"))
        updated_at = self._coerce_timestamp_field("updated_at", song.get("updated_at"), "milliseconds", 0)
        created_at = self._coerce_timestamp_field(
            "created_at", song.get("created_at"), "milliseconds", updated_at
        )
        return (
            stable_id,
            group_key,
            song_id,
            title,
            title_reading,
            artist,
            genre,
            bpm,
            duration_ms,
            is_playable,
            _serialize_json(difficulties) or "{}",
            _serialize_json(tags),
            _serialize_json(meta),
            tja_path_value,
            _serialize_json(sanitized_assets) if sanitized_assets else None,
            dir_path_value,
            tja_filename_value,
            updated_at,
            created_at,
        )

    def _coerce_timestamp_field(
        self, field: str, value: Any, unit: str, fallback: int
    ) -> int:
        if value is None:
            return fallback
        normalized, converted = _normalize_timestamp_value(value, unit=unit)
        if normalized is None:
            self._log_normalization_failure(field, value)
            return fallback
        if converted:
            self._log_normalization_event(field, value)
        return normalized

    def _log_normalization_event(self, field: str, value: Any) -> None:
        if field in self._normalization_warned_fields:
            return
        self._normalization_warned_fields.add(field)
        LOGGER.warning(
            "Normalizing song field %s from %s", field, type(value).__name__
        )

    def _log_normalization_failure(self, field: str, value: Any) -> None:
        if field in self._normalization_failure_warned_fields:
            return
        self._normalization_failure_warned_fields.add(field)
        LOGGER.warning(
            "Unable to normalise song field %s type=%s; using fallback", field, type(value).__name__
        )

    def _normalize_string(self, value: Any) -> Optional[str]:
        if isinstance(value, str):
            token = value.strip()
            return token or None
        return None

    def _normalize_identifier(self, value: Any) -> Optional[str]:
        normalized = self._normalize_string(value)
        if normalized is not None:
            return normalized
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            token = str(value).strip()
            return token or None
        return None

    def _normalize_asset_value(self, value: Any) -> Optional[str]:
        if isinstance(value, str):
            token = value.strip()
            return token or None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            token = str(value).strip()
            return token or None
        return None

    def _sanitize_assets_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        sanitized: dict[str, Any] = {}
        for raw_key, raw_value in payload.items():
            key_token = self._normalize_string(raw_key)
            if key_token is None:
                key_token = self._normalize_asset_value(raw_key)
            if not key_token:
                continue
            if isinstance(raw_value, Mapping):
                nested: dict[str, str] = {}
                for nested_key, nested_value in raw_value.items():
                    nested_key_token = self._normalize_string(nested_key)
                    if nested_key_token is None:
                        nested_key_token = self._normalize_asset_value(nested_key)
                    if not nested_key_token:
                        continue
                    nested_value_token = self._normalize_asset_value(nested_value)
                    if not nested_value_token:
                        continue
                    nested[nested_key_token] = nested_value_token
                if nested:
                    sanitized[key_token] = nested
            else:
                value_token = self._normalize_asset_value(raw_value)
                if value_token:
                    sanitized[key_token] = value_token
        return sanitized

    def _extract_filter_value(
        self, filter_doc: Mapping[str, Any] | None, key: str
    ) -> Optional[str]:
        if not isinstance(filter_doc, Mapping):
            return None
        candidate = filter_doc.get(key)
        if isinstance(candidate, Mapping):
            if "$eq" in candidate:
                return self._normalize_string(candidate.get("$eq"))
            return None
        return self._normalize_string(candidate)

    def _lookup_existing_group_key(self, scanner_stable_id: str) -> Optional[str]:
        try:
            cursor = self._db.execute(
                "SELECT group_key FROM songs WHERE scanner_stable_id = ? LIMIT 1",
                (scanner_stable_id,),
            )
        except Exception:
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug(
                    "SQLiteSongStore lookup for existing group_key failed scanner_stable_id=%s",
                    scanner_stable_id,
                    exc_info=True,
                )
            return None
        row = cursor.fetchone()
        if not row:
            return None
        if isinstance(row, sqlite3.Row):
            try:
                value = row["group_key"]
            except (KeyError, IndexError):
                try:
                    value = row[0]
                except (IndexError, TypeError):
                    value = None
        else:
            try:
                value = row[0]
            except (IndexError, TypeError):
                value = None
        return self._normalize_string(value)

    def _maybe_log_key_mismatch(
        self,
        filter_scanner: Optional[str],
        payload_scanner: Optional[str],
        filter_group: Optional[str],
        payload_group: Optional[str],
    ) -> None:
        if self._key_mismatch_logged:
            return
        mismatch = False
        if filter_scanner and payload_scanner and filter_scanner != payload_scanner:
            mismatch = True
        if filter_group and payload_group and filter_group != payload_group:
            mismatch = True
        if mismatch:
            self._key_mismatch_logged = True
            LOGGER.error(
                "SQLiteSongStore ignoring payload key mismatch: filter(scanner_stable_id=%s, group_key=%s) payload(scanner_stable_id=%s, group_key=%s)",
                filter_scanner,
                filter_group,
                payload_scanner,
                payload_group,
            )

    def _row_to_song(self, row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(row)
        payload["is_playable"] = bool(payload.get("is_playable"))
        payload["difficulties"] = _deserialize_json(payload.pop("difficulties_json", None)) or {}
        payload["tags"] = _deserialize_json(payload.pop("tags_json", None))
        payload["meta"] = _deserialize_json(payload.pop("meta_json", None))
        assets_payload = _deserialize_json(payload.pop("assets_json", None)) or {}
        if isinstance(assets_payload, Mapping):
            payload["assets"] = dict(assets_payload)
        elif assets_payload:
            payload["assets"] = assets_payload
        if "song_id" in payload and "id" not in payload:
            payload["id"] = payload["song_id"]
        return payload

    def _apply_projection(
        self, document: Mapping[str, Any], projection: Mapping[str, Any]
    ) -> dict[str, Any]:
        include_keys = {
            key for key, value in projection.items() if value and not key.startswith("-")
        }
        if not include_keys:
            exclude_keys = {
                key for key, value in projection.items() if not value and not key.startswith("-")
            }
            if not exclude_keys:
                return dict(document)
            return {k: v for k, v in document.items() if k not in exclude_keys}
        return {k: v for k, v in document.items() if k in include_keys}

    def _query_documents(
        self,
        *,
        filter: Optional[Mapping[str, Any]] = None,
        sort_spec: Optional[Sequence[tuple[str, int]]] = None,
        limit: int = -1,
        offset: int = 0,
    ) -> Iterator[dict[str, Any]]:
        song_filter = self._convert_filter_to_song_filter(filter)
        if song_filter is None:
            where_clause, parameters = "", []
        else:
            where_clause, parameters = self._build_where_clause(song_filter)
        sort_fields = None
        if sort_spec:
            sort_fields = [SortField(field=name, ascending=direction >= 0) for name, direction in sort_spec]
        order_clause = self._build_order_clause(sort_fields)
        query = f"SELECT * FROM songs {where_clause} {order_clause}"
        params: list[Any] = list(parameters)
        if limit is not None and limit >= 0:
            query += " LIMIT ?"
            params.append(limit)
        if offset:
            query += " OFFSET ?"
            params.append(offset)
        cursor = self._db.execute(query, params)
        rows = cursor.fetchall()
        documents = [self._row_to_song(row) for row in rows]
        if song_filter is None and filter:
            documents = [doc for doc in documents if self._match_document(filter, doc)]
        return iter(documents)

    def _convert_filter_to_song_filter(
        self, filter: Optional[Mapping[str, Any]]
    ) -> Optional[SongFilter]:
        if not filter:
            return SongFilter()
        kwargs: dict[str, Any] = {}
        song_ids: list[str] = []
        for key, value in filter.items():
            if key in {"song_id", "id"}:
                if isinstance(value, Mapping):
                    ids = self._extract_in_values(value)
                    if ids is None:
                        return None
                    song_ids.extend(ids)
                elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                    song_ids.extend(str(item) for item in value)
                else:
                    song_ids.append(str(value))
            elif key == "is_playable":
                if isinstance(value, Mapping):
                    allowed = value.get("$eq")
                    if isinstance(allowed, bool):
                        kwargs["is_playable"] = allowed
                        continue
                    return None
                if isinstance(value, bool):
                    kwargs["is_playable"] = value
                else:
                    return None
            elif key in {"genre", "genres"}:
                if isinstance(value, Mapping):
                    items = self._extract_in_values(value)
                    if items is None:
                        return None
                    kwargs["genres"] = tuple(str(item) for item in items)
                else:
                    kwargs["genres"] = (str(value),)
            elif key == "artist":
                if isinstance(value, str):
                    kwargs["artist"] = value
                else:
                    return None
            elif key == "search":
                if isinstance(value, str):
                    kwargs["search"] = value
                else:
                    return None
            else:
                return None
        if song_ids:
            kwargs["song_ids"] = tuple(song_ids)
        return SongFilter(**kwargs)

    def _extract_in_values(self, payload: Mapping[str, Any]) -> Optional[list[str]]:
        if "$in" in payload:
            values = payload.get("$in")
            if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
                return [str(item) for item in values]
        elif "$eq" in payload:
            value = payload.get("$eq")
            if value is not None:
                return [str(value)]
        return None

    def _match_document(
        self, filter: Mapping[str, Any], document: Mapping[str, Any]
    ) -> bool:
        for key, expected in filter.items():
            if key == "id":
                value = document.get("song_id")
            else:
                value = document.get(key)
            if isinstance(expected, Mapping):
                if "$in" in expected:
                    options = expected.get("$in")
                    if not isinstance(options, Sequence) or isinstance(options, (str, bytes)):
                        return False
                    if value not in options:
                        return False
                elif "$ne" in expected:
                    if value == expected.get("$ne"):
                        return False
                elif "$exists" in expected:
                    exists = bool(expected.get("$exists"))
                    if exists != (key in document and document[key] is not None):
                        return False
                else:
                    return False
            else:
                if value != expected:
                    return False
        return True

    def _build_where_clause(self, filter: SongFilter) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if filter.is_playable is not None:
            clauses.append("is_playable = ?")
            params.append(1 if filter.is_playable else 0)
        if filter.genres:
            genres = [genre for genre in filter.genres if genre is not None]
            if genres:
                placeholders = ",".join(["?"] * len(genres))
                clauses.append(f"genre IN ({placeholders})")
                params.extend(genres)
        if filter.artist:
            clauses.append("UPPER(artist) = ?")
            params.append(filter.artist.upper())
        if filter.song_ids:
            placeholders = ",".join(["?"] * len(filter.song_ids))
            clauses.append(f"song_id IN ({placeholders})")
            params.extend(filter.song_ids)
        if filter.search:
            token = f"%{filter.search.upper()}%"
            clauses.append("(UPPER(title) LIKE ? OR UPPER(artist) LIKE ?)")
            params.extend([token, token])
        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def _build_order_clause(self, sort: Sequence[SortField] | None) -> str:
        allowed = {
            "title": "title",
            "artist": "artist",
            "genre": "genre",
            "updated_at": "updated_at",
            "created_at": "created_at",
            "song_id": "song_id",
        }
        order_tokens: list[str] = []
        if sort:
            for item in sort:
                column = allowed.get(item.field)
                if column is None:
                    continue
                direction = "ASC" if item.ascending else "DESC"
                order_tokens.append(f"{column} {direction}")
        if not order_tokens:
            order_tokens.append("title ASC")
        order_tokens.append("song_id ASC")
        return "ORDER BY " + ", ".join(order_tokens)

    def _count_via_sql(self, where_clause: str, parameters: Sequence[Any]) -> int:
        cursor = self._db.execute(
            f"SELECT COUNT(*) FROM songs {where_clause}", parameters
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    def _matches_difficulties(
        self, row: sqlite3.Row | Mapping[str, Any], filters: Sequence[DifficultyFilter]
    ) -> bool:
        if isinstance(row, Mapping):
            payload = row.get("difficulties_json")
        else:
            payload = row["difficulties_json"]
        data = _deserialize_json(payload) or {}
        if not isinstance(data, Mapping):
            return False
        for constraint in filters:
            entry = data.get(constraint.name)
            if not isinstance(entry, Mapping):
                return False
            if constraint.require_valid and not bool(entry.get("valid", True)):
                return False
            stars = entry.get("stars")
            if constraint.min_stars is not None:
                if not isinstance(stars, (int, float)) or stars < constraint.min_stars:
                    return False
            if constraint.max_stars is not None:
                if not isinstance(stars, (int, float)) or stars > constraint.max_stars:
                    return False
        return True

    def _slice_rows(
        self, rows: Sequence[sqlite3.Row], limit: int, offset: int
    ) -> list[sqlite3.Row]:
        start = offset if offset >= 0 else 0
        if limit < 0:
            return list(rows[start:])
        end = start + limit
        return list(rows[start:end])


class SQLiteManifestStore:
    """SQLite-backed implementation of manifest key/value storage."""

    def __init__(self, database: SQLiteDatabase):
        self._db = database
        self._update_one_info_logged = False

    # Mongo compatibility surface -------------------------------------------------

    def find(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        **_: Any,
    ) -> Iterable[Mapping[str, Any]]:
        documents = [
            self._row_to_manifest(row) for row in self._db.execute("SELECT * FROM manifest")
        ]
        if filter:
            documents = [doc for doc in documents if self._match_manifest(filter, doc)]
        if projection:
            documents = [self._apply_manifest_projection(doc, projection) for doc in documents]
        return documents

    def find_one(
        self,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Mapping[str, Any]] = None,
        **_: Any,
    ) -> Optional[Mapping[str, Any]]:
        matches = list(self.find(filter, projection))
        return matches[0] if matches else None

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
        **_: Any,
    ) -> SQLiteUpdateResult:
        document = self.find_one(filter)
        if document is None:
            if not upsert:
                return SQLiteUpdateResult(0, 0)
            if "$set" in update and isinstance(update["$set"], Mapping):
                base = dict(update["$set"])
            else:
                base = dict(update)
            identifier = self._manifest_id_from_filter(filter)
            if identifier is None:
                raise ValueError("Manifest upsert requires _id in filter")
            self.put(identifier, base)
            return SQLiteUpdateResult(0, 1, identifier)
        payload = dict(document)
        modified = _apply_update_ops(payload, update)
        if modified:
            identifier = payload.get("_id")
            if identifier is None:
                raise ValueError("Manifest document missing _id")
            payload_copy = dict(payload)
            payload_copy.pop("_id", None)
            payload_copy.pop("updated_at", None)
            self.put(identifier, payload_copy)
        return SQLiteUpdateResult(1, 1 if modified else 0)

    def delete_many(self, filter: Mapping[str, Any], **_: Any) -> SQLiteDeleteResult:
        documents = list(self.find(filter))
        if not documents:
            return SQLiteDeleteResult(0)
        identifiers = [doc.get("_id") for doc in documents if isinstance(doc.get("_id"), str)]
        if not identifiers:
            return SQLiteDeleteResult(0)
        placeholders = ",".join(["?"] * len(identifiers))
        query = f"DELETE FROM manifest WHERE key IN ({placeholders})"
        with self._db.connection:
            self._db.execute(query, identifiers)
        return SQLiteDeleteResult(len(identifiers))

    def bulk_write(self, operations: Sequence[Any], **_: Any) -> None:
        for operation in operations:
            handler = getattr(operation, "_sqlite_apply", None)
            if callable(handler):
                handler(self)
                continue
            if self._apply_manifest_update_one(operation):
                if not self._update_one_info_logged:
                    LOGGER.info('SQLiteManifestStore enabling UpdateOne bulk compatibility')
                    self._update_one_info_logged = True
                continue
            LOGGER.debug(
                "Unsupported manifest bulk operation: %s", type(operation).__name__
            )

    def create_index(self, *args: Any, **kwargs: Any) -> None:
        LOGGER.debug("SQLiteManifestStore.create_index noop args=%s kwargs=%s", args, kwargs)

    def list_indexes(self, *args: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        return []

    def get(self, key: str) -> Optional[dict[str, Any]]:
        cursor = self._db.execute(
            "SELECT value_json FROM manifest WHERE key = ?", (key,)
        )
        row = cursor.fetchone()
        if not row:
            return None
        value = _deserialize_json(row[0])
        return value if isinstance(value, dict) else None

    def put(self, key: str, value: Mapping[str, Any]) -> None:
        payload = _serialize_json(dict(value)) or "{}"
        updated_at = int(time.time() * 1000)
        query = (
            """
            INSERT INTO manifest(key, value_json, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """
        )
        with self._db.connection:
            self._db.execute(query, (key, payload, updated_at))

    def _row_to_manifest(self, row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
        key = row["key"] if isinstance(row, sqlite3.Row) else row.get("key")
        value = _deserialize_json(row["value_json"] if isinstance(row, sqlite3.Row) else row.get("value_json"))
        document: dict[str, Any] = {"_id": key}
        if isinstance(value, Mapping):
            document.update(value)
        else:
            document["value"] = value
        document["updated_at"] = row["updated_at"] if isinstance(row, sqlite3.Row) else row.get("updated_at")
        return document

    def _apply_manifest_projection(
        self, document: Mapping[str, Any], projection: Mapping[str, Any]
    ) -> dict[str, Any]:
        include = {key for key, value in projection.items() if value}
        if include:
            return {key: document[key] for key in include if key in document}
        exclude = {key for key, value in projection.items() if not value}
        if not exclude:
            return dict(document)
        return {key: value for key, value in document.items() if key not in exclude}

    def _match_manifest(
        self, filter: Mapping[str, Any], document: Mapping[str, Any]
    ) -> bool:
        for key, expected in filter.items():
            value = document.get(key)
            if isinstance(expected, Mapping):
                if "$in" in expected:
                    candidates = expected.get("$in")
                    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
                        return False
                    if value not in candidates:
                        return False
                elif "$ne" in expected:
                    if value == expected.get("$ne"):
                        return False
                else:
                    return False
            else:
                if value != expected:
                    return False
        return True

    def _apply_manifest_update_one(self, operation: Any) -> bool:
        filter_doc = getattr(operation, 'filter', None)
        if filter_doc is None:
            filter_doc = getattr(operation, '_filter', None)
        update_doc = getattr(operation, 'update', None)
        if update_doc is None:
            update_doc = getattr(operation, '_doc', None)
        upsert_flag = getattr(operation, 'upsert', None)
        if upsert_flag is None:
            upsert_flag = getattr(operation, '_upsert', None)
        if upsert_flag is None and isinstance(getattr(operation, 'kwargs', None), Mapping):
            upsert_flag = operation.kwargs.get('upsert')  # type: ignore[union-attr]
        if upsert_flag is None and isinstance(getattr(operation, 'args', None), Sequence):
            args = getattr(operation, 'args')
            if len(args) >= 3:
                upsert_flag = args[2]
        upsert = bool(upsert_flag)

        if isinstance(filter_doc, Sequence) and not isinstance(filter_doc, Mapping):
            try:
                filter_doc = filter_doc[0]
            except Exception:
                filter_doc = None
        if not isinstance(filter_doc, Mapping):
            return False

        if isinstance(update_doc, Sequence) and not isinstance(update_doc, Mapping):
            try:
                update_doc = update_doc[0]
            except Exception:
                update_doc = None
        if not isinstance(update_doc, Mapping):
            return False

        set_payload = update_doc.get('$set') if isinstance(update_doc.get('$set'), Mapping) else None
        if set_payload is None:
            return False

        identifier = self._manifest_id_from_filter(filter_doc)
        if not identifier:
            LOGGER.error('Manifest UpdateOne missing _id filter=%s', filter_doc)
            return False

        existing = self.get(identifier)
        if existing is None and not upsert:
            return True

        base: dict[str, Any] = {}
        if isinstance(existing, Mapping):
            base.update(existing)
        for key, value in set_payload.items():
            base[key] = value

        stored_value = dict(base)
        stored_value.pop('_id', None)

        updated_at_value = set_payload.get('updated_at')
        updated_at = None
        if isinstance(updated_at_value, (int, float)) and not isinstance(updated_at_value, bool):
            try:
                updated_at = int(updated_at_value)
            except (TypeError, ValueError):
                updated_at = None
        if updated_at is None:
            updated_at = int(time.time() * 1000)

        payload_json = _serialize_json(stored_value) or "{}"
        query = (
            """
            INSERT INTO manifest(key, value_json, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """
        )

        with self._db.connection:
            self._db.execute(query, (identifier, payload_json, updated_at))

        return True

    def _manifest_id_from_filter(self, filter: Mapping[str, Any]) -> Optional[str]:
        def _normalise(value: Any) -> Optional[str]:
            if isinstance(value, str):
                token = value.strip()
                return token or None
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    token = str(int(value)).strip()
                except (TypeError, ValueError):
                    token = str(value).strip()
                return token or None
            return None

        for key in ("_id", "id"):
            candidate = filter.get(key)
            if isinstance(candidate, Mapping) and "$eq" in candidate:
                candidate = candidate.get("$eq")
            identifier = _normalise(candidate)
            if identifier:
                return identifier
        return None


class SQLiteStorage:
    """Convenience wrapper that exposes SQLite-backed stores."""

    def __init__(self, db_path: Path):
        self._database = SQLiteDatabase(db_path)
        self.song_store = SQLiteSongStore(self._database)
        self.manifest_store = SQLiteManifestStore(self._database)

    @property
    def schema_version(self) -> int:
        return self._database.schema_version

    @property
    def path(self) -> Path:
        return self._database.path

    def close(self) -> None:
        self._database.close()

