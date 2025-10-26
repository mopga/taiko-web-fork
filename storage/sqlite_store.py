"""SQLite-backed storage implementations for desktop profile."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence


LOGGER = logging.getLogger(__name__)


SCHEMA_VERSION = 1

UPSERT_CHUNK_SIZE = 500


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
        pragmas = [
            "PRAGMA journal_mode = WAL",
            "PRAGMA synchronous = NORMAL",
            "PRAGMA temp_store = MEMORY",
            "PRAGMA foreign_keys = ON",
            "PRAGMA cache_size = -20000",
        ]
        applied: dict[str, Any] = {}

        cursor = self._connection.cursor()
        try:
            for pragma in pragmas:
                try:
                    cursor.execute(pragma)
                except sqlite3.DatabaseError:
                    LOGGER.warning("Failed to apply pragma: %s", pragma, exc_info=True)
        finally:
            cursor.close()

        read_cursor = self._connection.cursor()
        try:
            for pragma_name in [
                "journal_mode",
                "synchronous",
                "temp_store",
                "foreign_keys",
                "cache_size",
            ]:
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
                self._create_schema_v1(cursor)
                cursor.execute(
                    "INSERT INTO schema_version(version, applied_at) VALUES(?, datetime('now'))",
                    (SCHEMA_VERSION,),
                )
                version = SCHEMA_VERSION
            else:
                version = int(row[0])
            cursor.close()
        return version

    def _create_schema_v1(self, cursor: sqlite3.Cursor) -> None:
        cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS songs(
                song_id TEXT PRIMARY KEY,
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
                updated_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS manifest(
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_songs_is_playable ON songs(is_playable);
            CREATE INDEX IF NOT EXISTS idx_songs_title ON songs(title);
            CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs(artist);
            CREATE INDEX IF NOT EXISTS idx_songs_genre ON songs(genre);
            CREATE INDEX IF NOT EXISTS idx_songs_updated_at ON songs(updated_at);
            """
        )

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

    @property
    def path(self) -> Path:
        return self._db.path

    def upsert_many(self, songs: Iterable[Mapping[str, Any]]) -> int:
        prepared_rows = [self._prepare_song_row(song) for song in songs]
        if not prepared_rows:
            return 0

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
                song_id, title, title_reading, artist, genre, bpm, duration_ms,
                is_playable, difficulties_json, tags_json, meta_json, updated_at, created_at
            ) VALUES(
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(song_id) DO UPDATE SET
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
                updated_at=excluded.updated_at,
                created_at=excluded.created_at
            """
        )

        processed = 0
        for chunk_index, chunk_start in enumerate(
            range(0, total_rows, UPSERT_CHUNK_SIZE), start=1
        ):
            chunk_rows = prepared_rows[chunk_start : chunk_start + UPSERT_CHUNK_SIZE]
            chunk_begin = time.perf_counter()
            self._db.execute_bulk(query, chunk_rows)
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

        duration_ms = (time.perf_counter() - start) * 1000
        LOGGER.info(
            "SQLiteSongStore upsert_many complete rows=%d duration_ms=%.2f",
            total_rows,
            duration_ms,
        )
        return total_rows

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
        self.upsert_many([document])
        return SQLiteInsertOneResult(document.get("song_id"))

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
            self.upsert_many([base])
        elif existing is None and upsert:
            self.upsert_many([base])
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
        self.upsert_many([replacement])
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
                self.upsert_many([payload])
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

    def _prepare_song_row(self, song: Mapping[str, Any]) -> tuple[Any, ...]:
        song_id = str(song.get("song_id"))
        if not song_id:
            raise ValueError("song payload missing song_id")
        title = song.get("title")
        if not isinstance(title, str):
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
        updated_at = int(song.get("updated_at", 0))
        created_at = int(song.get("created_at", updated_at))
        return (
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
            updated_at,
            created_at,
        )

    def _row_to_song(self, row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(row)
        payload["is_playable"] = bool(payload.get("is_playable"))
        payload["difficulties"] = _deserialize_json(payload.pop("difficulties_json", None)) or {}
        payload["tags"] = _deserialize_json(payload.pop("tags_json", None))
        payload["meta"] = _deserialize_json(payload.pop("meta_json", None))
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
            else:
                LOGGER.warning(
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

    def _manifest_id_from_filter(self, filter: Mapping[str, Any]) -> Optional[str]:
        identifier = filter.get("_id")
        if isinstance(identifier, str):
            return identifier
        if isinstance(identifier, Mapping):
            if "$eq" in identifier and isinstance(identifier["$eq"], str):
                return identifier["$eq"]
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

