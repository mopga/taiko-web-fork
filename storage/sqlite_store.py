"""SQLite-backed storage implementations for desktop profile."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence


LOGGER = logging.getLogger(__name__)


SCHEMA_VERSION = 1


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
        self._apply_pragmas()
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

    def _apply_pragmas(self) -> None:
        pragmas = [
            "PRAGMA journal_mode = WAL",
            "PRAGMA synchronous = NORMAL",
            "PRAGMA temp_store = MEMORY",
            "PRAGMA foreign_keys = ON",
            "PRAGMA cache_size = -20000",
        ]
        cursor = self._connection.cursor()
        for pragma in pragmas:
            try:
                cursor.execute(pragma)
            except sqlite3.DatabaseError:
                LOGGER.warning("Failed to apply pragma: %s", pragma, exc_info=True)
        cursor.close()

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
        try:
            cursor = self._connection.execute("PRAGMA journal_mode")
            journal_mode_row = cursor.fetchone()
            journal_mode = journal_mode_row[0] if journal_mode_row else "?"
        except sqlite3.DatabaseError:
            journal_mode = "?"
        LOGGER.info(
            "SQLite storage initialised path=%s journal_mode=%s schema_version=%s",
            self.path,
            journal_mode,
            self._schema_version,
        )


class SQLiteSongStore:
    """SQLite-backed implementation of the songs data access layer."""

    def __init__(self, database: SQLiteDatabase):
        self._db = database

    @property
    def path(self) -> Path:
        return self._db.path

    def upsert_many(self, songs: Iterable[Mapping[str, Any]]) -> int:
        rows = [self._prepare_song_row(song) for song in songs]
        if not rows:
            return 0
        start = time.perf_counter()
        LOGGER.info("SQLiteSongStore upsert_many start rows=%d", len(rows))
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
        with self._db.connection:
            self._db.executemany(query, rows)
        duration_ms = (time.perf_counter() - start) * 1000
        LOGGER.info(
            "SQLiteSongStore upsert_many complete rows=%d duration_ms=%.2f",
            len(rows),
            duration_ms,
        )
        return len(rows)

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
        return payload

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

