import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from storage.sqlite_store import (
    DifficultyFilter,
    Page,
    SongFilter,
    SortField,
    SQLiteSongUpsertItem,
    SQLiteStorage,
)


def _make_song(
    song_id: str,
    *,
    scanner_stable_id: str | None = None,
    group_key: str | None = None,
    title: str | None = None,
    title_reading: str | None = None,
    artist: str | None = None,
    genre: str | None = None,
    bpm: float | None = None,
    duration_ms: int | None = None,
    is_playable: bool = True,
    difficulties: dict | None = None,
    tags: list[str] | None = None,
    meta: dict | None = None,
    updated_at: int = 0,
    created_at: int | None = None,
):
    stable_id = scanner_stable_id if scanner_stable_id is not None else song_id
    group_value = group_key if group_key is not None else f"group::{song_id}"
    payload = {
        "song_id": song_id,
        "scanner_stable_id": stable_id,
        "group_key": group_value,
        "title": title or f"Song {song_id}",
        "title_reading": title_reading,
        "artist": artist,
        "genre": genre,
        "bpm": bpm,
        "duration_ms": duration_ms,
        "is_playable": is_playable,
        "difficulties": difficulties or {},
        "tags": tags,
        "meta": meta,
        "updated_at": updated_at,
        "created_at": created_at if created_at is not None else updated_at,
    }
    return payload


def _seed_songs(amount: int):
    for index in range(amount):
        song_id = f"song-{index}"
        yield _make_song(
            song_id,
            title=f"Song {index:05d}",
            artist=f"Artist {index % 7}",
            genre=f"Genre {index % 5}",
            bpm=120 + (index % 60),
            duration_ms=180000 + (index * 10),
            is_playable=(index % 2 == 0),
            difficulties={
                "easy": {"stars": (index % 3) + 1, "valid": True},
                "oni": {"stars": 5 + (index % 5), "valid": index % 4 != 0},
            },
            tags=["tag", str(index % 4)],
            meta={"index": index},
            updated_at=1_700_000_000_000 + index,
            created_at=1_600_000_000_000 + index,
        )


@pytest.fixture()
def sqlite_storage(tmp_path: Path) -> SQLiteStorage:
    db_path = tmp_path / "taiko.db"
    storage = SQLiteStorage(db_path)
    yield storage
    storage.close()


def test_schema_initialization(sqlite_storage: SQLiteStorage) -> None:
    assert sqlite_storage.schema_version == 2
    assert sqlite_storage.path.exists()


def test_creates_parent_directories(tmp_path: Path) -> None:
    db_path = tmp_path / "nested" / "layer" / "taiko.db"
    storage = SQLiteStorage(db_path)
    try:
        assert db_path.parent.exists()
        assert storage.path == db_path
    finally:
        storage.close()


def test_upsert_and_get(sqlite_storage: SQLiteStorage) -> None:
    payloads = [
        _make_song("alpha", title="Alpha Song", artist="Composer", genre="Pop", updated_at=101),
        _make_song("beta", title="Beta Song", artist="Composer", genre="Rock", updated_at=102),
        _make_song("gamma", title="Gamma Song", artist="Producer", genre="Pop", updated_at=103),
    ]
    inserted = sqlite_storage.song_store.upsert_many(payloads)
    assert len(inserted) == len(payloads)
    assert all(isinstance(identifier, int) for identifier in inserted)

    beta = sqlite_storage.song_store.get_by_id("beta")
    assert beta is not None
    assert beta["song_id"] == "beta"
    assert beta["title"] == "Beta Song"
    assert beta["difficulties"] == {}

    page = sqlite_storage.song_store.query(
        SongFilter(is_playable=True, genres=["Pop"]),
        [SortField("title", ascending=True)],
        limit=10,
        offset=0,
    )
    assert isinstance(page, Page)
    assert page.total == 2
    assert [song["song_id"] for song in page.items] == ["alpha", "gamma"]


def test_upsert_uses_filter_song_id(sqlite_storage: SQLiteStorage) -> None:
    store = sqlite_storage.song_store
    payload = {
        "scanner_stable_id": "ignored",
        "group_key": "group::ignored",
        "title": "Filter Song",
        "updated_at": 0,
        "created_at": 0,
    }
    filter_doc = {"scanner_stable_id": "stable-1", "group_key": "group::stable-1"}

    inserted = store.upsert_many([SQLiteSongUpsertItem(payload, filter_doc)])
    assert inserted

    stored = store.find_one({"scanner_stable_id": "stable-1"})
    assert stored is not None
    assert stored["song_id"] == "stable-1"
    assert stored["group_key"] == "group::stable-1"


def test_upsert_many_without_returning(sqlite_storage: SQLiteStorage) -> None:
    store = sqlite_storage.song_store
    store._supports_returning = False  # force fallback path
    first = store.upsert_many([
        _make_song("fallback-1", updated_at=100, created_at=100),
    ])
    assert first and all(identifier > 0 for identifier in first)

    store.upsert_many([
        {
            "song_id": "fallback-1",
            "scanner_stable_id": "fallback-1",
            "group_key": "group::fallback-1",
            "title": "Updated",
            "updated_at": 200,
            "created_at": 100,
        }
    ])

    stored = store.find_one({"song_id": "fallback-1"})
    assert stored is not None
    assert stored["title"] == "Updated"


def test_filters_and_sorting(sqlite_storage: SQLiteStorage) -> None:
    sqlite_storage.song_store.upsert_many(
        [
            _make_song("s1", title="Morning", artist="Zeta", genre="Pop", updated_at=1),
            _make_song("s2", title="Evening", artist="Alpha", genre="Pop", updated_at=2),
            _make_song("s3", title="Midnight", artist="Alpha", genre="Rock", updated_at=3),
        ]
    )

    page = sqlite_storage.song_store.query(
        SongFilter(search="ni"),
        [SortField("artist", ascending=True), SortField("title", ascending=True)],
        limit=50,
        offset=0,
    )
    assert [entry["song_id"] for entry in page.items] == ["s2", "s3", "s1"]

    page = sqlite_storage.song_store.query(
        SongFilter(artist="alpha"),
        [SortField("updated_at", ascending=False)],
        limit=10,
        offset=0,
    )
    assert [entry["song_id"] for entry in page.items] == ["s3", "s2"]


def test_difficulty_filter(sqlite_storage: SQLiteStorage) -> None:
    sqlite_storage.song_store.upsert_many(
        [
            _make_song(
                "d1",
                difficulties={
                    "oni": {"stars": 9, "valid": True},
                    "ura": {"stars": 10, "valid": True},
                },
            ),
            _make_song(
                "d2",
                difficulties={
                    "oni": {"stars": 6, "valid": False},
                },
            ),
            _make_song(
                "d3",
                difficulties={
                    "oni": {"stars": 7, "valid": True},
                },
            ),
        ]
    )

    filter = SongFilter(
        difficulties=[DifficultyFilter(name="oni", require_valid=True, min_stars=8)]
    )
    page = sqlite_storage.song_store.query(filter, [SortField("title")], limit=10, offset=0)
    assert [entry["song_id"] for entry in page.items] == ["d1"]
    assert sqlite_storage.song_store.count(filter) == 1


def test_upsert_logs_key_mismatch_once(
    sqlite_storage: SQLiteStorage, caplog: pytest.LogCaptureFixture
) -> None:
    store = sqlite_storage.song_store
    payload = _make_song(
        "payload-id",
        scanner_stable_id="payload-stable",
        group_key="group::payload",
        title="Payload Title",
    )
    filter_doc = {"scanner_stable_id": "stable-id", "group_key": "group::stable"}

    caplog.set_level(logging.ERROR)
    store.upsert_many([SQLiteSongUpsertItem(payload, filter_doc)])

    mismatch_logs = [
        record
        for record in caplog.records
        if "ignoring payload key mismatch" in record.getMessage()
    ]
    assert mismatch_logs

    caplog.clear()
    store.upsert_many([SQLiteSongUpsertItem(payload, filter_doc)])
    assert not any(
        "ignoring payload key mismatch" in record.getMessage() for record in caplog.records
    )

    stored = store.get_by_id("payload-id")
    assert stored is not None
    assert stored["scanner_stable_id"] == "stable-id"
    assert stored["group_key"] == "group::stable"


def test_delete_obsolete(sqlite_storage: SQLiteStorage) -> None:
    sqlite_storage.song_store.upsert_many(
        [_make_song("o1"), _make_song("o2"), _make_song("o3")]
    )

    removed = sqlite_storage.song_store.delete_obsolete({"o1", "o3"})
    assert removed == 1
    remaining = sqlite_storage.song_store.query(SongFilter(), [SortField("song_id")], 10, 0)
    assert [song["song_id"] for song in remaining.items] == ["o1", "o3"]


def test_stats(sqlite_storage: SQLiteStorage) -> None:
    sqlite_storage.song_store.upsert_many(
        [
            _make_song("st1", genre="Pop", is_playable=True, updated_at=10),
            _make_song("st2", genre="Rock", is_playable=False, updated_at=20),
            _make_song("st3", genre="Pop", is_playable=True, updated_at=30),
        ]
    )
    stats = sqlite_storage.song_store.stats()
    assert stats["total_songs"] == 3
    assert stats["playable_songs"] == 2
    assert stats["latest_update"] == 30
    assert stats["by_genre"] == {"Pop": 2, "Rock": 1}


def test_manifest_round_trip(sqlite_storage: SQLiteStorage) -> None:
    manifest_store = sqlite_storage.manifest_store
    assert manifest_store.get("catalog_stats") is None

    manifest_store.put("catalog_stats", {"total": 10})
    stored = manifest_store.get("catalog_stats")
    assert stored == {"total": 10}


def test_song_store_protocol_surface(sqlite_storage: SQLiteStorage) -> None:
    song_store = sqlite_storage.song_store
    song_store.upsert_many([
        _make_song("alpha", artist="One"),
        _make_song("beta", artist="Two"),
    ])

    results = list(song_store.find({"song_id": {"$in": ["alpha", "beta"]}}))
    assert {doc["song_id"] for doc in results} == {"alpha", "beta"}

    single = song_store.find_one({"id": "alpha"})
    assert single is not None and single["song_id"] == "alpha"

    song_store.insert_one(_make_song("gamma", artist="Three"))
    assert song_store.find_one({"song_id": "gamma"}) is not None

    update_result = song_store.update_one({"song_id": "beta"}, {"$set": {"artist": "Updated"}})
    assert update_result.matched_count == 1
    assert song_store.find_one({"song_id": "beta"})["artist"] == "Updated"

    replace_result = song_store.replace_one(
        {"song_id": "gamma"}, _make_song("gamma", artist="Replace")
    )
    assert replace_result.matched_count == 1
    assert song_store.find_one({"song_id": "gamma"})["artist"] == "Replace"

    delete_result = song_store.delete_one({"song_id": "alpha"})
    assert delete_result.deleted_count == 1
    assert song_store.find_one({"song_id": "alpha"}) is None

    assert song_store.count_documents({"song_id": {"$in": ["beta", "gamma"]}}) == 2


def test_manifest_store_protocol_surface(sqlite_storage: SQLiteStorage) -> None:
    manifest_store = sqlite_storage.manifest_store
    manifest_store.put("catalog_stats", {"total": 10})
    manifest_store.put("scanner_state", {"status": "ok"})

    all_docs = list(manifest_store.find())
    assert {doc["_id"] for doc in all_docs} == {"catalog_stats", "scanner_state"}

    single = manifest_store.find_one({"_id": "catalog_stats"})
    assert single and single["total"] == 10

    manifest_store.update_one({"_id": "catalog_stats"}, {"$set": {"total": 11}})
    assert manifest_store.get("catalog_stats") == {"total": 11}

    deleted = manifest_store.delete_many({"_id": {"$in": ["scanner_state"]}})
    assert deleted.deleted_count == 1


def test_manifest_bulk_update_one_supported(sqlite_storage: SQLiteStorage) -> None:
    manifest_store = sqlite_storage.manifest_store

    class DummyUpdateOne:
        def __init__(self, filter_doc, update_doc, upsert=False):
            self.filter = filter_doc
            self.update = update_doc
            self.upsert = upsert

    manifest_store.bulk_write(
        [
            DummyUpdateOne({"_id": "entry"}, {"$set": {"value": 1, "updated_at": 1000}}, upsert=True),
            DummyUpdateOne({"_id": "entry"}, {"$set": {"value": 2}}),
        ]
    )

    stored = manifest_store.get("entry")
    assert stored == {"value": 2, "updated_at": 1000}


def test_manifest_update_one_accepts_id_alias(sqlite_storage: SQLiteStorage) -> None:
    manifest_store = sqlite_storage.manifest_store
    manifest_store.update_one({"id": "alias"}, {"$set": {"value": 42}}, upsert=True)
    assert manifest_store.get("alias") == {"value": 42}


def test_perf_smoke(sqlite_storage: SQLiteStorage) -> None:
    sqlite_storage.song_store.upsert_many(list(_seed_songs(5000)))
    start = time.perf_counter()
    page = sqlite_storage.song_store.query(
        SongFilter(is_playable=True), [SortField("title")], limit=50, offset=0
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert elapsed_ms < 50, f"Query exceeded 50 ms budget: {elapsed_ms:.2f}ms"
    assert len(page.items) == 50


def test_hot_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "taiko.db"
    storage = SQLiteStorage(db_path)
    storage.song_store.upsert_many([_make_song("hot")])
    storage.close()

    storage_again = SQLiteStorage(db_path)
    try:
        item = storage_again.song_store.get_by_id("hot")
        assert item is not None
        assert item["song_id"] == "hot"
    finally:
        storage_again.close()


def test_prepare_song_row_normalizes_datetime(sqlite_storage: SQLiteStorage) -> None:
    store = sqlite_storage.song_store
    updated_dt = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    created_iso = "2023-12-31T23:59:59+00:00"
    row = store._prepare_song_row(  # type: ignore[attr-defined]
        {
            "song_id": "dt",
            "scanner_stable_id": "dt",
            "group_key": "group::dt",
            "title": "Date Song",
            "updated_at": updated_dt,
            "created_at": created_iso,
        },
        None,
    )
    expected_updated = int(round(updated_dt.timestamp() * 1000))
    expected_created = int(round(datetime.fromisoformat(created_iso).timestamp() * 1000))
    assert row[-2] == expected_updated
    assert row[-1] == expected_created


def test_prepare_song_row_coerces_float(sqlite_storage: SQLiteStorage) -> None:
    store = sqlite_storage.song_store
    row = store._prepare_song_row(  # type: ignore[attr-defined]
        {
            "song_id": "float",
            "scanner_stable_id": "float",
            "group_key": "group::float",
            "title": "Float Song",
            "updated_at": 1000.4,
            "created_at": 2000.9,
        },
        None,
    )
    assert row[-2] == 1000
    assert row[-1] == 2001


def test_prepare_song_row_handles_invalid_timestamp(
    sqlite_storage: SQLiteStorage, caplog: pytest.LogCaptureFixture
) -> None:
    store = sqlite_storage.song_store
    caplog.set_level(logging.WARNING)
    row = store._prepare_song_row(  # type: ignore[attr-defined]
        {
            "song_id": "invalid",
            "scanner_stable_id": "invalid",
            "group_key": "group::invalid",
            "title": "Invalid",
            "updated_at": {"bad": True},
        },
        None,
    )
    assert row[-2] == 0
    assert row[-1] == 0
    assert any("Unable to normalise song field updated_at" in record.message for record in caplog.records)


def test_title_recovery_callback_invoked(sqlite_storage: SQLiteStorage) -> None:
    store = sqlite_storage.song_store
    recovered: list[tuple[str, Optional[str]]] = []

    def _callback(song_id: str, source: Optional[str]) -> None:
        recovered.append((song_id, source))

    store.set_title_recovered_callback(_callback)  # type: ignore[attr-defined]
    payload = {
        "song_id": "missing",
        "scanner_stable_id": "missing-stable",
        "group_key": "group::missing",
        "title": " ",
        "title_en": "Recovered Title",
        "updated_at": datetime(2024, 1, 2, tzinfo=UTC),
    }
    store.upsert_many([payload])
    assert recovered == [("missing-stable", "title_en")]
