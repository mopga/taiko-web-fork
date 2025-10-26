import sys
import time
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from storage.sqlite_store import (
    DifficultyFilter,
    Page,
    SongFilter,
    SortField,
    SQLiteStorage,
)


def _make_song(
    song_id: str,
    *,
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
    payload = {
        "song_id": song_id,
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
    assert sqlite_storage.schema_version == 1
    assert sqlite_storage.path.exists()


def test_upsert_and_get(sqlite_storage: SQLiteStorage) -> None:
    payloads = [
        _make_song("alpha", title="Alpha Song", artist="Composer", genre="Pop", updated_at=101),
        _make_song("beta", title="Beta Song", artist="Composer", genre="Rock", updated_at=102),
        _make_song("gamma", title="Gamma Song", artist="Producer", genre="Pop", updated_at=103),
    ]
    inserted = sqlite_storage.song_store.upsert_many(payloads)
    assert inserted == len(payloads)

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
