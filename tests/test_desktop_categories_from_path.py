from pathlib import Path

import pytest

from storage.sqlite_store import SQLiteStorage

from songs_scanner import SongScanner
from tests.test_desktop_profile import _import_desktop_app
from tests.test_songs_scanner import _DummyDB


_TJA_TEMPLATE = "\n".join([
    "TITLE:{title}",
    "COURSE:Oni",
    "LEVEL:5",
    "#START",
    "1111,",
    "#END",
])


def _write_minimal_tja(directory: Path, title: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    tja_path = directory / "main.tja"
    tja_path.write_text(_TJA_TEMPLATE.format(title=title), encoding="utf-8")


def _create_desktop_scanner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[SongScanner, SQLiteStorage, Path]:
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("RUN_PROFILE", "desktop")
    sqlite_storage = SQLiteStorage(tmp_path / "taiko.db")

    db = _DummyDB()
    db.songs = sqlite_storage.song_store
    db.songs_manifest = sqlite_storage.manifest_store

    scanner = SongScanner(
        db=db,
        songs_dir=songs_dir,
        songs_baseurl="/songs/",
        ignore_globs=None,
        song_store=sqlite_storage.song_store,
        manifest_store=sqlite_storage.manifest_store,
    )
    return scanner, sqlite_storage, songs_dir


@pytest.mark.parametrize(
    "folder,title,expected",
    [
        ("02 Anime/TrackA", "TrackA", ("Anime", 2, "anime")),
        ("06 Classical/TrackB", "TrackB", ("Classical", 6, "classical")),
    ],
)
def test_desktop_categories_from_path(folder: str, title: str, expected: tuple[str, int, str], tmp_path, monkeypatch):
    scanner, storage, songs_dir = _create_desktop_scanner(Path(tmp_path), monkeypatch)
    folder_path = songs_dir / folder
    _write_minimal_tja(folder_path, title)

    summary = scanner.scan(full=True)
    assert summary.get("errors") == 0

    documents = list(storage.song_store.find({}, projection={
        "title": 1,
        "category": 1,
        "category_id": 1,
        "meta": 1,
    }))
    assert documents, "expected at least one scanned song"

    indexed = {doc.get("title"): doc for doc in documents}
    assert title in indexed
    document = indexed[title]
    expected_title, expected_id, expected_slug = expected
    meta = document.get("meta") or {}
    assert meta.get("category") == expected_title
    assert meta.get("category_id") == expected_id
    assert meta.get("category_slug") == expected_slug
    assert meta.get("category_key") == expected_slug


@pytest.mark.parametrize(
    "folder,title,expected",
    [
        ("04 Children and Folk/SongA", "SongA", ("Children & Folk", 4, "children")),
    ],
)
def test_desktop_children_and_folk_synonym(folder: str, title: str, expected: tuple[str, int, str], tmp_path, monkeypatch):
    scanner, storage, songs_dir = _create_desktop_scanner(Path(tmp_path), monkeypatch)
    folder_path = songs_dir / folder
    _write_minimal_tja(folder_path, title)

    summary = scanner.scan(full=True)
    assert summary.get("errors") == 0

    documents = list(storage.song_store.find({}, projection={
        "title": 1,
        "category": 1,
        "category_id": 1,
        "meta": 1,
    }))
    assert documents
    document = documents[0]
    expected_title, expected_id, expected_slug = expected
    meta = document.get("meta") or {}
    assert meta.get("category") == expected_title
    assert meta.get("category_id") == expected_id
    assert meta.get("category_slug") == expected_slug
    assert meta.get("category_key") == expected_slug


def test_desktop_categories_endpoint_aggregates(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, Path(tmp_path))
    songs_dir = Path(app_module.SONGS_DIR_PATH)

    _write_minimal_tja(songs_dir / "02 Anime" / "Alpha", "Alpha")
    _write_minimal_tja(songs_dir / "04 Children and Folk" / "Beta", "Beta")
    _write_minimal_tja(songs_dir / "06 Classical" / "Gamma", "Gamma")

    db = _DummyDB()
    db.songs = app_module.SONG_STORE
    db.songs_manifest = app_module.MANIFEST_STORE

    scanner = SongScanner(
        db=db,
        songs_dir=songs_dir,
        songs_baseurl="/songs/",
        ignore_globs=None,
        song_store=app_module.SONG_STORE,
        manifest_store=app_module.MANIFEST_STORE,
    )
    summary = scanner.scan(full=True)
    assert summary.get("errors") == 0

    client = app_module.app.test_client()
    response = client.get("/api/categories")
    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, list)
    titles = {entry.get("title") for entry in payload if isinstance(entry, dict)}
    assert {"Anime", "Children & Folk", "Classical"}.issubset(titles)
    assert "Unsorted" not in titles
    counts = {entry.get("title"): entry.get("count") for entry in payload if isinstance(entry, dict)}
    for title in ("Anime", "Children & Folk", "Classical"):
        assert counts.get(title, 0) > 0
