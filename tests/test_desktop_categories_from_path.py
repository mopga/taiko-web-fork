from pathlib import Path

import pytest

from storage.sqlite_store import SQLiteStorage

from songs_scanner import SongScanner
from desktop_categories import CANON_DESKTOP, resolve_category
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


def test_desktop_categories_empty_matches_canon(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, Path(tmp_path))
    client = app_module.app.test_client()

    response = client.get("/api/categories")
    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, list)
    assert len(payload) == len(CANON_DESKTOP)
    for index, canon in enumerate(CANON_DESKTOP):
        entry = payload[index]
        assert isinstance(entry, dict)
        assert entry.get("id") == canon["id"]
        assert entry.get("title") == canon["title"]
        assert entry.get("aliases") == canon["aliases"]
        assert entry.get("title_lang") == canon["title_lang"]
        assert entry.get("song_skin") == canon["song_skin"]
        assert entry.get("count") == 0


@pytest.mark.parametrize(
    "folder,title,expected",
    [
        ("01 Pop/TrackA", "TrackA", ("Pop", 1, "pop")),
        ("02 Anime/TrackB", "TrackB", ("Anime", 2, "anime")),
        ("03 VOCALOID/TrackC", "TrackC", ("VOCALOID", 3, "vocaloid")),
        ("04 Children & Folk/TrackF", "TrackF", ("Children & Folk", 4, "children")),
        ("04 Kids Party/TrackG", "TrackG", ("Children & Folk", 4, "children")),
        ("06 Game Music/TrackD", "TrackD", ("Game Music", 7, "game")),
        ("07 NAMCO Original/TrackE", "TrackE", ("NAMCO Original", 8, "namco")),
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


def test_desktop_categories_endpoint_aggregates(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, Path(tmp_path))
    songs_dir = Path(app_module.SONGS_DIR_PATH)

    _write_minimal_tja(songs_dir / "02 Anime" / "Alpha", "Alpha")
    _write_minimal_tja(songs_dir / "04 Children" / "Beta", "Beta")
    _write_minimal_tja(songs_dir / "06 Game Music" / "Gamma", "Gamma")
    _write_minimal_tja(songs_dir / "07 NAMCO Original" / "Delta", "Delta")

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
    assert len(payload) >= len(CANON_DESKTOP)
    titles = [entry.get("title") for entry in payload if isinstance(entry, dict)]
    assert titles[: len(CANON_DESKTOP)] == [canon["title"] for canon in CANON_DESKTOP]
    counts = {entry.get("title"): entry.get("count") for entry in payload if isinstance(entry, dict)}
    for title in ("Anime", "Children & Folk", "Game Music", "NAMCO Original"):
        assert counts.get(title, 0) > 0
    for canon in CANON_DESKTOP:
        if canon["title"] not in {"Anime", "Children & Folk", "Game Music", "NAMCO Original"}:
            assert counts.get(canon["title"], 0) == 0


def test_desktop_dynamic_category_emitted(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, Path(tmp_path))
    songs_dir = Path(app_module.SONGS_DIR_PATH)

    _write_minimal_tja(songs_dir / "99 Chiptune" / "Delta", "Delta")

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
    categories_response = client.get("/api/categories")
    assert categories_response.status_code == 200
    categories_payload = categories_response.get_json()
    assert isinstance(categories_payload, list)
    dynamic_entries = [
        entry
        for entry in categories_payload
        if isinstance(entry, dict) and entry.get("title") == "Chiptune"
    ]
    assert dynamic_entries, "expected dynamic category"
    dynamic_entry = dynamic_entries[0]
    assert isinstance(dynamic_entry.get("id"), int)
    assert dynamic_entry["id"] >= 100
    assert dynamic_entry.get("count") == 1

    canonical_slice = categories_payload[: len(CANON_DESKTOP)]
    assert len(canonical_slice) == len(CANON_DESKTOP)
    for canonical, actual in zip(CANON_DESKTOP, canonical_slice):
        assert actual.get("id") == canonical["id"]
        assert actual.get("title") == canonical["title"]
        assert actual.get("aliases") == canonical["aliases"]
        assert actual.get("title_lang") == canonical["title_lang"]
        assert actual.get("song_skin") == canonical["song_skin"]

    dynamic_titles = [
        entry.get("title")
        for entry in categories_payload[len(CANON_DESKTOP) :]
        if isinstance(entry, dict)
    ]
    assert dynamic_titles == sorted(dynamic_titles)

    songs_response = client.get("/api/songs")
    assert songs_response.status_code == 200
    songs_payload = songs_response.get_json()
    assert isinstance(songs_payload, list)
    song_entry = next(
        (
            entry
            for entry in songs_payload
            if isinstance(entry, dict) and entry.get("title") == "Delta"
        ),
        None,
    )
    assert song_entry, "expected scanned song"
    assert song_entry.get("category_id") == dynamic_entry["id"]


def test_desktop_dynamic_category_hot_reload(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, Path(tmp_path))
    songs_dir = Path(app_module.SONGS_DIR_PATH)

    _write_minimal_tja(songs_dir / "01 Pop" / "Alpha", "Alpha")

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

    initial_response = client.get("/api/categories")
    assert initial_response.status_code == 200
    initial_payload = initial_response.get_json()
    assert isinstance(initial_payload, list)
    assert not any(entry.get("title") == "Fusion" for entry in initial_payload if isinstance(entry, dict))

    _write_minimal_tja(songs_dir / "99 Fusion" / "Echo", "Echo")

    summary = scanner.scan(full=True)
    assert summary.get("errors") == 0

    refreshed_response = client.get("/api/categories")
    assert refreshed_response.status_code == 200
    refreshed_payload = refreshed_response.get_json()
    assert isinstance(refreshed_payload, list)
    fusion_entries = [
        entry
        for entry in refreshed_payload
        if isinstance(entry, dict) and entry.get("title") == "Fusion"
    ]
    assert fusion_entries, "expected newly added category to appear after rescan"


def test_desktop_resolve_category_prefers_alias_for_legacy_ids():
    canonical = resolve_category(4, "Variety")
    assert canonical is not None
    assert canonical.slug == "variety"

    canonical = resolve_category(6, "Game Music")
    assert canonical is not None
    assert canonical.slug == "game"

    canonical = resolve_category(7, "NAMCO Original")
    assert canonical is not None
    assert canonical.slug == "namco"


def test_desktop_songs_api_category_ids_within_canon(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, Path(tmp_path))
    songs_dir = Path(app_module.SONGS_DIR_PATH)

    _write_minimal_tja(songs_dir / "01 Pop" / "Alpha", "Alpha")
    _write_minimal_tja(songs_dir / "02 Anime" / "Beta", "Beta")
    _write_minimal_tja(songs_dir / "06 Game Music" / "Gamma", "Gamma")

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
    response = client.get("/api/songs?limit=10")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload
    if isinstance(payload, list):
        songs = payload
    elif isinstance(payload, dict):
        candidates = [
            payload.get("items"),
            payload.get("songs"),
            payload.get("data"),
        ]
        songs = next((value for value in candidates if isinstance(value, list)), [])
    else:
        songs = []
    assert songs, "expected songs payload"
    for song in songs:
        if not isinstance(song, dict):
            continue
        category_id = song.get("category_id")
        assert isinstance(category_id, int)
        assert category_id in {entry["id"] for entry in CANON_DESKTOP} or category_id >= 100


def test_desktop_cache_headers(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, Path(tmp_path))
    songs_dir = Path(app_module.SONGS_DIR_PATH)

    _write_minimal_tja(songs_dir / "01 Pop" / "Alpha", "Alpha")

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

    categories_response = client.get("/api/categories")
    assert categories_response.status_code == 200
    assert categories_response.headers.get("Cache-Control") == "no-store, must-revalidate"
    assert categories_response.headers.get("ETag")

    songs_response = client.get("/api/songs")
    assert songs_response.status_code == 200
    assert songs_response.headers.get("Cache-Control") == "no-store, must-revalidate"
    assert songs_response.headers.get("ETag")
