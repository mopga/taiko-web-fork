import importlib
import logging
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from cachelib.file import FileSystemCache

import server.paths as server_paths

from songs_scanner import SongScanner

from tests.test_songs_scanner import _DummyDB
from desktop_categories import CANON_DESKTOP

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _import_desktop_app(monkeypatch, tmp_path: Path):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir(parents=True, exist_ok=True)

    def _songs_dir_factory() -> Path:
        songs_dir.mkdir(parents=True, exist_ok=True)
        return songs_dir

    monkeypatch.setenv("RUN_PROFILE", "desktop")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    monkeypatch.setattr(server_paths, "songs_dir", _songs_dir_factory)
    monkeypatch.setattr(server_paths, "get_songs_dir", _songs_dir_factory)
    monkeypatch.setattr(server_paths, "get_songs_dir_desktop", _songs_dir_factory)
    sys.modules.pop("app", None)
    for module_name in list(sys.modules.keys()):
        if module_name == "flask_session" or module_name.startswith("flask_session."):
            sys.modules.pop(module_name, None)
    return importlib.import_module("app")


def test_desktop_healthz(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.get("/healthz")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "status": "ok",
        "profile": "desktop",
        "db_path": str((tmp_path / "taiko.db").resolve()),
    }


def test_sessions_filesystem_directory_created(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    cache_backend = app_module.app.config["SESSION_CACHELIB"]
    assert isinstance(cache_backend, FileSystemCache)
    sessions_dir = Path(cache_backend._path)
    assert sessions_dir.exists()
    assert app_module.app.config.get("SESSION_TYPE") == "cachelib"

    def _touch_session():
        from flask import session as flask_session

        flask_session["ping"] = "pong"
        return "ok"

    app_module.app.add_url_rule("/_test_session", "_test_session", _touch_session)

    def _read_session():
        from flask import session as flask_session

        return flask_session.get("ping", "missing")

    app_module.app.add_url_rule("/_read_session", "_read_session", _read_session)

    client = app_module.app.test_client()
    response = client.get("/_test_session")
    assert response.status_code == 200

    cookie_header = response.headers.get("Set-Cookie", "")
    assert "session=" in cookie_header

    follow_response = client.get("/_read_session")
    assert follow_response.status_code == 200
    assert follow_response.get_data(as_text=True) == "pong"

    cache = app_module.app.session_interface.cache
    data_files = list(cache._list_dir())
    assert data_files


def test_no_redis_in_desktop(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    assert app_module.app.config.get("SESSION_TYPE") == "cachelib"
    assert app_module.app.config.get("SESSION_REDIS") is None
    assert "redis" not in caplog.text.lower()


def test_desktop_song_route_serves_and_restricts(tmp_path, monkeypatch):
    songs_dir = tmp_path / "songs"
    app_module = _import_desktop_app(monkeypatch, tmp_path)

    pack_dir = songs_dir / "TestPack"
    pack_dir.mkdir()
    tja_payload = "#TITLE Test Pack\n"
    tja_path = pack_dir / "Test.tja"
    tja_path.write_text(tja_payload, encoding="utf-8")
    ogg_payload = b"OggS\x00\x02"
    ogg_path = pack_dir / "Test.ogg"
    ogg_path.write_bytes(ogg_payload)

    client = app_module.app.test_client()

    tja_response = client.get("/songs/TestPack/Test.tja")
    assert tja_response.status_code == 404

    ogg_response = client.get("/songs/TestPack/Test.ogg")
    assert ogg_response.status_code == 404

    traversal_response = client.get("/songs/../../etc/passwd")
    assert traversal_response.status_code == 404


def test_desktop_song_route_id_fallback(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    songs_dir = tmp_path / "songs"
    song_store = app_module.SONG_STORE
    assert song_store is not None
    manifest_store = app_module.MANIFEST_STORE

    now = int(time.time())
    song_store.upsert_many(
        [
            {
                "song_id": "custom-id",
                "scanner_stable_id": "stable-custom",
                "group_key": "group::custom",
                "title": "Custom Song",
                "tja_path": "CustomPack/main.tja",
                "assets": {
                    "tja_main": "CustomPack/main.tja",
                    "files": {"jacket.png": "CustomPack/jacket.png"},
                },
                "updated_at": now,
                "created_at": now,
            }
        ]
    )

    if manifest_store is not None:
        manifest_store.put(
            "stable-custom",
            {
                "file_path": "CustomPack/main.tja",
                "paths": {
                    "tja_url": "/songs/CustomPack/main.tja",
                    "dir_url": "/songs/CustomPack/",
                },
            },
        )

    pack_dir = songs_dir / "CustomPack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    main_path = pack_dir / "main.tja"
    main_path.write_text("#TITLE Custom", encoding="utf-8")
    cover_path = pack_dir / "jacket.png"
    cover_payload = b"PNG"
    cover_path.write_bytes(cover_payload)

    client = app_module.app.test_client()

    main_response = client.get("/songs/custom-id/main.tja")
    assert main_response.status_code == 200
    assert main_response.data == main_path.read_bytes()

    cover_response = client.get("/songs/custom-id/jacket.png")
    assert cover_response.status_code == 200
    assert cover_response.data == cover_payload

    missing_response = client.get("/songs/custom-id/missing.bin")
    assert missing_response.status_code == 404


def test_desktop_serves_main_alias(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    manifest_store = app_module.MANIFEST_STORE
    songs_dir = tmp_path / "alias_songs"
    songs_dir.mkdir()
    app_module.DESKTOP_SONGS_DIR = songs_dir
    if hasattr(app_module, "SONGS_DIR_PATH"):
        app_module.SONGS_DIR_PATH = songs_dir

    pack_dir = songs_dir / "ScannerPack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    tja_path = pack_dir / "SongName.tja"
    tja_payload = "\n".join(
        [
            "TITLE:Alias Song",
            "WAVE:SongName.ogg",
            "COURSE:Oni",
            "LEVEL:5",
            "#START",
            "1111,",
            "#END",
        ]
    )
    tja_path.write_text(tja_payload, encoding="utf-8")
    audio_path = pack_dir / "SongName.ogg"
    audio_payload = b"\x99\x88"
    audio_path.write_bytes(audio_payload)

    scanner = SongScanner(
        db=_DummyDB(),
        songs_dir=songs_dir,
        songs_baseurl="/songs/",
        song_store=song_store,
        manifest_store=manifest_store,
    )
    summary = scanner.scan(full=True)
    assert summary.get("errors", 0) == 0

    stored_doc = song_store.find_one()
    assert stored_doc is not None
    song_identifier = stored_doc.get("song_id")
    assert isinstance(song_identifier, str)

    client = app_module.app.test_client()

    alias_response = client.get(f"/songs/{song_identifier}/main.tja")
    assert alias_response.status_code == 200
    assert alias_response.data == tja_path.read_bytes()

    direct_response = client.get(f"/songs/{song_identifier}/SongName.tja")
    assert direct_response.status_code == 200
    assert direct_response.data == tja_path.read_bytes()

    audio_response = client.get(f"/songs/{song_identifier}/SongName.ogg")
    assert audio_response.status_code == 200
    assert audio_response.data == audio_payload

    traversal_response = client.get(f"/songs/{song_identifier}/../../x")
    assert traversal_response.status_code == 404


def test_desktop_dojo_and_tower_chart_endpoints(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    songs_dir = tmp_path / "songs"

    dojo_dir = songs_dir / "DojoSample"
    dojo_dir.mkdir(parents=True, exist_ok=True)
    dojo_playlist = dojo_dir / "dojo_segments.t3u8"
    dojo_main = dojo_dir / "main.tja"

    dojo_segments = [
        dojo_dir / "dan_segment_1.tja",
        dojo_dir / "dan_segment_2.tja",
    ]

    dojo_main.write_text("\n".join([
        "TITLE:Dojo Sample",
        "WAVE:dojo_segments.t3u8",
        "COURSE:DAN",
        "LEVEL:1",
        "#START",
        "#END",
    ]), encoding="utf-8")

    dojo_playlist.write_text("\n".join([
        "#EXTM3U",
        dojo_segments[0].name,
        dojo_segments[1].name,
    ]), encoding="utf-8")

    dojo_segments[0].write_text("\n".join([
        "TITLE:Segment A",
        "COURSE:Oni",
        "LEVEL:5",
        "BPM:120",
        "#START",
        "1111,",
        "#END",
    ]), encoding="utf-8")

    dojo_segments[1].write_text("\n".join([
        "TITLE:Segment B",
        "COURSE:Oni",
        "LEVEL:6",
        "BPM:150",
        "#START",
        "2222,",
        "#END",
    ]), encoding="utf-8")

    tower_dir = songs_dir / "TowerSample"
    tower_dir.mkdir(parents=True, exist_ok=True)
    tower_playlist = tower_dir / "tower_segments.t3u8"
    tower_main = tower_dir / "main.tja"

    tower_segments = [
        tower_dir / "tower_segment_1.tja",
        tower_dir / "tower_segment_2.tja",
    ]

    tower_main.write_text("\n".join([
        "TITLE:Tower Sample",
        "WAVE:tower_segments.t3u8",
        "COURSE:TOWER",
        "LEVEL:2",
        "#START",
        "#END",
    ]), encoding="utf-8")

    tower_playlist.write_text("\n".join([
        "#EXTM3U",
        tower_segments[0].name,
        tower_segments[1].name,
    ]), encoding="utf-8")

    tower_segments[0].write_text("\n".join([
        "TITLE:Tower Segment 1",
        "COURSE:Oni",
        "LEVEL:7",
        "BPM:140",
        "#START",
        "3333,",
        "#END",
    ]), encoding="utf-8")

    tower_segments[1].write_text("\n".join([
        "TITLE:Tower Segment 2",
        "COURSE:Oni",
        "LEVEL:8",
        "BPM:160",
        "#START",
        "4444,",
        "#END",
    ]), encoding="utf-8")

    scan_summary = app_module.perform_song_scan(full=True)
    assert scan_summary.get('errors', 0) == 0

    client = app_module.app.test_client()

    song_store = app_module.SONG_STORE
    assert song_store is not None

    entries = list(song_store.find({}))
    dojo_entry = next(
        (
            doc
            for doc in entries
            if doc.get('assets', {}).get('playlist_path') == 'DojoSample/dojo_segments.t3u8'
        ),
        None,
    )
    assert dojo_entry is not None

    tower_entry = next(
        (
            doc
            for doc in entries
            if doc.get('assets', {}).get('playlist_path') == 'TowerSample/tower_segments.t3u8'
        ),
        None,
    )
    assert tower_entry is not None

    manifest_store = app_module.MANIFEST_STORE
    assert manifest_store is not None
    manifest_entries = list(manifest_store.find({'_id': {'$ne': '__meta__'}}))

    dojo_manifest = next(
        (
            doc
            for doc in manifest_entries
            if any(
                (chart.get('mode') or '').strip().lower() == 'dandojo'
                for chart in doc.get('charts') or []
            )
        ),
        None,
    )
    assert dojo_manifest is not None
    dojo_chart_entry = next(
        chart
        for chart in dojo_manifest.get('charts') or []
        if (chart.get('mode') or '').strip().lower() == 'dandojo'
    )
    assert dojo_chart_entry.get('chart_data', {}).get('meta', {}).get('segments')
    rank_token = str(
        dojo_chart_entry.get('display_course')
        or dojo_chart_entry.get('rank')
        or dojo_chart_entry.get('canonical_course')
        or 'dan'
    ).strip().lower()

    tower_manifest = next(
        (
            doc
            for doc in manifest_entries
            if any(
                (chart.get('mode') or '').strip().lower() == 'tower'
                for chart in doc.get('charts') or []
            )
        ),
        None,
    )
    assert tower_manifest is not None
    tower_chart_entry = next(
        chart
        for chart in tower_manifest.get('charts') or []
        if (chart.get('mode') or '').strip().lower() == 'tower'
    )
    assert tower_chart_entry.get('chart_data', {}).get('meta', {}).get('segments')
    course_token = str(
        tower_chart_entry.get('canonical_course')
        or tower_chart_entry.get('course')
        or 'oni'
    ).strip().lower()

    dan_response = client.get(
        "/api/dan/chart",
        query_string={"title": "Dojo Sample", "rank": rank_token},
    )
    assert dan_response.status_code == 200
    dan_payload = dan_response.get_json()
    assert dan_payload["status"] == "ok"
    dan_chart = dan_payload["chart_data"]
    assert dan_chart["duration_ms"] > 0
    assert dan_chart["notes"]
    assert dan_chart.get("meta", {}).get("segments")
    assert dan_chart.get("meta", {}).get("playlist_path") == "DojoSample/dojo_segments.t3u8"

    tower_response = client.get(
        "/api/tower/chart",
        query_string={"title": "Tower Sample", "course": course_token},
    )
    assert tower_response.status_code == 200
    tower_payload = tower_response.get_json()
    assert tower_payload["status"] == "ok"
    tower_chart = tower_payload["chart_data"]
    assert tower_chart["duration_ms"] > 0
    assert tower_chart["notes"]
    assert tower_chart.get("meta", {}).get("segments")
    assert tower_chart.get("meta", {}).get("playlist_path") == "TowerSample/tower_segments.t3u8"


def test_desktop_hot_start_fast_path(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)

    songs_dir = tmp_path / "songs"
    pack_dir = songs_dir / "HotStart"
    pack_dir.mkdir(parents=True, exist_ok=True)

    tja_path = pack_dir / "main.tja"
    tja_path.write_text(
        "\n".join(
            [
                "TITLE:Hot Start Song",
                "WAVE:main.ogg",
                "COURSE:Oni",
                "LEVEL:5",
                "#START",
                "1111,",
                "#END",
            ]
        ),
        encoding="utf-8",
    )
    (pack_dir / "main.ogg").write_bytes(b"OggS\x00\x02")

    summary_first = app_module.perform_song_scan(full=False)
    assert summary_first.get('fast_path') is False
    assert (summary_first.get('songs_count_after') or 0) >= 1

    duration_first = float(summary_first.get('duration_seconds') or 0.0)

    summary_second = app_module.perform_song_scan(full=False)
    assert summary_second.get('fast_path') is True
    assert summary_second.get('reason') == 'hot_start'
    assert summary_second.get('inserted', 0) == 0
    assert summary_second.get('updated', 0) == 0
    duration_second = float(summary_second.get('duration_seconds') or 0.0)
    assert duration_second <= duration_first
    assert (summary_second.get('songs_count_after') or 0) >= 1

    tja_path.write_text(
        "\n".join(
            [
                "TITLE:Hot Start Song",
                "WAVE:main.ogg",
                "COURSE:Oni",
                "LEVEL:5",
                "#START",
                "1111,",
                "2222,",
                "#END",
            ]
        ),
        encoding="utf-8",
    )

    summary_third = app_module.perform_song_scan(full=False)
    assert summary_third.get('fast_path') is False


def test_web_profile_unchanged(tmp_path, monkeypatch):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir()
    monkeypatch.setenv("RUN_PROFILE", "web")
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    monkeypatch.setenv("TAIKO_WEB_MONGO_HOST", "localhost:27017")
    monkeypatch.setenv("TAIKO_WEB_MONGO_DB", "taiko-test")
    monkeypatch.setenv("TAIKO_WEB_REDIS_HOST", "localhost")
    monkeypatch.setenv("TAIKO_WEB_REDIS_PORT", "6379")
    monkeypatch.setenv("TAIKO_WEB_REDIS_DB", "0")
    monkeypatch.delenv("TAIKO_WEB_REDIS_PASSWORD", raising=False)

    fake_client = mock.MagicMock()
    fake_db = mock.MagicMock()
    fake_client.__getitem__.return_value = fake_db

    sys.modules.pop("app", None)
    with mock.patch("pymongo.MongoClient", return_value=fake_client):
        with mock.patch("redis.Redis.ping", return_value=True):
            app_module = importlib.import_module("app")

    assert app_module.app.config["SESSION_TYPE"] == "redis"
    assert app_module.app.config.get("SESSION_BACKEND") == "redis"
    assert app_module.app.config.get("SESSION_FILE_DIR") is None


def test_standalone_entrypoint_imports_and_runs_uvicorn(tmp_path, monkeypatch):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir()
    monkeypatch.delenv("RUN_PROFILE", raising=False)
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    sys.modules.pop("app", None)

    config_calls = {}

    class FakeConfig:
        def __init__(self, app, host, port, log_level, access_log, lifespan):
            config_calls["app"] = app
            config_calls["host"] = host
            config_calls["port"] = port

    class FakeServer:
        def __init__(self, config):
            config_calls["config"] = config
            self.should_exit = False

        def run(self):
            config_calls["run"] = True

    fake_middleware = SimpleNamespace(WSGIMiddleware=lambda app: app)
    fake_uvicorn = SimpleNamespace(Config=FakeConfig, Server=FakeServer)
    sys.modules["uvicorn"] = fake_uvicorn
    sys.modules["uvicorn.middleware"] = SimpleNamespace(wsgi=fake_middleware)
    sys.modules["uvicorn.middleware.wsgi"] = fake_middleware

    module = importlib.reload(importlib.import_module("standalone.run_desktop"))
    exit_code = module.main(["--port", "12345"])

    assert exit_code == 0
    assert config_calls["host"] == "127.0.0.1"
    assert config_calls["port"] == 12345
    assert config_calls["run"] is True


def test_standalone_entrypoint_waitress(monkeypatch, tmp_path):
    songs_dir = tmp_path / "songs"
    songs_dir.mkdir()
    monkeypatch.delenv("RUN_PROFILE", raising=False)
    monkeypatch.setenv("SCAN_ON_START", "skip")
    monkeypatch.setenv("ENABLE_SONG_WATCHER", "0")
    monkeypatch.setenv("SONGS_DIR", str(songs_dir))
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("TAIKO_DESKTOP_SERVER", "waitress")

    sys.modules.pop("app", None)

    serve_calls = {}

    def fake_serve(app, host, port):
        serve_calls["app"] = app
        serve_calls["host"] = host
        serve_calls["port"] = port

    fake_waitress = SimpleNamespace(serve=fake_serve)
    sys.modules["waitress"] = fake_waitress

    module = importlib.reload(importlib.import_module("standalone.run_desktop"))
    exit_code = module.main(["--port", "23456"])

    assert exit_code == 0
    assert serve_calls["host"] == "127.0.0.1"
    assert serve_calls["port"] == 23456


def test_desktop_admin_routes_guarded(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.get("/admin/songs")
    assert response.status_code == 503
    assert b"desktop profile" in response.data.lower()


def test_desktop_api_modes_endpoint(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.get("/api/modes")
    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, dict)
    status_value = payload.get("status")
    assert status_value in {"ok", "disabled"}
    if status_value == "ok":
        assert isinstance(payload.get("modes"), list)


def test_desktop_modes_and_categories_with_song_data(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    assert song_store is not None

    now = int(time.time())
    song_store.upsert_many(
        [
            {
                "song_id": "song-001",
                "scanner_stable_id": "song-001",
                "group_key": "group::song-001",
                "title": "Tower Intro",
                "category_id": 7,
                "category": "Taiko Towers",
                "meta": {"category": "Taiko Towers", "category_id": 7},
                "updated_at": now,
                "created_at": now,
            },
            {
                "song_id": "song-002",
                "scanner_stable_id": "song-002",
                "group_key": "group::song-002",
                "title": "Dan Challenge",
                "category_id": 9,
                "category": "Dan Dojo",
                "meta": {"category": "Dan Dojo", "category_id": 9},
                "updated_at": now,
                "created_at": now,
            },
        ]
    )

    client = app_module.app.test_client()
    categories_response = client.get("/api/categories")
    assert categories_response.status_code == 200
    categories_payload = categories_response.get_json()
    assert isinstance(categories_payload, list)
    assert categories_payload
    assert all(isinstance(item, dict) for item in categories_payload)
    canonical_slice = categories_payload[: len(CANON_DESKTOP)]
    assert len(canonical_slice) == len(CANON_DESKTOP)
    for canonical, entry in zip(CANON_DESKTOP, canonical_slice):
        assert entry.get("id") == canonical["id"]
        assert entry.get("title") == canonical["title"]

    dynamic_titles = {
        entry.get("title")
        for entry in categories_payload[len(CANON_DESKTOP) :]
        if isinstance(entry, dict)
    }
    assert {"Taiko Towers", "Dan Dojo"}.issubset(dynamic_titles)

    modes_response = client.get("/api/modes")
    assert modes_response.status_code == 200
    manifest = modes_response.get_json()
    assert isinstance(manifest, dict)
    assert manifest.get("status") == "ok"
    modes_payload = manifest.get("modes") or []
    assert any(mode.get("key") == "tower" for mode in modes_payload if isinstance(mode, dict))
    assert any(mode.get("key") == "dandojo" for mode in modes_payload if isinstance(mode, dict))


def test_desktop_song_static_route_validates_song_id(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    assert song_store is not None

    songs_dir_override = tmp_path / "songs_override"
    songs_dir_override.mkdir()
    app_module.DESKTOP_SONGS_DIR = songs_dir_override
    if hasattr(app_module, "SONGS_DIR_PATH"):
        app_module.SONGS_DIR_PATH = songs_dir_override

    now = int(time.time())
    song_store.upsert_many(
        [
            {
                "song_id": "static-001",
                "scanner_stable_id": "static-001",
                "group_key": "group::static-001",
                "title": "Static Song",
                "paths": {"dir_url": "/songs/static-001/"},
                "updated_at": now,
                "created_at": now,
            }
        ]
    )

    song_dir = app_module.DESKTOP_SONGS_DIR / "static-001"
    song_dir.mkdir()
    (song_dir / "main.tja").write_text("# TJA")

    client = app_module.app.test_client()

    ok_response = client.get("/songs/static-001/main.tja")
    assert ok_response.status_code == 200

    missing_response = client.get("/songs/missing/main.tja")
    assert missing_response.status_code == 404

    directory_response = client.get("/songs/static-001/")
    assert directory_response.status_code == 404


def test_resolve_main_tja_path_prefers_manifest(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    manifest_store = app_module.MANIFEST_STORE
    songs_dir = tmp_path / "songs_manifest"
    songs_dir.mkdir()
    app_module.DESKTOP_SONGS_DIR = songs_dir
    if hasattr(app_module, "SONGS_DIR_PATH"):
        app_module.SONGS_DIR_PATH = songs_dir

    now = int(time.time())
    song_store.upsert_many(
        [
            {
                "song_id": "alpha-song",
                "scanner_stable_id": "alpha-stable",
                "group_key": "group::alpha",
                "title": "Alpha",
                "paths": {"dir_url": "/songs/AlphaPack/"},
                "updated_at": now,
                "created_at": now,
            }
        ]
    )
    manifest_store.put("alpha-stable", {"file_path": "AlphaPack/main-alpha.tja"})

    pack_dir = songs_dir / "AlphaPack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    target_path = pack_dir / "main-alpha.tja"
    target_path.write_text("#alpha")

    resolved = app_module.resolve_main_tja_path(
        "alpha-song",
        song_store=song_store,
        manifest_store=manifest_store,
        songs_dir=songs_dir,
    )
    assert resolved == target_path.resolve()


def test_resolve_main_tja_path_detects_main_case_insensitive(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    manifest_store = app_module.MANIFEST_STORE
    songs_dir = tmp_path / "songs_case"
    songs_dir.mkdir()
    app_module.DESKTOP_SONGS_DIR = songs_dir
    if hasattr(app_module, "SONGS_DIR_PATH"):
        app_module.SONGS_DIR_PATH = songs_dir

    now = int(time.time())
    song_store.upsert_many(
        [
            {
                "song_id": "beta-song",
                "scanner_stable_id": "beta-stable",
                "group_key": "group::beta",
                "title": "Beta",
                "paths": {"dir_url": "/songs/BetaPack/"},
                "updated_at": now,
                "created_at": now,
            }
        ]
    )
    manifest_store.put("beta-stable", {"paths": {"dir_url": "/songs/BetaPack/"}})

    pack_dir = songs_dir / "BetaPack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    main_path = pack_dir / "Main.TJA"
    main_path.write_text("#beta")
    (pack_dir / "extra.tja").write_text("#other")

    resolved = app_module.resolve_main_tja_path(
        "beta-song",
        song_store=song_store,
        manifest_store=manifest_store,
        songs_dir=songs_dir,
    )
    assert resolved == main_path.resolve()


def test_resolve_main_tja_path_missing_file(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    manifest_store = app_module.MANIFEST_STORE
    songs_dir = tmp_path / "songs_missing"
    songs_dir.mkdir()
    app_module.DESKTOP_SONGS_DIR = songs_dir
    if hasattr(app_module, "SONGS_DIR_PATH"):
        app_module.SONGS_DIR_PATH = songs_dir

    now = int(time.time())
    song_store.upsert_many(
        [
            {
                "song_id": "gamma-song",
                "scanner_stable_id": "gamma-stable",
                "group_key": "group::gamma",
                "title": "Gamma",
                "paths": {"dir_url": "/songs/GammaPack/"},
                "updated_at": now,
                "created_at": now,
            }
        ]
    )
    manifest_store.put("gamma-stable", {"paths": {"dir_url": "/songs/GammaPack/"}})

    with pytest.raises(FileNotFoundError):
        app_module.resolve_main_tja_path(
            "gamma-song",
            song_store=song_store,
            manifest_store=manifest_store,
            songs_dir=songs_dir,
        )


def test_desktop_scanner_populates_song_and_main_tja(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    manifest_store = app_module.MANIFEST_STORE
    songs_dir = tmp_path / "scanner_songs"
    songs_dir.mkdir()
    app_module.DESKTOP_SONGS_DIR = songs_dir
    if hasattr(app_module, "SONGS_DIR_PATH"):
        app_module.SONGS_DIR_PATH = songs_dir

    pack_dir = songs_dir / "ScannerPack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    tja_path = pack_dir / "SongName.tja"
    tja_path.write_text(
        "\n".join(
            [
                "TITLE:Scanner Song",
                "WAVE:SongName.ogg",
                "COURSE:Oni",
                "LEVEL:5",
                "#START",
                "1111,",
                "#END",
            ]
        ),
        encoding="utf-8",
    )
    audio_path = pack_dir / "SongName.ogg"
    audio_payload = b"\x11\x22"
    audio_path.write_bytes(audio_payload)

    tja_contents = tja_path.read_text(encoding="utf-8")
    wave_name = None
    for line in tja_contents.splitlines():
        if line.upper().startswith("WAVE:"):
            wave_name = line.split(":", 1)[1].strip()
            break
    assert wave_name

    scanner = SongScanner(
        db=_DummyDB(),
        songs_dir=songs_dir,
        songs_baseurl="/songs/",
        song_store=song_store,
        manifest_store=manifest_store,
    )

    summary_first = scanner.scan(full=True)
    metrics_first = summary_first.get('metrics') or {}
    assert metrics_first.get('songs_upserted_total', 0) >= 1
    assert summary_first.get('errors', 0) == 0

    stored_doc = song_store.find_one()
    assert stored_doc is not None
    song_identifier = stored_doc.get('song_id')
    assert isinstance(song_identifier, str)
    dir_path_value = stored_doc.get('dir_path')
    assert dir_path_value == str(pack_dir.resolve())
    assert stored_doc.get('tja_filename') == tja_path.name

    client = app_module.app.test_client()
    response = client.get(f"/songs/{song_identifier}/main.tja")
    assert response.status_code == 200
    assert response.data == tja_path.read_bytes()

    direct_response = client.get(f"/songs/{song_identifier}/SongName.tja")
    assert direct_response.status_code == 200
    assert direct_response.data == tja_path.read_bytes()

    audio_response = client.get(f"/songs/{song_identifier}/SongName.ogg")
    assert audio_response.status_code == 200
    assert audio_response.data == audio_payload

    missing_response = client.get(f"/songs/{song_identifier}/nope.tja")
    assert missing_response.status_code == 404

    api_response = client.get("/api/songs?limit=5")
    assert api_response.status_code == 200
    payload = api_response.get_json()
    assert isinstance(payload, list)
    assert payload
    entry_by_id = None
    for entry in payload:
        assert isinstance(entry, dict)
        entry_id = entry.get('id')
        assert isinstance(entry_id, str) and entry_id.strip()
        if entry_id == song_identifier:
            entry_by_id = entry
        song_response = client.get(f"/songs/{entry_id}/main.tja")
        assert song_response.status_code == 200
    first_entry = payload[0]
    assert isinstance(first_entry.get('id'), str)
    assert isinstance(first_entry.get('url'), str)
    assert first_entry['url'].startswith('/songs/')
    assert first_entry['url'].endswith('/main.tja')
    assert isinstance(first_entry.get('category'), str)
    assert isinstance(first_entry.get('category_id'), int)
    assert entry_by_id is not None
    assert entry_by_id['id'] == song_identifier
    entry_url = entry_by_id.get('url')
    assert isinstance(entry_url, str) and entry_url == f"/songs/{song_identifier}/main.tja"
    assert isinstance(entry_by_id.get('category'), str)
    assert isinstance(entry_by_id.get('category_id'), int)
    paths_value = entry_by_id.get('paths')
    assert isinstance(paths_value, dict)
    assert set(paths_value).issubset({'tja_url', 'audio_url', 'dir_url'})
    assert paths_value.get('tja_url') == entry_url
    assert paths_value.get('dir_url') == f"/songs/{song_identifier}/"
    if 'audio_url' in paths_value:
        assert paths_value['audio_url'] == f"/songs/{song_identifier}/{wave_name}"

    summary_second = scanner.scan(full=True)
    assert summary_second.get('errors', 0) == 0
    assert summary_second.get('updated', 0) >= 1

    wave_response = client.get(f"/songs/{song_identifier}/{wave_name}")
    assert wave_response.status_code == 200
    assert wave_response.data == audio_payload

    categories_response = client.get("/api/categories")
    assert categories_response.status_code == 200
    categories_payload = categories_response.get_json()
    assert isinstance(categories_payload, list)
    assert len(categories_payload) > 0


def test_desktop_scanner_handles_dojo_and_tower(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    song_store = app_module.SONG_STORE
    manifest_store = app_module.MANIFEST_STORE
    songs_dir = tmp_path / "dojo_songs"
    songs_dir.mkdir()
    app_module.DESKTOP_SONGS_DIR = songs_dir
    if hasattr(app_module, "SONGS_DIR_PATH"):
        app_module.SONGS_DIR_PATH = songs_dir

    dojo_dir = songs_dir / "DanDojo"
    dojo_dir.mkdir(parents=True, exist_ok=True)
    dojo_tja = dojo_dir / "Nijiiro 2022 Second Dan.tja"
    dojo_tja.write_text(
        "\n".join(
            [
                "TITLE:Dojo Challenge",
                "COURSE:Dan",
                "LEVEL:5",
                "WAVE:Nijiiro 2022 Second Dan.mp3",
                "#START",
                "1111,",
                "#END",
            ]
        ),
        encoding="utf-8",
    )
    (dojo_dir / "Nijiiro 2022 Second Dan.mp3").write_bytes(b"mp3")
    (dojo_dir / "Nijiiro 2022 Second Dan.t3u8").write_text("#EXTM3U\n", encoding="utf-8")

    tower_dir = songs_dir / "TowerPack"
    tower_dir.mkdir(parents=True, exist_ok=True)
    tower_tja = tower_dir / "Taiko Tower 3 Ama-kuchi.tja"
    tower_tja.write_text(
        "\n".join(
            [
                "TITLE:Tower Trial",
                "COURSE:Tower Floor 1",
                "LEVEL:4",
                "WAVE:Taiko Tower 3 Ama-kuchi.mp3",
                "#START",
                "1111,",
                "#END",
            ]
        ),
        encoding="utf-8",
    )
    (tower_dir / "Taiko Tower 3 Ama-kuchi.mp3").write_bytes(b"mp3")
    hls_dir = tower_dir / "HLS"
    hls_dir.mkdir()
    (hls_dir / "playlist.m3u8").write_text("#EXTM3U\n", encoding="utf-8")

    scanner = SongScanner(
        db=_DummyDB(),
        songs_dir=songs_dir,
        songs_baseurl="/songs/",
        song_store=song_store,
        manifest_store=manifest_store,
    )

    summary = scanner.scan(full=True)
    assert summary.get('errors', 0) == 0

    docs = list(song_store.find())
    assert len(docs) >= 2

    dojo_doc = next(doc for doc in docs if doc.get('title') == 'Dojo Challenge')
    tower_doc = next(doc for doc in docs if doc.get('title') == 'Tower Trial')

    assert dojo_doc.get('tja_filename') == dojo_tja.name
    dojo_assets = dojo_doc.get('assets') or {}
    assert isinstance(dojo_assets, dict)
    assert dojo_assets.get('tja_main')
    assert dojo_assets['tja_main'].endswith(dojo_tja.name)
    dojo_files = dojo_assets.get('files') or {}
    assert isinstance(dojo_files, dict)
    assert any(
        (isinstance(key, str) and key.endswith('.t3u8'))
        or (isinstance(value, str) and value.endswith('.t3u8'))
        for key, value in dojo_files.items()
    )

    assert tower_doc.get('tja_filename') == tower_tja.name
    tower_assets = tower_doc.get('assets') or {}
    assert isinstance(tower_assets, dict)
    assert tower_assets.get('tja_main')
    assert tower_assets['tja_main'].endswith(tower_tja.name)
    tower_files = tower_assets.get('files') or {}
    assert isinstance(tower_files, dict)
    assert any(
        (isinstance(key, str) and key.endswith('.m3u8'))
        or (isinstance(value, str) and value.endswith('.m3u8'))
        for key, value in tower_files.items()
    )

    dojo_id = dojo_doc.get('song_id')
    assert isinstance(dojo_id, str)

    song_store.update_one(
        {'song_id': dojo_id},
        {
            '$set': {
                'assets': {},
                'tja_path': None,
                'tja_filename': None,
            }
        },
    )

    resolved_main = app_module.resolve_main_tja_path(
        dojo_id,
        song_store=song_store,
        manifest_store=manifest_store,
        songs_dir=songs_dir,
    )
    assert resolved_main == dojo_tja.resolve()

    resolved_alias = app_module.resolve_song_file_path(
        dojo_id,
        'main.tja',
        song_store=song_store,
        manifest_store=manifest_store,
        songs_dir=songs_dir,
    )
    assert resolved_alias == dojo_tja.resolve()

    updated_doc = song_store.find_one({'song_id': dojo_id})
    assert isinstance(updated_doc, dict)
    updated_assets = updated_doc.get('assets') or {}
    assert updated_assets.get('tja_main')
    assert updated_assets['tja_main'].endswith(dojo_tja.name)
    assert updated_doc.get('tja_path')
    assert updated_doc.get('tja_filename') == dojo_tja.name


def test_desktop_api_login_guarded(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.post("/api/login", json={"username": "user", "password": "pass"})
    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "error"
    assert payload["message"] == "desktop_profile_feature_unavailable"
