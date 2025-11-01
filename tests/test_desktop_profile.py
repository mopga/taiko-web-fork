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
    titles = {item.get("title") for item in categories_payload if isinstance(item, dict)}
    assert "Taiko Towers" in titles
    assert "Dan Dojo" in titles

    modes_response = client.get("/api/modes")
    assert modes_response.status_code == 200
    manifest = modes_response.get_json()
    assert isinstance(manifest, dict)
    assert manifest.get("status") == "ok"
    modes_payload = manifest.get("modes") or []
    assert any(
        mode.get("key") == "tower" and "Taiko Towers" in (mode.get("categories") or [])
        for mode in modes_payload
        if isinstance(mode, dict)
    )
    assert any(
        mode.get("key") == "dandojo" and "Dan Dojo" in (mode.get("categories") or [])
        for mode in modes_payload
        if isinstance(mode, dict)
    )


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
    tja_path = pack_dir / "CustomName.tja"
    tja_path.write_text(
        "\n".join(
            [
                "TITLE:Scanner Song",
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
    (pack_dir / "main.ogg").write_bytes(b"\x00\x00")

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

    client = app_module.app.test_client()
    response = client.get(f"/songs/{song_identifier}/main.tja")
    assert response.status_code == 200
    assert response.data == tja_path.read_bytes()

    direct_response = client.get(f"/songs/{song_identifier}/CustomName.tja")
    assert direct_response.status_code == 200
    assert direct_response.data == tja_path.read_bytes()

    missing_response = client.get(f"/songs/{song_identifier}/nope.tja")
    assert missing_response.status_code == 404

    api_response = client.get("/api/songs?limit=5")
    assert api_response.status_code == 200
    payload = api_response.get_json()
    assert isinstance(payload, list)
    assert payload
    for entry in payload:
        assert isinstance(entry, dict)
        entry_id = entry.get('id')
        assert isinstance(entry_id, str) and entry_id.strip()
        song_response = client.get(f"/songs/{entry_id}/main.tja")
        assert song_response.status_code == 200

    summary_second = scanner.scan(full=True)
    assert summary_second.get('errors', 0) == 0
    assert summary_second.get('updated', 0) >= 1


def test_desktop_api_login_guarded(tmp_path, monkeypatch):
    app_module = _import_desktop_app(monkeypatch, tmp_path)
    client = app_module.app.test_client()
    response = client.post("/api/login", json={"username": "user", "password": "pass"})
    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "error"
    assert payload["message"] == "desktop_profile_feature_unavailable"
