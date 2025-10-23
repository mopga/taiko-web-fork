import importlib
import sys
from unittest import mock

from redis import Redis


def test_session_redis_is_object(tmp_path, monkeypatch):
    monkeypatch.setenv('SCAN_ON_START', 'skip')
    monkeypatch.setenv('ENABLE_SONG_WATCHER', '0')
    monkeypatch.setenv('SONGS_DIR', str(tmp_path))
    monkeypatch.setenv('TAIKO_WEB_REDIS_HOST', 'localhost')
    monkeypatch.setenv('TAIKO_WEB_REDIS_PORT', '6379')
    monkeypatch.setenv('TAIKO_WEB_REDIS_DB', '0')
    monkeypatch.delenv('TAIKO_WEB_REDIS_PASSWORD', raising=False)
    monkeypatch.delenv('TAIKO_WEB_MONGO_URI', raising=False)
    monkeypatch.setenv('TAIKO_WEB_MONGO_HOST', 'localhost:27017')
    monkeypatch.setenv('TAIKO_WEB_MONGO_DB', 'taiko-test')

    fake_client = mock.MagicMock()
    fake_db = mock.MagicMock()
    fake_client.__getitem__.return_value = fake_db
    fake_client.address = ('localhost', 27017)

    sys.modules.pop('app', None)
    with mock.patch('pymongo.MongoClient', return_value=fake_client):
        with mock.patch('redis.Redis.ping', return_value=True):
            app_module = importlib.import_module('app')

    assert isinstance(app_module.app.config['SESSION_REDIS'], Redis)
