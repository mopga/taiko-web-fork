import os
from types import SimpleNamespace
from unittest import mock

from tests._helpers import load_app_module

taiko_app = load_app_module()


def test_resolve_catalog_source_desktop_without_dsn():
    config_module = SimpleNamespace(MONGO={})
    with mock.patch.dict(os.environ, {}, clear=True):
        result = taiko_app._resolve_catalog_source(run_profile='desktop', config_module=config_module)
    assert result == 'sqlite'


def test_resolve_catalog_source_respects_env_override():
    config_module = SimpleNamespace(MONGO={})
    with mock.patch.dict(os.environ, {'CATALOG_SOURCE': 'mongo'}, clear=True):
        result = taiko_app._resolve_catalog_source(run_profile='desktop', config_module=config_module)
    assert result == 'mongo'


def test_resolve_catalog_source_legacy_env_false():
    config_module = SimpleNamespace(MONGO={})
    with mock.patch.dict(os.environ, {'USE_MONGO_CATALOG': '0'}, clear=True):
        result = taiko_app._resolve_catalog_source(run_profile='web', config_module=config_module)
    assert result == 'filesystem'


def test_resolve_catalog_source_web_defaults_to_mongo():
    config_module = SimpleNamespace(MONGO={})
    with mock.patch.dict(os.environ, {}, clear=True):
        result = taiko_app._resolve_catalog_source(run_profile='web', config_module=config_module)
    assert result == 'mongo'
