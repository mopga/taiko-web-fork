import sys
import unittest
from pathlib import Path
from typing import Optional
from unittest import mock

sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[1]))

import test_app_songs_api as songs_api_tests

taiko_app = songs_api_tests.taiko_app


class _StubManifestStore:
    def __init__(self, *, result=None, error: Optional[Exception] = None):
        self.result = result
        self.error = error
        self.calls: list[dict] = []

    def find_one(self, filter=None, projection=None, *args, **kwargs):  # pragma: no cover - signature compatibility
        self.calls.append({'filter': filter, 'projection': projection})
        if self.error is not None:
            raise self.error
        return self.result


class LoadManifestMetaTests(unittest.TestCase):
    def test_returns_manifest_meta_dict_when_available(self):
        meta = {'_id': '__meta__', 'manifest_checksum': 'abc123'}
        store = _StubManifestStore(result=meta)
        with mock.patch.object(taiko_app, '_get_manifest_store', return_value=store):
            result = taiko_app._load_manifest_meta()
        self.assertEqual({'_id': '__meta__', 'manifest_checksum': 'abc123'}, result)
        self.assertIsNot(meta, result)
        self.assertEqual([{'filter': {'_id': '__meta__'}, 'projection': None}], store.calls)

    def test_returns_none_when_store_returns_none(self):
        store = _StubManifestStore(result=None)
        with mock.patch.object(taiko_app, '_get_manifest_store', return_value=store):
            result = taiko_app._load_manifest_meta()
        self.assertIsNone(result)
        self.assertEqual([{'filter': {'_id': '__meta__'}, 'projection': None}], store.calls)

    def test_returns_none_when_store_raises(self):
        store = _StubManifestStore(error=RuntimeError('temporary error'))
        with mock.patch.object(taiko_app, '_get_manifest_store', return_value=store):
            result = taiko_app._load_manifest_meta()
        self.assertIsNone(result)
        self.assertEqual([{'filter': {'_id': '__meta__'}, 'projection': None}], store.calls)


if __name__ == '__main__':
    unittest.main()
