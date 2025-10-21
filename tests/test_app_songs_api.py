import json
import unittest
from pathlib import Path
from unittest import mock

import sys
import types

class _StubCollection:
    def create_index(self, *args, **kwargs):
        return None

    def drop_index(self, *args, **kwargs):
        return None

    def update_one(self, *args, **kwargs):
        return None


class _StubDatabase:
    def __init__(self):
        self.users = _StubCollection()
        self.songs = _StubCollection()
        self.scores = _StubCollection()
        self.song_scanner_state = _StubCollection()
        self.counters = _StubCollection()


class _StubMongoClient:
    def __init__(self, *args, **kwargs):
        self._db = _StubDatabase()

    def __getitem__(self, name):
        return self._db


class _StubRedis:
    def __init__(self, *args, **kwargs):
        pass

class _StubCache:
    def __init__(self, *args, **kwargs):
        pass

    def init_app(self, *args, **kwargs):
        pass

    def cached(self, *args, **kwargs):
        def _decorator(func):
            return func
        return _decorator

class _StubSession:
    def __init__(self, *args, **kwargs):
        pass

    def init_app(self, *args, **kwargs):
        pass

sys.modules.setdefault('redis', types.SimpleNamespace(Redis=_StubRedis))
sys.modules.setdefault('flask_caching', types.SimpleNamespace(Cache=_StubCache))
sys.modules.setdefault('flask_session', types.SimpleNamespace(Session=_StubSession))

with mock.patch('pymongo.MongoClient', new=_StubMongoClient):
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    import app as taiko_app

sys.path.append(str(Path(__file__).resolve().parents[1]))

import app as taiko_app


class _ManifestCursor:
    def __init__(self, docs, projection):
        self._docs = [self._project(doc, projection) for doc in docs]

    def _project(self, doc, projection):
        if not projection:
            return dict(doc)
        output = {}
        for key, enabled in projection.items():
            if not enabled:
                continue
            output[key] = doc.get(key)
        return output

    def sort(self, key, direction):
        reverse = direction < 0
        self._docs.sort(key=lambda doc: doc.get(key), reverse=reverse)
        return self

    def skip(self, amount):
        self._docs = self._docs[amount:]
        return self

    def limit(self, amount):
        if amount >= 0:
            self._docs = self._docs[:amount]
        return self

    def __iter__(self):
        return iter(self._docs)


class _ManifestCollection:
    def __init__(self, entries, meta):
        self._entries = entries
        self._meta = meta

    def find_one(self, filter_, projection=None):
        if filter_ == {'_id': '__meta__'}:
            return dict(self._meta)
        target_id = filter_.get('_id') if isinstance(filter_, dict) else None
        if target_id:
            for entry in self._entries:
                if entry.get('_id') == target_id:
                    return entry
        return None

    def find(self, filter_, projection=None):
        results = [entry for entry in self._entries if entry.get('_id') != '__meta__']
        return _ManifestCursor(results, projection or {})


class _SongsCollection:
    def __init__(self, docs):
        self._docs = {doc.get('scanner_stable_id'): dict(doc) for doc in docs}

    class _Cursor:
        def __init__(self, docs):
            self._docs = docs

        def sort(self, spec):
            if isinstance(spec, list):
                for key, direction in reversed(spec):
                    reverse = direction < 0
                    self._docs.sort(key=lambda doc: doc.get(key), reverse=reverse)
            return self

        def skip(self, amount):
            if isinstance(amount, int) and amount > 0:
                self._docs = self._docs[amount:]
            return self

        def limit(self, amount):
            if isinstance(amount, int) and amount >= 0:
                self._docs = self._docs[:amount]
            return self

        def __iter__(self):
            return iter(self._docs)

    def find_one(self, filter_, projection=None):
        stable_id = filter_.get('scanner_stable_id') if isinstance(filter_, dict) else None
        doc = self._docs.get(stable_id)
        if doc is None:
            return None
        if not projection:
            return dict(doc)
        projected = {}
        for key, enabled in projection.items():
            if enabled:
                if key == '_id':
                    continue
                projected[key] = doc.get(key)
        return projected

    def find(self, filter_, projection=None):
        ids = filter_.get('scanner_stable_id', {}).get('$in', []) if isinstance(filter_, dict) else []
        results = []
        for stable_id in ids:
            doc = self._docs.get(stable_id)
            if not doc:
                continue
            if projection:
                projected = {}
                for key, enabled in projection.items():
                    if enabled and key != '_id':
                        projected[key] = doc.get(key)
                results.append(projected)
            else:
                results.append(dict(doc))
        return self._Cursor(results)


class SongsApiTestCase(unittest.TestCase):
    def setUp(self):
        self.app = taiko_app.app
        self.client = self.app.test_client()

    def _patch_collections(self, manifest_entries, manifest_meta, songs_docs):
        manifest_collection = _ManifestCollection(manifest_entries, manifest_meta)
        songs_collection = _SongsCollection(songs_docs)
        return mock.patch.multiple(
            taiko_app,
            _get_manifest_collection=mock.Mock(return_value=manifest_collection),
            db=mock.Mock(songs=songs_collection),
        )

    def test_api_songs_etag_304(self):
        manifest_entries = [
            {
                '_id': 'song-1',
                'id': 'song-1',
                'title': 'Song One',
                'title_lc': 'song one',
                'subtitle': '',
                'category': 'General',
                'difficulties': {'easy': False, 'normal': True, 'hard': False, 'oni': False, 'ura': False},
                'duration_ms': 1234,
                'preview_available': True,
                'source_type': 'tja',
                'paths': {},
            },
        ]
        manifest_meta = {'_id': '__meta__', 'manifest_checksum': 'abc123', 'count': 1}
        songs_docs = [
            {
                'scanner_stable_id': 'song-1',
                'id': 1,
                'title': 'Song One',
                'subtitle': '',
                'subtitleJa': '',
                'titleJa': '',
                'category': 'General',
                'preview': 0,
                'music_type': 'ogg',
                'type': 'tja',
                'paths': {},
                'courses': {},
                'import_issues': [],
                'valid_chart_count': 0,
                'charts': [],
                'hash': 'hash',
                'fingerprint': 'fp',
            }
        ]

        with self._patch_collections(manifest_entries, manifest_meta, songs_docs):
            first_response = self.client.get('/api/songs')
            self.assertEqual(first_response.status_code, 200)
            self.assertIn('ETag', first_response.headers)
            self.assertIn('Cache-Control', first_response.headers)
            self.assertIn('Vary', first_response.headers)
            etag_value = first_response.headers['ETag']
            self.assertTrue(etag_value)

            second_response = self.client.get('/api/songs', headers={'If-None-Match': etag_value})
            self.assertEqual(second_response.status_code, 304)
            self.assertEqual(second_response.data, b'')
            self.assertEqual(second_response.headers.get('ETag'), etag_value)
            self.assertEqual(
                second_response.headers.get('Cache-Control'),
                'public, max-age=86400, stale-while-revalidate=600',
            )
            vary_header = second_response.headers.get('Vary', '')
            vary_tokens = {token.strip() for token in vary_header.split(',') if token.strip()}
            self.assertIn('If-None-Match', vary_tokens)
            self.assertIn('Accept-Encoding', vary_tokens)

    def test_songs_etag_changes_after_manifest_update(self):
        manifest_entries = [
            {
                '_id': 'song-1',
                'id': 'song-1',
                'title': 'Song One',
                'title_lc': 'song one',
                'subtitle': '',
                'category': 'General',
                'difficulties': {'easy': False, 'normal': True, 'hard': False, 'oni': False, 'ura': False},
                'duration_ms': 1234,
                'preview_available': True,
                'source_type': 'tja',
                'paths': {},
            },
        ]
        manifest_meta = {'_id': '__meta__', 'manifest_checksum': 'abc123', 'count': 1}
        songs_docs = [
            {
                'scanner_stable_id': 'song-1',
                'id': 1,
                'title': 'Song One',
                'subtitle': '',
                'subtitleJa': '',
                'titleJa': '',
                'category': 'General',
                'preview': 0,
                'music_type': 'ogg',
                'type': 'tja',
                'paths': {},
                'courses': {},
                'import_issues': [],
                'valid_chart_count': 0,
                'charts': [],
                'hash': 'hash',
                'fingerprint': 'fp',
            }
        ]

        with self._patch_collections(manifest_entries, manifest_meta, songs_docs):
            first_response = self.client.get('/api/songs')
            self.assertEqual(first_response.status_code, 200)
            first_etag = first_response.headers['ETag']

            manifest_meta['manifest_checksum'] = 'def456'
            manifest_entries[0]['duration_ms'] = 4321

            second_response = self.client.get('/api/songs', headers={'If-None-Match': first_etag})
            self.assertEqual(second_response.status_code, 200)
            second_etag = second_response.headers['ETag']
            self.assertNotEqual(first_etag, second_etag)

    def test_details_notes_none_order(self):
        manifest_entries = []
        manifest_meta = {'_id': '__meta__', 'manifest_checksum': 'aaa', 'count': 2}
        songs_docs = [
            {
                'scanner_stable_id': 'song-1',
                'id': 1,
                'title': 'Song One',
                'subtitle': '',
                'subtitleJa': '',
                'titleJa': '',
                'category': 'General',
                'preview': 0,
                'music_type': 'ogg',
                'type': 'tja',
                'paths': {},
                'courses': {},
                'import_issues': [],
                'valid_chart_count': 1,
                'charts': [
                    {
                        'course': 'oni',
                        'canonical_course': 'oni',
                        'mode': 'standard',
                        'display_course': 'Oni',
                        'level': 5,
                        'branch': False,
                        'valid': True,
                        'issues': [],
                        'total_notes': 10,
                        'tja_path': 'song-1.tja',
                        'rank': None,
                        'tja_url': '/song-1.tja',
                        'chart_data': {'duration_ms': 1000, 'measures': []},
                    }
                ],
                'hash': 'hash1',
                'fingerprint': 'fp1',
            },
            {
                'scanner_stable_id': 'song-2',
                'id': 2,
                'title': 'Song Two',
                'subtitle': '',
                'subtitleJa': '',
                'titleJa': '',
                'category': 'General',
                'preview': 0,
                'music_type': 'ogg',
                'type': 'tja',
                'paths': {},
                'courses': {},
                'import_issues': [],
                'valid_chart_count': 0,
                'charts': [],
                'hash': 'hash2',
                'fingerprint': 'fp2',
            },
        ]

        with self._patch_collections(manifest_entries, manifest_meta, songs_docs):
            response = self.client.get('/api/songs/details?ids=song-2,song-1&notes=none')
            self.assertEqual(response.status_code, 200)
            payload = json.loads(response.data.decode('utf-8'))
            self.assertEqual(len(payload), 2)
            self.assertEqual(payload[0]['id'], 'song-2')
            self.assertEqual(payload[1]['id'], 'song-1')
            charts = payload[1]['charts']
            self.assertTrue(isinstance(charts, list))
            for chart in charts:
                self.assertNotIn('chart_data', chart)

    def test_details_error_no_500(self):
        manifest_entries = []
        manifest_meta = {'_id': '__meta__', 'manifest_checksum': 'aaa', 'count': 0}

        class _FailingSongsCollection:
            def find(self, *args, **kwargs):  # pragma: no cover - behaviour validated via API
                raise RuntimeError('database temporarily unavailable')

        failing_db = types.SimpleNamespace(songs=_FailingSongsCollection())
        manifest_collection = _ManifestCollection(manifest_entries, manifest_meta)

        with mock.patch.multiple(
            taiko_app,
            _get_manifest_collection=mock.Mock(return_value=manifest_collection),
            db=failing_db,
        ):
            response = self.client.get('/api/songs/details?ids=song-1,song-2&notes=none')
            self.assertEqual(response.status_code, 400)
            payload = json.loads(response.data.decode('utf-8'))
            self.assertEqual(payload.get('error'), 'songs_details_failed')
            self.assertTrue(payload.get('reason'))

