import types

from storage.sqlite_store import _recover_song_title
from songs_scanner import _resolve_song_title


def _make_record(title='', title_ja=None, locale=None):
    return types.SimpleNamespace(
        title=title,
        title_ja=title_ja,
        locale=locale or {},
        normalized_title='',
    )


def test_resolve_song_title_prefers_payload_title():
    document = {'title': 'Primary Title'}
    resolved, source = _resolve_song_title(document, [], 'audio:hash:folder')
    assert resolved == 'Primary Title'
    assert source == 'payload.title'


def test_resolve_song_title_uses_localised_payload():
    document = {'titles': {'en': 'Localized Title'}}
    resolved, source = _resolve_song_title(document, [], 'audio:hash:folder')
    assert resolved == 'Localized Title'
    assert source == 'payload.titles.en'


def test_resolve_song_title_falls_back_to_record():
    record = _make_record(title='Record Title')
    resolved, source = _resolve_song_title({}, [record], 'audio:hash:folder')
    assert resolved == 'Record Title'
    assert source == 'record.title'


def test_resolve_song_title_uses_group_key_tail():
    document = {}
    records = []
    group_key = 'audio:abc123:02 anime'
    resolved, source = _resolve_song_title(document, records, group_key)
    assert resolved == '02 anime'
    assert source == 'group_key'


def test_resolve_song_title_returns_none_when_unavailable():
    resolved, source = _resolve_song_title({}, [], None)
    assert resolved is None
    assert source is None


def test_recover_song_title_from_charts_meta():
    song = {
        'charts': [
            {'meta': {'title': 'Chart Meta Title'}},
        ]
    }
    recovered, source = _recover_song_title(song)
    assert recovered == 'Chart Meta Title'
    assert source == 'charts.meta.title'


def test_recover_song_title_from_group_key():
    song = {'group_key': 'audio:deadbeef:Sample Song'}
    recovered, source = _recover_song_title(song)
    assert recovered == 'Sample Song'
    assert source == 'group_key'
