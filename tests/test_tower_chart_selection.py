import pytest

from tower_chart_selection import normalise_course_tokens, select_best_chart
from tests._helpers import load_app_module


def test_select_best_chart_prefers_tower_over_standard():
    charts = [
        {"course": "Oni", "mode": "standard", "display_course": "oni"},
        {"course": "Oni", "mode": "tower", "display_course": "tower", "name": "Tower A"},
    ]

    best_chart = select_best_chart(charts, "oni")

    assert best_chart is charts[1]


def test_select_best_chart_chooses_first_tower_when_multiple():
    charts = [
        {"course": "Oni", "mode": "standard", "display_course": "oni"},
        {"course": "Oni", "mode": "tower", "display_course": "tower", "name": "Tower A"},
        {"course": "Oni", "mode": "tower", "display_course": "tower", "name": "Tower B"},
    ]

    best_chart = select_best_chart(charts, "oni")

    assert best_chart is charts[1]


def test_select_best_chart_allows_custom_prefer_modes():
    charts = [
        {"course": "Oni", "mode": "standard", "display_course": "oni"},
        {"course": "Oni", "mode": "tower", "display_course": "tower"},
        {"course": "Oni", "mode": "dandojo", "display_course": "dandojo"},
    ]

    best_chart = select_best_chart(charts, "oni", prefer_modes=("dandojo",))

    assert best_chart is charts[2]


def test_select_best_chart_respects_prefer_modes_order():
    charts = [
        {"course": "Oni", "mode": "tower", "display_course": "tower"},
        {"course": "Oni", "mode": "dandojo", "display_course": "oni"},
    ]

    best_chart = select_best_chart(charts, "oni", prefer_modes=("dandojo", "tower"))

    assert best_chart is charts[1]


def test_select_best_chart_handles_empty_prefer_modes():
    charts = [
        {"course": "Oni", "mode": "standard", "display_course": "oni"},
        {"course": "Oni", "mode": "tower", "display_course": "tower"},
    ]

    best_chart = select_best_chart(charts, "oni", prefer_modes=[])

    assert best_chart is charts[0]


def test_select_best_chart_prefers_display_course_match():
    charts = [
        {"course": "Oni", "mode": "tower", "display_course": "oni"},
        {"course": "Oni", "mode": "tower", "display_course": "tower", "canonical_course": "oni"},
    ]

    best_chart = select_best_chart(charts, "oni")

    assert best_chart is charts[0]


def test_select_best_chart_uses_preferred_mode_when_no_course_match():
    charts = [
        {"course": "Hard", "mode": "standard"},
        {"course": "Hard", "mode": "tower"},
    ]

    best_chart = select_best_chart(charts, "oni")

    assert best_chart is charts[1]


def test_select_best_chart_prefers_chart_mode_token():
    charts = [
        {"course": "Oni", "mode": "standard", "chart_mode": "tower", "display_course": "tower"},
        {"course": "Oni", "mode": "standard", "display_course": "oni"},
    ]

    best_chart = select_best_chart(charts, "tower")

    assert best_chart is charts[0]


def test_select_best_chart_prefers_matching_rank_for_dandojo():
    charts = [
        {
            "mode": "dandojo",
            "course": "Kaiden",
            "canonical_course": "kaiden",
            "rank": "tatsujin",
        },
        {
            "mode": "dandojo",
            "course": "Kaiden",
            "canonical_course": "kaiden",
            "rank": "kaiden",
        },
    ]

    best_chart = select_best_chart(charts, "kaiden")

    assert best_chart is charts[1]


def test_normalise_course_tokens_includes_numeric_aliases():
    chart = {
        "course": "Oni",
        "display_course": "Tower",
        "raw_course": "Tower Floor 1",
        "chart_mode": "tower",
    }

    tokens = normalise_course_tokens(chart)

    assert "oni" in tokens
    assert "1" in tokens


def test_normalise_course_tokens_handles_special_aliases():
    chart = {
        "course": "Ura",
        "display_course": "Kara-kuchi",
        "raw_course": "Tower Floor 2",
        "mode": "tower",
    }

    tokens = normalise_course_tokens(chart)

    assert "ura" in tokens
    assert "2" in tokens
    assert "karakuchi" in tokens


def test_select_best_chart_supports_numeric_course_token():
    charts = [
        {"course": "Oni", "mode": "tower", "display_course": "tower"},
        {"course": "Ura", "mode": "tower", "display_course": "tower"},
    ]

    best_chart = select_best_chart(charts, "1")

    assert best_chart is charts[0]


@pytest.mark.parametrize('mode_query', ["", "&mode=tower"])
def test_tower_chart_route_falls_back_to_standard_chart(monkeypatch, mode_query):
    app_module = load_app_module()

    monkeypatch.setattr(app_module, 'RUN_PROFILE', 'web')

    fallback_chart = {
        'mode': 'tower',
        'course': 'oni',
        'display_course': 'Oni',
        'chart_data': {
            'duration_ms': 1200,
            'measures': [{'notes': [{'type': 'don', 'time': 0}]}],
        },
    }
    candidate_entry = {'id': 'tower-song', 'title': 'Tower Song', 'charts': []}
    candidate_song = {'title': 'Tower Song', 'charts': [fallback_chart]}

    def _fake_lookup(title):
        assert title
        return [(0, candidate_entry, candidate_song)]

    def _fake_resolve(song, entry):
        assert song is candidate_song
        assert entry is candidate_entry
        return list(song.get('charts', []))

    call_sequences: list[tuple[object, ...]] = []

    def _fake_select(charts, course, prefer_modes=("tower", "dandojo")):
        call_sequences.append(tuple(prefer_modes or ()))
        if prefer_modes:
            return None
        return charts[0] if charts else None

    monkeypatch.setattr(app_module, '_lookup_song_candidates_by_title', _fake_lookup)
    monkeypatch.setattr(app_module, '_resolve_song_charts', _fake_resolve)
    monkeypatch.setattr(app_module, 'select_best_chart', _fake_select)

    client = app_module.app.test_client()
    response = client.get(f'/api/tower/chart?title=Tower+Song&course=oni{mode_query}')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'ok'
    assert isinstance(payload['chart_data']['measures'], list)
    assert payload['mode'] == fallback_chart['mode']
    # Ensure the initial tower lookup was attempted before falling back to standard charts.
    assert any(modes for modes in call_sequences)
    assert tuple() in call_sequences


def test_tower_chart_route_prefers_standard_when_playlist_missing(monkeypatch):
    app_module = load_app_module()

    monkeypatch.setattr(app_module, 'RUN_PROFILE', 'web')

    tower_chart = {
        'mode': 'tower',
        'course': 'tower',
        'display_course': 'Tower',
        'chart_data': {
            'duration_ms': 0,
            'measures': [],
            'meta': {},
        },
    }
    standard_chart = {
        'mode': 'standard',
        'course': 'oni',
        'display_course': 'Oni',
        'chart_data': {
            'duration_ms': 1234,
            'measures': [{'notes': [{'time': 0, 'type': 'don'}]}],
        },
    }
    candidate_entry = {'id': 'tower-song', 'title': 'Tower Song'}
    candidate_song = {'title': 'Tower Song', 'charts': [tower_chart, standard_chart]}

    def _fake_lookup(title):
        assert title
        return [(0, candidate_entry, candidate_song)]

    def _fake_resolve(song, entry):
        assert song is candidate_song
        assert entry is candidate_entry
        return list(song.get('charts', []))

    def _fake_select(charts, course, prefer_modes=("tower", "dandojo")):
        if prefer_modes == ('standard',):
            return standard_chart
        return tower_chart

    monkeypatch.setattr(app_module, '_lookup_song_candidates_by_title', _fake_lookup)
    monkeypatch.setattr(app_module, '_resolve_song_charts', _fake_resolve)
    monkeypatch.setattr(app_module, 'select_best_chart', _fake_select)

    client = app_module.app.test_client()
    response = client.get('/api/tower/chart?title=Tower+Song&course=tower&mode=tower')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'ok'
    assert payload['mode'] == 'standard'
    measures = payload['chart_data']['measures']
    assert isinstance(measures, list)
    assert measures
    assert any(isinstance(m.get('notes'), list) and m['notes'] for m in measures if isinstance(m, dict))
