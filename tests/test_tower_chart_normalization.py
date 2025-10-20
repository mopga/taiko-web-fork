from tower_chart_normalization import normalize_measures_relative


def test_normalize_longs_relative_to_measure_start():
    measures = [
        {
            "bpm": 120,
            "notes": [{"at": 1000, "type": "don"}],
            "longs": [{"at": 1100, "end_at": 1500, "type": "drumroll"}],
        }
    ]

    normalized = normalize_measures_relative(measures)

    assert normalized[0]["start_ms"] == 1000
    note = normalized[0]["notes"][0]
    assert note["at"] == 0
    assert note["offset"] == 0
    assert note["p"] == 0.0
    assert note["kind"] == 1
    long_note = normalized[0]["longs"][0]
    assert long_note["at"] == 100
    assert long_note["end_at"] == 500
    assert long_note["len_ms"] == 400


def test_normalize_longs_without_notes_uses_long_start():
    measures = [
        {
            "bpm": 150,
            "notes": [],
            "longs": [{"at": 2400, "end_at": 3000, "type": "balloon"}],
        }
    ]

    normalized = normalize_measures_relative(measures)

    assert normalized[0]["start_ms"] == 2400
    assert normalized[0]["duration_ms"] == int(round(4 * (60000 / 150)))
    long_note = normalized[0]["longs"][0]
    assert long_note["at"] == 0
    assert long_note["end_at"] == 600
    assert long_note["len_ms"] == 600
    assert normalized[0]["notes"] == []
