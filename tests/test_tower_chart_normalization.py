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
    assert normalized[0]["notes"][0]["at"] == 0
    assert normalized[0]["longs"][0]["at"] == 100
    assert normalized[0]["longs"][0]["end_at"] == 500


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
    assert normalized[0]["longs"][0]["at"] == 0
    assert normalized[0]["longs"][0]["end_at"] == 600
    assert normalized[0]["notes"] == []
