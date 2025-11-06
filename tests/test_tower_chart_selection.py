from tower_chart_selection import normalise_course_tokens, select_best_chart


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
