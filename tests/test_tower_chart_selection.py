from tower_chart_selection import select_best_chart


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
