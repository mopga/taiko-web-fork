"""Helpers for selecting tower/dan charts for REST fallbacks."""

from __future__ import annotations

from typing import Mapping, Optional, Sequence


def normalise_course_tokens(chart: Mapping[str, object]) -> set[str]:
    tokens: set[str] = set()
    for key in ("course", "canonical_course", "display_course", "raw_course"):
        value = chart.get(key)
        if isinstance(value, str) and value:
            tokens.add(value.strip().casefold())
    mode_value = chart.get("mode")
    if isinstance(mode_value, str) and mode_value:
        tokens.add(mode_value.strip().casefold())
    return {token for token in tokens if token}


def select_best_chart(
    charts: Optional[Sequence[Mapping[str, object]]],
    course: Optional[str] = None,
    prefer_modes: Sequence[object] = ("tower", "dan"),
) -> Optional[Mapping[str, object]]:
    """Return the preferred chart for the provided course token."""

    if not charts:
        return None

    course_token = (course or "").strip().casefold()
    prefer_mode_tokens: tuple[str, ...] = tuple(
        token
        for token in (
            str(mode or "").strip().casefold()
            for mode in (prefer_modes or ())
        )
        if token
    )

    best_chart: Optional[Mapping[str, object]] = None
    best_priority: Optional[tuple[int, int]] = None

    def _score_chart(index: int, chart: Mapping[str, object]) -> tuple[int, int]:
        score = 0
        mode_value = str(chart.get("mode") or "").strip().casefold()
        if prefer_mode_tokens and mode_value in prefer_mode_tokens:
            score -= 10
        if course_token:
            display_course = str(chart.get("display_course") or "").strip().casefold()
            canonical_course = str(chart.get("canonical_course") or "").strip().casefold()
            if display_course == course_token:
                score -= 3
            if canonical_course == course_token:
                score -= 2
        return (score, index)

    filtered: list[tuple[int, Mapping[str, object]]] = []
    if course_token:
        for index, chart in enumerate(charts):
            if not isinstance(chart, Mapping):
                continue
            tokens = normalise_course_tokens(chart)
            if course_token not in tokens:
                continue
            filtered.append((index, chart))

    candidates = filtered if filtered else [
        (index, chart)
        for index, chart in enumerate(charts)
        if isinstance(chart, Mapping)
    ]

    for index, chart in candidates:
        priority = _score_chart(index, chart)
        if best_priority is None or priority < best_priority:
            best_priority = priority
            best_chart = chart

    return best_chart


__all__ = ["normalise_course_tokens", "select_best_chart"]
