"""Helpers for selecting tower/dan charts for REST fallbacks."""

from __future__ import annotations

from typing import Mapping, Optional, Sequence


def _canonical_mode_token(mode: object) -> str:
    if not isinstance(mode, str):
        return ""
    token = mode.strip().casefold()
    if token in {"dan", "dojo"}:
        return "dandojo"
    return token


def normalise_course_tokens(chart: Mapping[str, object]) -> set[str]:
    tokens: set[str] = set()
    for key in ("course", "canonical_course", "display_course", "raw_course"):
        value = chart.get(key)
        if isinstance(value, str) and value:
            tokens.add(value.strip().casefold())
    mode_value = _canonical_mode_token(chart.get("mode"))
    if mode_value:
        tokens.add(mode_value)
    rank_value = chart.get("rank")
    if isinstance(rank_value, str) and rank_value.strip():
        tokens.add(rank_value.strip().casefold())
    elif isinstance(rank_value, (int, float)):
        tokens.add(str(rank_value).strip().casefold())
    return {token for token in tokens if token}


def select_best_chart(
    charts: Optional[Sequence[Mapping[str, object]]],
    course: Optional[str] = None,
    prefer_modes: Sequence[object] = ("tower", "dandojo"),
) -> Optional[Mapping[str, object]]:
    """Return the preferred chart for the provided course token."""

    if not charts:
        return None

    course_token = (course or "").strip().casefold()
    prefer_mode_tokens: tuple[str, ...] = tuple(
        token
        for token in (
            _canonical_mode_token(mode) for mode in (prefer_modes or ())
        )
        if token
    )

    best_chart: Optional[Mapping[str, object]] = None
    best_priority: Optional[tuple[int, int, int]] = None

    prefer_mode_order = {
        token: idx for idx, token in enumerate(prefer_mode_tokens)
    }

    def _score_chart(index: int, chart: Mapping[str, object]) -> tuple[int, int, int]:
        mode_value = _canonical_mode_token(chart.get("mode"))
        mode_priority = prefer_mode_order.get(mode_value, len(prefer_mode_tokens))

        score = 0
        if course_token:
            display_course = str(chart.get("display_course") or "").strip().casefold()
            canonical_course = str(chart.get("canonical_course") or "").strip().casefold()
            if display_course == course_token:
                score -= 3
            if canonical_course == course_token:
                score -= 2
        return (mode_priority, score, index)

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
