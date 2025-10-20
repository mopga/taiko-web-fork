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
    best_priority = (float("inf"), float("inf"))

    for index, chart in enumerate(charts):
        if not isinstance(chart, Mapping):
            continue
        if course_token:
            tokens = normalise_course_tokens(chart)
            if course_token not in tokens:
                continue
        mode_value = str(chart.get("mode") or "").strip().casefold()
        if prefer_mode_tokens:
            try:
                mode_priority = prefer_mode_tokens.index(mode_value)
            except ValueError:
                mode_priority = len(prefer_mode_tokens)
        else:
            mode_priority = 0
        priority = (mode_priority, index)
        if priority < best_priority:
            best_priority = priority
            best_chart = chart

    if best_chart is not None:
        return best_chart

    for chart in charts:
        if not isinstance(chart, Mapping):
            continue
        mode_value = str(chart.get("mode") or "").strip().casefold()
        if prefer_mode_tokens and mode_value in prefer_mode_tokens:
            return chart

    return None


__all__ = ["normalise_course_tokens", "select_best_chart"]
