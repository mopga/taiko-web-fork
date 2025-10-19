"""Helpers for selecting tower/dan charts for REST fallbacks."""

from __future__ import annotations

from typing import Mapping, Optional, Sequence

_TOWER_MODES = {"tower", "dan"}


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
    course: str,
) -> Optional[Mapping[str, object]]:
    """Return the preferred chart for the provided course token."""

    if not charts:
        return None

    course_token = (course or "").strip().casefold()
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
        priority = (0 if mode_value in _TOWER_MODES else 1, index)
        if priority < best_priority:
            best_priority = priority
            best_chart = chart

    if best_chart is not None:
        return best_chart

    for chart in charts:
        if not isinstance(chart, Mapping):
            continue
        mode_value = str(chart.get("mode") or "").strip().casefold()
        if mode_value in _TOWER_MODES:
            return chart

    return None


__all__ = ["normalise_course_tokens", "select_best_chart"]
