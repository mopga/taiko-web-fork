"""Helpers for selecting tower/dan charts for REST fallbacks."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Mapping, Optional

_NUMERIC_ALIASES = {
    "easy": {"5"},
    "normal": {"4"},
    "hard": {"3"},
    "oni": {"1"},
    "ura": {"2"},
}

_SPECIAL_ALIASES = {
    "ama-kuchi": {"oni", "1"},
    "amakuchi": {"oni", "1"},
    "kara-kuchi": {"ura", "2"},
    "karakuchi": {"ura", "2"},
}


def _canonical_mode_token(mode: object) -> str:
    if not isinstance(mode, str):
        return ""
    token = mode.strip().casefold()
    if token in {"dan", "dojo"}:
        return "dandojo"
    return token


def _extend_aliases(seed: str, destination: set[str]) -> None:
    queue = [seed]
    seen: set[str] = set()

    while queue:
        token = queue.pop(0)
        if not token or token in seen:
            continue
        seen.add(token)
        destination.add(token)

        aliases: set[str] = set()
        aliases.update(_NUMERIC_ALIASES.get(token, set()))
        aliases.update(_SPECIAL_ALIASES.get(token, set()))

        compact = re.sub(r"[\s_\-]+", "", token)
        if compact and compact not in seen:
            aliases.add(compact)

        digit_aliases = {match.lstrip('0') or '0' for match in re.findall(r"\d+", token)}
        aliases.update(digit_aliases)

        for alias in aliases:
            if not alias:
                continue
            normalised = alias.strip().casefold()
            if normalised and normalised not in seen:
                queue.append(normalised)


def normalise_course_tokens(chart: Mapping[str, object]) -> set[str]:
    tokens: set[str] = set()

    def _queue_token(raw: object) -> None:
        if not isinstance(raw, str):
            return
        cleaned = raw.strip()
        if not cleaned:
            return
        _extend_aliases(cleaned.casefold(), tokens)

    for key in ("course", "canonical_course", "display_course", "raw_course"):
        _queue_token(chart.get(key))

    mode_value = _canonical_mode_token(chart.get("chart_mode") or chart.get("mode"))
    if mode_value:
        _extend_aliases(mode_value, tokens)

    rank_value = chart.get("rank")
    if isinstance(rank_value, str) and rank_value.strip():
        _queue_token(rank_value)
    elif isinstance(rank_value, (int, float)):
        _queue_token(str(rank_value))

    return {token for token in tokens if token}


def _normalise_filter_tokens(filter_mode: object) -> tuple[str, ...]:
    tokens: list[str] = []

    def _append_token(raw: object) -> None:
        token = _canonical_mode_token(raw)
        if token and token not in tokens:
            tokens.append(token)

    def _queue(value: object) -> None:
        if value is None:
            return
        if isinstance(value, str):
            _append_token(value)
            return
        if isinstance(value, Sequence):
            for item in value:
                _queue(item)
            return
        _append_token(str(value))

    _queue(filter_mode)
    return tuple(tokens)


def select_best_chart(
    charts: Optional[Sequence[Mapping[str, object]]],
    course: Optional[str] = None,
    prefer_modes: Sequence[object] = ("tower", "dandojo"),
    filter_mode: object = None,
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

    filter_mode_tokens = _normalise_filter_tokens(filter_mode)

    best_chart: Optional[Mapping[str, object]] = None
    best_priority: Optional[tuple[int, int, int]] = None

    prefer_mode_order = {
        token: idx for idx, token in enumerate(prefer_mode_tokens)
    }

    def _score_chart(
        index: int, chart: Mapping[str, object], mode_value: Optional[str] = None
    ) -> tuple[int, int, int]:
        mode_value = mode_value or _canonical_mode_token(
            chart.get("chart_mode") or chart.get("mode")
        )
        mode_priority = prefer_mode_order.get(mode_value, len(prefer_mode_tokens))

        score = 0
        if course_token:
            display_course = str(chart.get("display_course") or "").strip().casefold()
            canonical_course = str(chart.get("canonical_course") or "").strip().casefold()
            if display_course == course_token:
                score -= 3
            if canonical_course == course_token:
                score -= 2

            rank_value = chart.get("rank")
            if isinstance(rank_value, str):
                rank_token = rank_value.strip().casefold()
            elif isinstance(rank_value, (int, float)):
                rank_token = str(rank_value).strip().casefold()
            else:
                rank_token = ""
            if rank_token and rank_token == course_token:
                score -= 3
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

    candidates = (
        filtered
        if filtered
        else [
            (index, chart)
            for index, chart in enumerate(charts)
            if isinstance(chart, Mapping)
        ]
    )

    for index, chart in candidates:
        mode_value = _canonical_mode_token(chart.get("chart_mode") or chart.get("mode"))
        if filter_mode_tokens and mode_value not in filter_mode_tokens:
            continue
        priority = _score_chart(index, chart, mode_value)
        if best_priority is None or priority < best_priority:
            best_priority = priority
            best_chart = chart

    return best_chart


__all__ = ["normalise_course_tokens", "select_best_chart"]
