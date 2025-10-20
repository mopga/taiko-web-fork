"""Utilities for tower chart timing normalization."""

from __future__ import annotations

from typing import Iterable, List, Mapping, MutableMapping, Optional, Sequence


NOTE_KIND_MAP = {
    "don": 1,
    "dai": 1,
    "ka": 2,
}


def _coerce_int(value: object) -> Optional[int]:
    try:
        return int(round(float(value)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _coerce_float(value: object) -> Optional[float]:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if result != result or result in {float("inf"), float("-inf")}:
        return None
    return result


def _note_kind_token(note_type: Optional[str]) -> Optional[int]:
    if not note_type:
        return None
    lowered = note_type.strip().casefold()
    if not lowered:
        return None
    if lowered in NOTE_KIND_MAP:
        return NOTE_KIND_MAP[lowered]
    if lowered.startswith("dai"):
        base = lowered[3:]
        return NOTE_KIND_MAP.get(base)
    return NOTE_KIND_MAP.get(lowered)


def _measure_ratio(measure: Mapping[str, object]) -> float:
    time_sig = measure.get("time_sig")
    if isinstance(time_sig, Mapping):
        num = _coerce_float(time_sig.get("num"))
        den = _coerce_float(time_sig.get("den"))
        if num and den and den > 0:
            return num / den
    ratio_value = _coerce_float(measure.get("ratio"))
    if ratio_value and ratio_value > 0:
        return ratio_value
    return 1.0


def normalize_measures_relative(measures: Sequence[object]) -> List[MutableMapping[str, object]]:
    """Convert absolute note timings to be relative to their measure start."""

    normalized_measures: List[MutableMapping[str, object]] = []
    if not isinstance(measures, Iterable):
        return normalized_measures

    previous_start: Optional[int] = None
    previous_duration: Optional[int] = None

    for index, measure in enumerate(measures):
        if isinstance(measure, MutableMapping):
            new_measure: MutableMapping[str, object] = dict(measure)
        else:
            try:
                new_measure = dict(measure)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                new_measure = {}

        if not isinstance(new_measure, MutableMapping):
            new_measure = {}

        notes_source = new_measure.get("notes") if isinstance(new_measure, Mapping) else []
        if not isinstance(notes_source, Iterable):
            notes_source = []

        longs_source = new_measure.get("longs") if isinstance(new_measure, Mapping) else []
        if not isinstance(longs_source, Iterable):
            longs_source = []

        bpm_value = _coerce_float(new_measure.get("bpm"))
        if not bpm_value or bpm_value <= 0:
            bpm_value = 120.0

        ratio_value = _measure_ratio(new_measure)
        if ratio_value <= 0:
            ratio_value = 1.0

        duration_value = _coerce_int(new_measure.get("duration_ms"))
        if duration_value is None or duration_value <= 0:
            duration_value = int(round((240000.0 / bpm_value) * ratio_value))
        if duration_value < 0:
            duration_value = 0

        absolute_positions: List[int] = []
        for note in notes_source:
            note_mapping = note if isinstance(note, Mapping) else None
            if note_mapping is None:
                try:
                    note_mapping = dict(note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            absolute_at = _coerce_int(note_mapping.get("at"))
            if absolute_at is not None:
                absolute_positions.append(absolute_at)

        for long_note in longs_source:
            long_mapping = long_note if isinstance(long_note, Mapping) else None
            if long_mapping is None:
                try:
                    long_mapping = dict(long_note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            absolute_at = _coerce_int(long_mapping.get("at"))
            if absolute_at is not None:
                absolute_positions.append(absolute_at)
            absolute_end = _coerce_int(long_mapping.get("end_at"))
            if absolute_end is not None:
                absolute_positions.append(absolute_end)

        start_value = _coerce_int(new_measure.get("start_ms"))
        if start_value is None:
            if absolute_positions:
                start_value = min(absolute_positions)
            elif previous_start is not None and previous_duration is not None:
                start_value = previous_start + previous_duration
            else:
                start_value = index * duration_value if duration_value else 0

        normalized_notes: List[MutableMapping[str, object]] = []
        for note in notes_source:
            note_mapping = note if isinstance(note, Mapping) else None
            if note_mapping is None:
                try:
                    note_mapping = dict(note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            note_copy: MutableMapping[str, object] = dict(note_mapping)
            absolute_at = _coerce_int(note_copy.get("at"))
            if absolute_at is None:
                absolute_at = start_value
            relative_at = absolute_at - start_value
            if relative_at < 0:
                relative_at = 0
            note_copy["at"] = relative_at
            note_copy["offset"] = relative_at
            kind_code = _note_kind_token(str(note_copy.get("type") or ""))
            if kind_code is not None:
                note_copy["kind"] = kind_code
            if duration_value > 0:
                position = relative_at / duration_value
            else:
                position = 0.0
            if position < 0:
                position = 0.0
            elif position > 1:
                position = 1.0
            note_copy["p"] = position
            normalized_notes.append(note_copy)

        normalized_notes.sort(key=lambda entry: entry.get("at", 0))

        normalized_longs: List[MutableMapping[str, object]] = []
        for long_note in longs_source:
            long_mapping = long_note if isinstance(long_note, Mapping) else None
            if long_mapping is None:
                try:
                    long_mapping = dict(long_note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            long_copy: MutableMapping[str, object] = dict(long_mapping)
            absolute_at = _coerce_int(long_copy.get("at"))
            if absolute_at is None:
                absolute_at = start_value
            relative_at = absolute_at - start_value
            if relative_at < 0:
                relative_at = 0
            absolute_end = _coerce_int(long_copy.get("end_at"))
            if absolute_end is None:
                absolute_end = absolute_at
            relative_end = absolute_end - start_value
            if relative_end < relative_at:
                relative_end = relative_at
            long_copy["at"] = relative_at
            long_copy["end_at"] = relative_end
            long_copy["len_ms"] = max(0, relative_end - relative_at)
            normalized_longs.append(long_copy)

        new_measure["start_ms"] = start_value
        new_measure["duration_ms"] = duration_value
        new_measure["notes"] = normalized_notes
        new_measure["longs"] = normalized_longs

        normalized_measures.append(new_measure)
        previous_start = start_value
        previous_duration = duration_value

    return normalized_measures


__all__ = ["normalize_measures_relative"]
