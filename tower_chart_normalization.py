"""Utilities for tower chart timing normalization."""

from __future__ import annotations

from typing import Iterable, List, Mapping, MutableMapping, Sequence


def normalize_measures_relative(measures: Sequence[object]) -> List[MutableMapping[str, object]]:
    """Convert absolute note timings to be relative to their measure start."""

    normalized_measures: List[MutableMapping[str, object]] = []
    if not isinstance(measures, Iterable):
        return normalized_measures

    for index, measure in enumerate(measures):
        if isinstance(measure, MutableMapping):
            new_measure: MutableMapping[str, object] = dict(measure)
        else:
            try:
                new_measure = dict(measure)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                new_measure = {}

        notes_source = new_measure.get('notes') if isinstance(new_measure, Mapping) else []
        if not isinstance(notes_source, Iterable):
            notes_source = []

        longs_source = new_measure.get('longs') if isinstance(new_measure, Mapping) else []
        if not isinstance(longs_source, Iterable):
            longs_source = []

        bpm_value = new_measure.get('bpm') if isinstance(new_measure, Mapping) else None
        try:
            bpm = float(bpm_value)  # type: ignore[arg-type]
            if bpm <= 0:
                raise ValueError
        except (TypeError, ValueError):
            bpm = 120.0
        measure_len = int(round(4 * (60000.0 / bpm)))

        absolute_positions = []
        for note in notes_source:
            note_mapping = note if isinstance(note, Mapping) else None
            if note_mapping is None:
                try:
                    note_mapping = dict(note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            try:
                absolute_positions.append(float(note_mapping.get('at')))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue

        for long_note in longs_source:
            long_mapping = long_note if isinstance(long_note, Mapping) else None
            if long_mapping is None:
                try:
                    long_mapping = dict(long_note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            try:
                absolute_positions.append(float(long_mapping.get('at')))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                pass
            try:
                absolute_positions.append(float(long_mapping.get('end_at')))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue

        if absolute_positions:
            start_ms = int(round(min(absolute_positions)))
        else:
            start_ms = int(index * measure_len)

        normalized_notes = []
        for note in notes_source:
            note_mapping = note if isinstance(note, Mapping) else None
            if note_mapping is None:
                try:
                    note_mapping = dict(note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            note_copy = dict(note_mapping)
            try:
                absolute_at = int(round(float(note_copy.get('at'))))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                absolute_at = start_ms
            relative_at = absolute_at - start_ms
            if relative_at < 0:
                relative_at = 0
            note_copy['at'] = relative_at
            normalized_notes.append(note_copy)

        normalized_longs = []
        for long_note in longs_source:
            long_mapping = long_note if isinstance(long_note, Mapping) else None
            if long_mapping is None:
                try:
                    long_mapping = dict(long_note)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
            long_copy = dict(long_mapping)
            try:
                absolute_at = int(round(float(long_copy.get('at'))))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                absolute_at = start_ms
            relative_at = absolute_at - start_ms
            if relative_at < 0:
                relative_at = 0
            long_copy['at'] = relative_at

            try:
                absolute_end = int(round(float(long_copy.get('end_at'))))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                absolute_end = absolute_at
            relative_end = absolute_end - start_ms
            if relative_end < relative_at:
                relative_end = relative_at
            long_copy['end_at'] = relative_end

            normalized_longs.append(long_copy)

        new_measure['start_ms'] = start_ms
        new_measure['duration_ms'] = measure_len
        new_measure['notes'] = normalized_notes
        new_measure['longs'] = normalized_longs
        normalized_measures.append(new_measure)

    return normalized_measures


__all__ = ["normalize_measures_relative"]
