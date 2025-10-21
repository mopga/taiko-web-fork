"""Helpers for controlling TJA validation verbosity and aggregation."""

from __future__ import annotations

import logging
import os
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set


LOGGER = logging.getLogger('songs_scanner')


def _coerce_bool(value: Optional[str], *, default: bool = False) -> bool:
    if value is None:
        return default
    token = value.strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    return default


def _coerce_mode(value: Optional[str]) -> str:
    token = (value or "warn").strip().lower()
    if token not in {"off", "warn", "strict"}:
        return "warn"
    return token


@dataclass
class _FileStats:
    counts: Counter = field(default_factory=Counter)
    courses: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))


class TjaValidationController:
    """Track validation diagnostics and emit aggregated summaries."""

    def __init__(self) -> None:
        self._mode = _coerce_mode(os.getenv("TJA_VALIDATION_MODE"))
        self._log_enabled = _coerce_bool(os.getenv("TJA_VALIDATION_LOG"), default=False)
        self._summary_enabled = _coerce_bool(os.getenv("TJA_VALIDATION_SUMMARY"), default=True)
        self._lock = threading.Lock()
        self._file_stats: Dict[str, _FileStats] = {}
        self._global_counts: Counter = Counter()
        self._files_by_code: Dict[str, Set[str]] = defaultdict(set)
        self._seen_files: Set[str] = set()

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def logging_enabled(self) -> bool:
        return self._log_enabled

    def reset_run(self) -> None:
        with self._lock:
            self._file_stats.clear()
            self._global_counts.clear()
            self._files_by_code.clear()
            self._seen_files.clear()

    def register_file(self, path: object) -> None:
        key = str(path)
        with self._lock:
            self._seen_files.add(key)
            self._file_stats.setdefault(key, _FileStats())

    def report(self, code: str, *, path: object, course: Optional[str] = None, token: Optional[str] = None) -> bool:
        if self._mode == "off":
            return False
        key = str(path)
        with self._lock:
            stats = self._file_stats.setdefault(key, _FileStats())
            stats.counts[code] += 1
            if course:
                course_key = course.strip()
                if course_key:
                    stats.courses.setdefault(code, set()).add(course_key)
            self._global_counts[code] += 1
            self._files_by_code[code].add(key)
        return self._mode == "strict"

    def finalize_file(self, path: object) -> None:
        key = str(path)
        stats: Optional[_FileStats]
        with self._lock:
            stats = self._file_stats.pop(key, None)
        if self._mode == "strict" and self._log_enabled and stats and stats.counts:
            for code in sorted(stats.counts):
                count = stats.counts[code]
                courses = sorted(stats.courses.get(code, set())) if stats.courses else []
                if courses:
                    LOGGER.error(
                        "validation-error: file=%s code=%s count=%d courses=%s",
                        key,
                        code,
                        count,
                        ",".join(courses),
                    )
                else:
                    LOGGER.error(
                        "validation-error: file=%s code=%s count=%d",
                        key,
                        code,
                        count,
                    )

    def flush_summary(self) -> None:
        if self._mode == "off" or not self._summary_enabled or not self._log_enabled:
            return
        with self._lock:
            total_files = len(self._seen_files)
            counts_snapshot = Counter(self._global_counts)
            files_snapshot = {code: set(files) for code, files in self._files_by_code.items()}
        summary_mode = self._mode or "warn"
        if not counts_snapshot:
            if total_files:
                LOGGER.info(
                    "Validation summary (mode=%s): files=%d, no issues detected",
                    summary_mode,
                    total_files,
                )
            return

        parts = []
        for code in sorted(counts_snapshot):
            issue_count = counts_snapshot[code]
            file_count = len(files_snapshot.get(code, set()))
            parts.append(f"{code}={issue_count} ({file_count} files)")
        summary = ", ".join(parts)
        LOGGER.info(
            "Validation summary (mode=%s): files=%d, %s",
            summary_mode,
            total_files,
            summary,
        )


_VALIDATOR = TjaValidationController()


def get_tja_validator() -> TjaValidationController:
    return _VALIDATOR


def is_validation_logging_enabled() -> bool:
    return _VALIDATOR.logging_enabled

