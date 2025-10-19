from pathlib import Path
import logging
import queue
import sys
import tempfile
import threading
import unittest
from typing import List, Optional
from unittest import mock

sys.path.append(str(Path(__file__).resolve().parents[1]))

import songs_scanner
from songs_scanner import ChartRecord, SongScanner, TjaImportRecord, compute_group_key, parse_tja


class _DummyUpdateResult:
    def __init__(self, matched: int, modified: int, upserted: Optional[int]):
        self.matched_count = matched
        self.modified_count = modified
        self.upserted_id = upserted
        self.acknowledged = True


class _MemoryCollection:
    def __init__(self):
        self._docs = []
        self._lock = threading.Lock()

    def create_index(self, *args, **kwargs):
        return None

    def drop_index(self, *args, **kwargs):
        return None

    def _matches(self, doc, filter_):
        if not filter_:
            return True
        for key, expected in filter_.items():
            if isinstance(expected, dict):
                if '$exists' in expected:
                    has_field = self._has_path(doc, key)
                    if expected['$exists'] and not has_field:
                        return False
                    if not expected['$exists'] and has_field:
                        return False
                    value = self._resolve_key(doc, key)
                else:
                    value = self._resolve_key(doc, key)
                if '$type' in expected:
                    expected_type = expected['$type']
                    if expected_type == 'string':
                        if not isinstance(value, str):
                            return False
                    elif expected_type == 'number':
                        if not isinstance(value, (int, float)):
                            return False
                    continue
                if '$ne' in expected and value == expected['$ne']:
                    return False
                if '$in' in expected and value not in expected['$in']:
                    return False
                if '$nin' in expected and value in expected['$nin']:
                    return False
            else:
                value = self._resolve_key(doc, key)
                if value != expected:
                    return False
        return True

    def _resolve_key(self, doc, dotted):
        current = doc
        for part in dotted.split('.'):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _clone(self, value):
        if isinstance(value, dict):
            return {k: self._clone(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._clone(v) for v in value]
        return value

    def _has_path(self, doc, dotted):
        current = doc
        for part in dotted.split('.'):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return False
        return True

    def _set_path(self, doc, dotted, value):
        parts = dotted.split('.')
        target = doc
        for part in parts[:-1]:
            if isinstance(target, dict):
                target = target.setdefault(part, {})
            else:
                return
        if isinstance(target, dict):
            target[parts[-1]] = value

    def _parse_array_filters(self, array_filters):
        mapping = {}
        for filter_doc in array_filters or []:
            for key, expected in filter_doc.items():
                placeholder, *path = key.split('.')
                mapping.setdefault(placeholder, []).append((path, expected))
        return mapping

    def _element_matches_filter(self, element, filters):
        if not filters:
            return True
        for path, expected in filters:
            value = element
            for part in path:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
                    break
            if isinstance(expected, dict):
                if '$in' in expected and value not in expected['$in']:
                    return False
                if '$nin' in expected and value in expected['$nin']:
                    return False
                if '$ne' in expected and value == expected['$ne']:
                    return False
                continue
            if value != expected:
                return False
        return True

    def _apply_array_set(self, doc, path, value, array_filters):
        parts = path.split('.')
        if len(parts) < 2:
            self._set_path(doc, path, self._clone(value))
            return
        array_field = parts[0]
        placeholder = parts[1]
        if not placeholder.startswith('$['):
            self._set_path(doc, path, self._clone(value))
            return
        placeholder_key = placeholder[2:-1]
        remaining = parts[2:]
        array = doc.setdefault(array_field, [])
        if not isinstance(array, list):
            return
        filters_map = self._parse_array_filters(array_filters)
        filter_conditions = filters_map.get(placeholder_key, [])
        for index, element in enumerate(array):
            if isinstance(element, dict) or not remaining:
                if self._element_matches_filter(element, filter_conditions):
                    if not remaining:
                        array[index] = self._clone(value)
                    else:
                        target = element if isinstance(element, dict) else {}
                        if not isinstance(element, dict):
                            array[index] = target
                        self._set_path(target, '.'.join(remaining), self._clone(value))

    def _should_pull(self, element, condition):
        if isinstance(condition, dict):
            for key, expected in condition.items():
                value = None
                if isinstance(element, dict):
                    value = self._resolve_key(element, key) if '.' in key else element.get(key)
                if isinstance(expected, dict):
                    if '$nin' in expected:
                        if value in expected['$nin']:
                            return False
                        continue
                    if '$in' in expected:
                        if value not in expected['$in']:
                            return False
                        continue
                    if '$ne' in expected:
                        if value == expected['$ne']:
                            return False
                        continue
                    if '$eq' in expected:
                        if value != expected['$eq']:
                            return False
                        continue
                    if value != expected:
                        return False
                else:
                    if value != expected:
                        return False
            return True
        return element == condition

    def _apply_update(self, doc, update, *, array_filters=None):
        if '$set' in update:
            for key, value in update['$set'].items():
                if '$[' in key and array_filters:
                    self._apply_array_set(doc, key, value, array_filters)
                else:
                    self._set_path(doc, key, self._clone(value))
        if '$inc' in update:
            for key, amount in update['$inc'].items():
                current = self._resolve_key(doc, key)
                if not isinstance(current, (int, float)):
                    current = 0
                self._set_path(doc, key, current + amount)
        if '$max' in update:
            for key, value in update['$max'].items():
                current = self._resolve_key(doc, key)
                if current is None or current < value:
                    self._set_path(doc, key, self._clone(value))
        if '$addToSet' in update:
            for key, value in update['$addToSet'].items():
                array = doc.setdefault(key, [])
                if not isinstance(array, list):
                    continue
                candidate = self._clone(value)
                if candidate not in array:
                    array.append(candidate)
        if '$pull' in update:
            for key, condition in update['$pull'].items():
                array = doc.get(key)
                if not isinstance(array, list):
                    continue
                doc[key] = [item for item in array if not self._should_pull(item, condition)]

    def _project(self, doc, projection):
        if not projection:
            return dict(doc)
        include_keys = {key for key, enabled in projection.items() if enabled}
        if not include_keys:
            return dict(doc)
        projected = {}
        for key in include_keys:
            projected[key] = self._resolve_key(doc, key)
        return projected

    def find_one(self, filter_=None, projection=None, sort=None, **kwargs):
        with self._lock:
            matches = [doc for doc in self._docs if self._matches(doc, filter_ or {})]
        if sort:
            for key, direction in reversed(sort):
                reverse = direction < 0
                matches.sort(key=lambda doc, k=key: self._resolve_key(doc, k), reverse=reverse)
        if not matches:
            return None
        return self._project(matches[0], projection or {})

    def find(self, filter_=None, projection=None):
        with self._lock:
            snapshot = list(self._docs)
        for doc in snapshot:
            if self._matches(doc, filter_ or {}):
                yield self._project(doc, projection or {})

    def find_one_and_update(self, filter_, update, upsert=False, return_document=None, **kwargs):
        with self._lock:
            doc = None
            for candidate in self._docs:
                if self._matches(candidate, filter_ or {}):
                    doc = candidate
                    break
            inserted = False
            if doc is None and upsert:
                base = dict(update.get('$setOnInsert', {}))
                for key, value in (filter_ or {}).items():
                    if isinstance(value, dict):
                        continue
                    base.setdefault(key, value)
                base.setdefault('_id', len(self._docs) + 1)
                self._docs.append(base)
                doc = base
                inserted = True
                if hasattr(self, 'inserted'):
                    self.inserted.append(doc)
            if doc is None:
                return None
            if update:
                self._apply_update(doc, update, array_filters=kwargs.get('array_filters'))
            if inserted and '$setOnInsert' in update:
                doc.update(update['$setOnInsert'])
            return dict(doc)

    def insert_one(self, document):
        with self._lock:
            self._docs.append(dict(document))

    def update_one(self, filter_, update, upsert=False, array_filters=None):
        with self._lock:
            for doc in self._docs:
                if self._matches(doc, filter_ or {}):
                    before = self._clone(doc)
                    if update:
                        self._apply_update(doc, update, array_filters=array_filters)
                    modified = 1 if doc != before else 0
                    return _DummyUpdateResult(1, modified, None)
        if upsert and ('$set' in update or '$setOnInsert' in update):
            new_doc = {}
            if '$setOnInsert' in update:
                new_doc.update(self._clone(update['$setOnInsert']))
            if '$set' in update:
                new_doc.update(self._clone(update['$set']))
            if filter_:
                for key, value in filter_.items():
                    if isinstance(value, dict):
                        continue
                    new_doc.setdefault(key, value)
            if '_id' not in new_doc:
                new_doc['_id'] = len(self._docs) + 1
            self._docs.append(new_doc)
            if hasattr(self, 'inserted'):
                self.inserted.append(new_doc)
            return _DummyUpdateResult(0, 0, new_doc.get('_id'))
        return _DummyUpdateResult(0, 0, None)

    def delete_many(self, filter_):
        with self._lock:
            self._docs = [doc for doc in self._docs if not self._matches(doc, filter_ or {})]


class _SeqCollection(_MemoryCollection):
    def __init__(self):
        super().__init__()
        self._docs = [{'name': 'songs', 'value': 0}]

    def find_one(self, filter_=None, projection=None):
        return super().find_one(filter_, projection)

    def update_one(self, filter_, update, upsert=False):
        with self._lock:
            for doc in self._docs:
                if self._matches(doc, filter_ or {}):
                    if '$set' in update:
                        doc.update(update['$set'])
                    return
        if upsert:
            super().update_one(filter_, update, upsert=True)


class _CountersCollection(_MemoryCollection):
    def __init__(self):
        super().__init__()
        self._docs = [{'_id': 'songs', 'seq': 0}]


class _SongsCollection(_MemoryCollection):
    def __init__(self):
        super().__init__()
        self.inserted = []

    def insert_one(self, document):
        super().insert_one(document)
        with self._lock:
            self.inserted.append(self._docs[-1])


class _DummyDB:
    def __init__(self):
        self.seq = _SeqCollection()
        self.counters = _CountersCollection()
        self.songs = _SongsCollection()
        self.categories = _MemoryCollection()
        self.song_scanner_state = _MemoryCollection()
        self.import_issues = _MemoryCollection()


class TestSongsScanner(unittest.TestCase):
    def _base_record_kwargs(self):
        return dict(
            relative_path="Pack/Sample.tja",
            relative_dir="Pack",
            tja_url="/songs/Pack/Sample.tja",
            dir_url="/songs/Pack/",
            audio_url="/songs/Pack/sample.ogg",
            audio_path="Pack/sample.ogg",
            audio_hash="hash123",
            audio_mtime_ns=None,
            audio_size=None,
            music_type=None,
            diagnostics=[],
            title="Sample",
            title_ja=None,
            subtitle="",
            subtitle_ja=None,
            locale={},
            offset=0.0,
            preview=0.0,
            fingerprint="fp",
            tja_hash="tja-hash",
            wave="sample.ogg",
            song_id=None,
            genre=None,
            category_id=0,
            category_title="Unsorted",
            charts=[],
            import_issues=[],
            normalized_title="sample",
        )

    def _make_record(self, **overrides):
        base = self._base_record_kwargs()
        base.update(overrides)
        return TjaImportRecord(**base)

    def test_parse_tja_extracts_metadata(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "chart.tja"
        content = "\n".join(
            [
                "TITLE:Test Song",
                "TITLEJA:テストソング",
                "SUBTITLE:Artist",
                "SUBTITLEJA:サブタイトル",
                "OFFSET:1.5",
                "DEMOSTART:12.5",
                "COURSE:Oni",
                "LEVEL:8",
                "#BRANCHSTART",
                "COURSE:Hard",
                "LEVEL:5",
            ]
        )
        tja_path.write_text(content, encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(parsed.title, "Test Song")
        self.assertEqual(parsed.title_ja, "テストソング")
        self.assertEqual(parsed.subtitle, "Artist")
        self.assertEqual(parsed.subtitle_ja, "サブタイトル")
        self.assertAlmostEqual(parsed.offset, 1.5)
        self.assertAlmostEqual(parsed.preview, 12.5)
        courses = {course.canonical: course for course in parsed.courses}
        self.assertEqual(courses["oni"].stars, 8)
        self.assertTrue(courses["oni"].branch)
        self.assertEqual(courses["hard"].stars, 5)

    def test_parse_tja_directive_after_start_preserves_chart(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "chart.tja"
        tja_path.write_text("\n".join([
            "TITLE:Directive Test",
            "COURSE:Oni",
            "LEVEL:5",
            "#START",
            "#BPMCHANGE 80",
            "1110,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        chart = parsed.courses[0]
        self.assertEqual(chart.total_notes, 3)
        self.assertEqual(chart.hit_notes, 3)
        self.assertEqual(chart.measures, 1)
        self.assertEqual(chart.first_note_preview, "1110,")

    def test_parse_tja_handles_gogo_sections_without_resetting_counts(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "gogo.tja"
        tja_path.write_text("\n".join([
            "TITLE:Gogo Test",
            "COURSE:Oni",
            "LEVEL:3",
            "#START",
            "1110,",
            "#GOGOSTART",
            "2220,",
            "#GOGOEND",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        chart = parsed.courses[0]
        self.assertEqual(chart.total_notes, 6)
        self.assertEqual(chart.hit_notes, 6)
        self.assertEqual(chart.measures, 2)
        self.assertEqual(chart.first_note_preview, "1110,")

    def test_parse_tja_counts_measures_with_nine_token(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "nine.tja"
        tja_path.write_text("\n".join([
            "TITLE:Nine Token",
            "COURSE:Oni",
            "LEVEL:4",
            "#START",
            "10000900,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        chart = parsed.courses[0]
        self.assertEqual(chart.total_notes, 1)
        self.assertEqual(chart.hit_notes, 1)
        self.assertEqual(chart.measures, 1)

    def test_parse_tja_counts_other_tokens_when_nine_present(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "mixed_nine.tja"
        tja_path.write_text("\n".join([
            "TITLE:Mixed Nine",
            "COURSE:Oni",
            "LEVEL:4",
            "#START",
            "1,2,90001,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        chart = parsed.courses[0]
        self.assertEqual(chart.total_notes, 3)
        self.assertEqual(chart.hit_notes, 3)
        self.assertEqual(chart.measures, 3)

    def test_parse_tja_unknown_directive_does_not_reset_counts(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "unknown_directive.tja"
        tja_path.write_text("\n".join([
            "TITLE:Unknown Directive Test",
            "COURSE:Oni",
            "LEVEL:4",
            "#START",
            "1110,",
            "#FOOBAR",
            "2220,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        chart = parsed.courses[0]
        self.assertEqual(chart.total_notes, 6)
        self.assertEqual(chart.hit_notes, 6)
        self.assertEqual(chart.measures, 2)
        self.assertEqual(chart.first_note_preview, "1110,")

    def test_implicit_header_closes_previous_chart(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "implicit_header.tja"
        tja_path.write_text("\n".join([
            "TITLE:Implicit End",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "11,",
            "11,",
            "COURSE:Hard",
            "LEVEL:5",
            "#START",
            "1110,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertIn("oni", parsed.charts)
        self.assertIn("hard", parsed.charts)
        self.assertGreater(parsed.charts["oni"].notes_count, 0)
        self.assertGreater(parsed.charts["hard"].notes_count, 0)
        self.assertEqual(parsed.implicit_end_due_to_header, 1)

    def test_non_whitelisted_colon_line_inside_notes_does_not_close_chart(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "colon_inside_notes.tja"
        tja_path.write_text("\n".join([
            "TITLE:Colon Line",
            "COURSE:Oni",
            "LEVEL:5",
            "#START",
            "11,",
            "COMMENT: this is not a header",
            "22,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(parsed.implicit_end_due_to_header, 0)
        self.assertIn("oni", parsed.charts)
        chart = parsed.charts["oni"]
        self.assertEqual(chart.total_notes, 4)
        self.assertIn("unknown-metadata", chart.issues)
        self.assertEqual(parsed.unknown_directives, 1)

    def test_branching_directives_do_not_increment_unknown_counters(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "branching.tja"
        tja_path.write_text("\n".join([
            "TITLE:Branching",
            "COURSE:Oni",
            "LEVEL:4",
            "#START",
            "#BRANCHSTART",
            "#N",
            "1110,",
            "#BRANCHSWITCH",
            "#E",
            "2220,",
            "#BRANCHEND",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        chart = parsed.courses[0]
        self.assertEqual(chart.total_notes, 6)
        self.assertEqual(chart.hit_notes, 6)
        self.assertEqual(chart.measures, 2)
        self.assertEqual(chart.unknown_directives, 0)
        self.assertEqual(parsed.unknown_directives, 0)

    def test_parse_tja_dan_downcasts_to_standard_course(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "Second Dan" / "dojo.tja"
        tja_path.parent.mkdir(parents=True, exist_ok=True)
        tja_path.write_text("\n".join([
            "TITLE:Trial Second Dan",
            "COURSE:Dan",
            "LEVEL:1",
            "WAVE:segment1.ogg",
            "#START",
            "1110,",
            "#EXAM1 0 1 2",
            "#NEXTSONG",
            "WAVE:segment2.ogg",
            "2220,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
            parsed = parse_tja(tja_path)

        self.assertFalse(parsed.has_dojo_course)
        self.assertEqual(len(parsed.courses), 1)
        course = parsed.courses[0]
        self.assertEqual(course.mode, "standard")
        self.assertEqual(course.canonical, "oni")
        self.assertIn("mapped-course", course.issues)
        self.assertEqual(course.total_notes, 6)
        self.assertEqual(course.hit_notes, 6)
        self.assertEqual(course.measures, 2)
        self.assertEqual(course.unknown_directives, 0)
        self.assertTrue(any('mapped-course(parser): DAN→ONI' in message for message in logs.output))

    def test_parse_tja_tower_downcasts_to_oni(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "Tower" / "tower.tja"
        tja_path.parent.mkdir(parents=True, exist_ok=True)
        tja_path.write_text("\n".join([
            "TITLE:Tower Trial",
            "COURSE:Tower",
            "LEVEL:5",
            "#START",
            "1111,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
            parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        course = parsed.courses[0]
        self.assertEqual(course.canonical, "oni")
        self.assertEqual(course.mode, "standard")
        self.assertIn("mapped-course", course.issues)
        self.assertTrue(any('mapped-course(parser): TOWER→ONI' in message for message in logs.output))

    def test_parse_tja_skips_comments_before_start_and_counts_notes(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "Tower" / "tower-comment.tja"
        tja_path.parent.mkdir(parents=True, exist_ok=True)
        tja_path.write_text("\n".join([
            "// leading comment",
            "",
            "COURSE:Tower",
            "LEVEL:7",
            "#START",
            "1110,",
            "#END",
        ]), encoding="utf-8-sig")

        with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
            parsed = parse_tja(tja_path)

        self.assertEqual(parsed.mapped_courses, 1)
        self.assertEqual(parsed.skipped_no_course, 0)
        self.assertEqual(len(parsed.courses), 1)
        course = parsed.courses[0]
        self.assertEqual(course.canonical, "oni")
        self.assertEqual(course.mode, "standard")
        self.assertGreater(course.total_notes, 0)
        self.assertGreater(course.hit_notes, 0)
        self.assertIn("mapped-course", course.issues)
        self.assertTrue(any('mapped-course(parser): TOWER→ONI' in message for message in logs.output))

    def test_tower_after_blank_and_comments_is_playable(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "tower.tja"
        tja_path.write_text(
            """//TJADB Project
TITLE:X
BPM:165
WAVE:x.ogg
LIFE:1

COURSE:Tower
LEVEL:7

#START

11,
1110,
#BPMCHANGE 165.16
1120,
#END
""",
            encoding="utf-8",
        )

        parsed = parse_tja(tja_path)

        self.assertIn('oni', parsed.charts)
        self.assertGreater(parsed.charts['oni'].notes_count, 0)

    def test_tower_7_chart_parses_strict_without_fallback(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "Taiko Tower 7 Ama-kuchi.tja"
        filler_lines = ["0000," for _ in range(30)]
        tja_content = "\n".join([
            "TITLE:Fallback",
            "COURSE:Tower",
            "LEVEL:7",
            "#START",
            *filler_lines,
            "1111,",
            "#END",
        ])
        tja_path.write_text(tja_content, encoding="utf-8")

        with mock.patch.object(songs_scanner, "TJA_LENIENT_FALLBACK", True):
            with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
                parsed = parse_tja(tja_path)

        self.assertFalse(any("fallback-to-lenient" in message for message in logs.output))
        self.assertIn('oni', parsed.charts)
        self.assertGreater(parsed.charts['oni'].total_notes, 0)
        self.assertNotIn('lenient-fallback', parsed.charts['oni'].issues)
        chart_data = parsed.charts['oni'].chart_data
        self.assertIsNotNone(chart_data)
        if chart_data is not None:
            self.assertEqual(chart_data.get('course'), 'oni')
            self.assertEqual(chart_data.get('total_notes'), parsed.charts['oni'].total_notes)
            self.assertTrue(chart_data.get('measures'))
            for measure in chart_data.get('measures', []):
                self.assertIn('notes', measure)

    def test_strict_parser_logs_empty_and_non_empty_courses(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "placeholder.difficulties.tja"
        tja_path.write_text("\n".join([
            "TITLE:Test",
            "COURSE:Oni",
            "LEVEL:9",
            "#START",
            "#END",
            "COURSE:Hard",
            "LEVEL:6",
            "#START",
            "11,11,11,11,",
            "11,11,,",
            "#END",
        ]), encoding="utf-8")

        with mock.patch.object(songs_scanner, "TJA_LENIENT_FALLBACK", True):
            with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
                parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn("end-notes(strict): course=oni", joined_logs)
        self.assertIn("end-notes(strict): course=hard", joined_logs)
        self.assertNotIn("fallback-to-lenient", joined_logs)
        self.assertIn('oni', parsed.charts)
        self.assertIn('hard', parsed.charts)
        self.assertEqual(parsed.charts['oni'].total_notes, 0)
        self.assertEqual(parsed.charts['oni'].chart_data.get('measures'), [])
        self.assertGreater(parsed.charts['hard'].total_notes, 0)
        hard_chart = parsed.charts['hard'].chart_data or {}
        measures = hard_chart.get('measures', [])
        self.assertTrue(measures)
        note_counts = [len(measure.get('notes', [])) for measure in measures]
        self.assertTrue(any(count > 0 for count in note_counts))

    def test_lenient_fallback_handles_courses_without_hits(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "all.empty.tja"
        tja_path.write_text("\n".join([
            "TITLE:Empty",
            "COURSE:Normal",
            "LEVEL:4",
            "#START",
            "5000,",
            "0008,",
            "#END",
            "COURSE:Easy",
            "LEVEL:2",
            "#START",
            "6000,",
            "0008,",
            "#END",
        ]), encoding="utf-8")

        with mock.patch.object(songs_scanner, "TJA_LENIENT_FALLBACK", True):
            with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
                parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn("fallback-to-lenient: file=", joined_logs)
        self.assertIn("reason=no-valid-courses", joined_logs)
        courses = {course.canonical: course for course in parsed.courses}
        self.assertIn('normal', courses)
        self.assertIn('easy', courses)
        course = courses['normal']
        self.assertIn('lenient-fallback', course.issues)
        chart_data = course.chart_data or {}
        self.assertEqual(chart_data.get('total_notes', 0), 0)
        measures = chart_data.get('measures', [])
        self.assertTrue(measures)
        self.assertTrue(all('notes' in measure for measure in measures))
        self.assertTrue(any(measure.get('longs') for measure in measures))
        longs = [long for measure in measures for long in measure.get('longs', [])]
        self.assertTrue(longs)
        for long_note in longs:
            self.assertIn(long_note.get('kind'), {'drumroll', 'balloon'})
            self.assertIn('at', long_note)
            self.assertIn('end_at', long_note)
            self.assertIsInstance(long_note.get('big'), bool)
        self.assertEqual(courses['easy'].hit_notes, 0)
        self.assertNotIn('lenient-fallback', courses['easy'].issues)

    def test_strict_parse_failure_logs_and_preserves_other_courses(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "broken.one.tja"
        tja_path.write_text("\n".join([
            "TITLE:Mixed",
            "COURSE:Oni",
            "LEVEL:8",
            "#START",
            "11,",
            "#END",
            "COURSE:Hard",
            "LEVEL:6",
            "#START",
            "22,22,22,",
            "#END",
        ]), encoding="utf-8")

        original_cleaner = songs_scanner.NOTE_TOKEN_CLEAN_RE

        class _FailingCleaner:
            def __init__(self, pattern):
                self._pattern = pattern

            def sub(self, repl, string):
                if string == "11":
                    raise ValueError("forced parse failure")
                return self._pattern.sub(repl, string)

        with mock.patch.object(songs_scanner, "NOTE_TOKEN_CLEAN_RE", _FailingCleaner(original_cleaner)):
            with mock.patch.object(songs_scanner, "TJA_LENIENT_FALLBACK", True):
                with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
                    parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn("strict-parse-failed: course=oni", joined_logs)
        self.assertNotIn("fallback-to-lenient", joined_logs)
        self.assertIn('oni', parsed.charts)
        self.assertIn('hard', parsed.charts)
        self.assertEqual(parsed.charts['oni'].total_notes, 0)
        self.assertIn('strict-parse-failed', parsed.charts['oni'].issues)
        self.assertGreater(parsed.charts['hard'].total_notes, 0)

    def test_strict_parser_flushes_final_measure_with_timing(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "measure.flush.tja"
        tja_path.write_text("\n".join([
            "TITLE:Flush",
            "COURSE:Oni",
            "LEVEL:5",
            "#START",
            "11,",
            "#BPMCHANGE 180",
            "11,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
            parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn("end-notes(strict): course=oni", joined_logs)
        chart = parsed.charts['oni']
        chart_data = chart.chart_data or {}
        measures = chart_data.get('measures', [])
        self.assertEqual(len(measures), 2)
        first_notes = measures[0].get('notes', [])
        second_notes = measures[1].get('notes', [])
        self.assertEqual([note['at'] for note in first_notes], [0, 1000])
        self.assertEqual([note['at'] for note in second_notes], [2000, 2667])
        self.assertEqual(measures[0].get('bpm'), 120.0)
        self.assertEqual(measures[1].get('bpm'), 180.0)

    def test_strict_parser_records_long_events(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "long.events.tja"
        tja_path.write_text("\n".join([
            "TITLE:Longs",
            "COURSE:Oni",
            "LEVEL:9",
            "#START",
            "11,",
            "5000,",
            "0008,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertIn('oni', parsed.charts)
        course = parsed.charts['oni']
        self.assertGreater(course.total_notes, 0)
        chart_data = course.chart_data or {}
        measures = chart_data.get('measures', [])
        self.assertGreaterEqual(len(measures), 2)
        longs = [long for measure in measures for long in measure.get('longs', [])]
        self.assertTrue(longs)
        for long_note in longs:
            self.assertIn(long_note.get('kind'), {'drumroll', 'balloon'})
            self.assertIn('at', long_note)
            self.assertIn('end_at', long_note)
            self.assertIsInstance(long_note.get('big'), bool)

    def test_strict_auto_closes_overlapping_longs(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "overlap_longs.tja"
        tja_path.write_text("\n".join([
            "TITLE:Overlap",
            "COURSE:Oni",
            "LEVEL:5",
            "#START",
            "9000090000,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="WARNING") as logs:
            parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn('strict-long-start-overlap', joined_logs)
        self.assertNotIn('strict-long-end-without-start', joined_logs)
        self.assertNotIn('strict-long-without-end', joined_logs)

        course = parsed.charts['oni']
        chart_data = course.chart_data or {}
        measures = chart_data.get('measures', [])
        longs = [long for measure in measures for long in measure.get('longs', [])]
        self.assertEqual(len(longs), 2)
        for long_note in longs:
            self.assertIn('end_at', long_note)
            self.assertGreaterEqual(long_note['end_at'], long_note['at'] + 1)
        self.assertGreaterEqual(longs[0]['end_at'], longs[1]['at'])
        notes = [note for measure in measures for note in measure.get('notes', [])]
        self.assertFalse(any(note.get('synthetic') for note in notes))

    def test_strict_closes_longs_at_end_of_file(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "eof_long.tja"
        tja_path.write_text("\n".join([
            "TITLE:EOF Long",
            "COURSE:Oni",
            "LEVEL:4",
            "#START",
            "90000,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
            parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertNotIn('strict-long-without-end', joined_logs)

        course = parsed.charts['oni']
        chart_data = course.chart_data or {}
        measures = chart_data.get('measures', [])
        longs = [long for measure in measures for long in measure.get('longs', [])]
        self.assertEqual(len(longs), 1)
        long_note = longs[0]
        self.assertIn('end_at', long_note)
        self.assertGreaterEqual(long_note['end_at'], long_note['at'] + 1)

    def test_dojo_long_only_chart_has_synthetic_notes(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "dojo_long.tja"
        tja_path.write_text("\n".join([
            "TITLE:Dojo Long",
            "COURSE:Dojo",
            "LEVEL:1",
            "#START",
            "5000,",
            "0008,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
            parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn('end-notes(strict): course=dojo', joined_logs)
        self.assertIn('notes=1', joined_logs)
        self.assertIn('longs=1', joined_logs)

        course = parsed.charts['dojo']
        self.assertGreater(course.total_notes, 0)
        self.assertEqual(course.hit_notes, 0)
        chart_data = course.chart_data or {}
        measures = chart_data.get('measures', [])
        self.assertTrue(measures)
        notes = [note for measure in measures for note in measure.get('notes', [])]
        self.assertTrue(notes)
        synthetic_notes = [note for note in notes if note.get('synthetic')]
        self.assertEqual(len(synthetic_notes), len(notes))
        self.assertTrue(all(note['type'] == 'don' for note in synthetic_notes))
        longs = [long for measure in measures for long in measure.get('longs', [])]
        self.assertEqual(len(synthetic_notes), len(longs))

    def test_tower_long_only_chart_injects_synthetic_notes(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "tower_long.tja"
        tja_path.write_text("\n".join([
            "TITLE:Tower Long",
            "COURSE:Tower",
            "LEVEL:7",
            "#START",
            "5000,",
            "0008,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
            parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn('end-notes(strict): course=oni', joined_logs)
        self.assertIn('notes=1', joined_logs)
        self.assertIn('longs=1', joined_logs)

        course = parsed.charts['oni']
        self.assertEqual(course.normalised, 'TOWER')
        self.assertGreater(course.total_notes, 0)
        self.assertEqual(course.hit_notes, 0)
        chart_data = course.chart_data or {}
        measures = chart_data.get('measures', [])
        self.assertTrue(measures)
        notes = [note for measure in measures for note in measure.get('notes', [])]
        self.assertTrue(notes)
        synthetic_notes = [note for note in notes if note.get('synthetic')]
        self.assertEqual(len(synthetic_notes), len(notes))
        longs = [long for measure in measures for long in measure.get('longs', [])]
        self.assertEqual(len(longs), len(synthetic_notes))

    def test_standard_chart_with_hits_has_no_synthetic_notes(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "standard_hits.tja"
        tja_path.write_text("\n".join([
            "TITLE:Standard Hits",
            "COURSE:Oni",
            "LEVEL:3",
            "#START",
            "1111,",
            "2222,",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        course = parsed.charts['oni']
        self.assertGreater(course.hit_notes, 0)
        self.assertEqual(course.total_notes, course.hit_notes)
        chart_data = course.chart_data or {}
        notes = [note for measure in chart_data.get('measures', []) for note in measure.get('notes', [])]
        self.assertTrue(notes)
        self.assertFalse(any(note.get('synthetic') for note in notes))

    def test_lenient_fallback_injects_synthetic_notes_for_dojo(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "dojo_fallback.tja"
        tja_path.write_text("\n".join([
            "TITLE:Fallback Dojo",
            "COURSE:Dojo",
            "LEVEL:1",
            "#START",
            "5000,",
            "0008,",
            "#END",
        ]), encoding="utf-8")

        original_cleaner = songs_scanner.NOTE_TOKEN_CLEAN_RE

        class _FailingCleaner:
            def __init__(self, pattern):
                self._pattern = pattern

            def sub(self, repl, string):
                if string == "5000":
                    raise ValueError("forced parse failure")
                return self._pattern.sub(repl, string)

        with mock.patch.object(songs_scanner, "NOTE_TOKEN_CLEAN_RE", _FailingCleaner(original_cleaner)):
            with mock.patch.object(songs_scanner, "TJA_LENIENT_FALLBACK", True):
                with self.assertLogs(songs_scanner.LOGGER, level="INFO") as logs:
                    parsed = parse_tja(tja_path)

        joined_logs = "\n".join(logs.output)
        self.assertIn('fallback-to-lenient: file=', joined_logs)
        self.assertIn('end-notes(lenient): course=dojo', joined_logs)
        self.assertIn('notes=1', joined_logs)
        self.assertIn('longs=1', joined_logs)

        course = parsed.courses[0]
        self.assertEqual(course.canonical, 'dojo')
        self.assertGreater(course.total_notes, 0)
        self.assertEqual(course.hit_notes, 0)
        chart_data = course.chart_data or {}
        measures = chart_data.get('measures', [])
        notes = [note for measure in measures for note in measure.get('notes', [])]
        self.assertTrue(notes)
        self.assertTrue(all(note.get('synthetic') for note in notes))
        longs = [long for measure in measures for long in measure.get('longs', [])]
        self.assertEqual(len(notes), len(longs))

    def test_parse_tja_unknown_course_skips_chart(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = tmp_dir / "Unknown" / "unknown.tja"
        tja_path.parent.mkdir(parents=True, exist_ok=True)
        tja_path.write_text("\n".join([
            "TITLE:Unknown Course",
            "COURSE:Custom Alpha",
            "LEVEL:3",
            "#START",
            "1111,",
            "#END",
        ]), encoding="utf-8")

        with self.assertLogs(songs_scanner.LOGGER, level="WARNING") as logs:
            parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 0)
        self.assertEqual(parsed.skipped_charts, 1)
        self.assertTrue(any('Unknown COURSE "Custom Alpha" → skip chart block' in message for message in logs.output))

    def test_determine_category_from_directory(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        target_dir = songs_dir / "02 Anime" / "Artist"
        target_dir.mkdir(parents=True, exist_ok=True)
        chart_path = target_dir / "example.tja"
        chart_path.write_text("TITLE:Example", encoding="utf-8")

        scanner = SongScanner(
            db=_DummyDB(),
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        category_id, category_title = scanner._determine_category(chart_path.resolve())

        self.assertEqual(category_id, 2)
        self.assertEqual(category_title, "Anime")

    def test_scan_removes_null_characters_from_metadata(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        chart_dir = songs_dir / "01 Nulls"
        chart_dir.mkdir(parents=True, exist_ok=True)
        tja_path = chart_dir / "example.tja"
        tja_path.write_text(
            "TITLE:Bad\x00Title\u200b\n"
            "TITLEJA:\ufeffテ\u00a0スト\n"
            "SUBTITLE:Artist\x00\u00a0Name\n"
            "SUBTITLEJA:\u200c副題\n",
            encoding="utf-8",
        )

        collecting_db = _DummyDB()

        scanner = SongScanner(
            db=collecting_db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan()

        self.assertEqual(summary['inserted'], 1)
        inserted = collecting_db.songs.inserted[0]
        self.assertEqual(inserted['title'], 'BadTitle')
        self.assertEqual(inserted['subtitle'], 'Artist Name')
        self.assertEqual(inserted['titleJa'], 'テ スト')
        self.assertEqual(inserted['subtitleJa'], '副題')
        self.assertEqual(inserted['title_lang']['ja'], 'テ スト')
        self.assertEqual(inserted['subtitle_lang']['ja'], '副題')
        self.assertIn('locale', inserted)
        self.assertEqual(inserted['locale']['en']['title'], 'BadTitle')
        self.assertEqual(inserted['locale']['ja']['title'], 'テ スト')
        self.assertEqual(inserted['locale']['ja']['subtitle'], '副題')
        self.assertIn('charts', inserted)

    def test_fast_scan_skips_unchanged_files(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)
        tja_path = songs_dir / "song.tja"
        tja_path.write_text("TITLE:First\nWAVE:song.ogg\n", encoding="utf-8")
        audio_path = songs_dir / "song.ogg"
        audio_path.write_bytes(b"12345")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        first_summary = scanner.scan()
        self.assertEqual(first_summary['inserted'], 1)
        self.assertEqual(first_summary['skipped'], 0)

        second_summary = scanner.scan()
        self.assertEqual(second_summary['inserted'], 0)
        self.assertEqual(second_summary['updated'], 0)
        self.assertEqual(second_summary['skipped'], 1)

        audio_path.write_bytes(b"changed")
        third_summary = scanner.scan()
        self.assertEqual(third_summary['inserted'], 0)
        self.assertEqual(third_summary['updated'], 1)
        self.assertEqual(third_summary['disabled'], 0)
        self.assertEqual(third_summary['skipped'], 0)

    def test_scan_imports_dojo_chart_with_segments(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        dojo_dir = songs_dir / "Dojo" / "Second Dan"
        hls_dir = dojo_dir / "HLS"
        hls_dir.mkdir(parents=True, exist_ok=True)
        tja_path = dojo_dir / "dojo.tja"
        playlist_path = hls_dir / "dojo.t3u8"
        playlist_path.write_text("#EXTM3U\n", encoding="utf-8")
        tja_path.write_text("\n".join([
            "TITLE:Dojo Second Dan",
            "COURSE:Dan",
            "LEVEL:1",
            "#START",
            "1110,",
            "#NEXTSONG",
            "2220,",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        inserted = db.songs.inserted[0]
        self.assertTrue(inserted['enabled'])
        charts = inserted['charts']
        self.assertEqual(len(charts), 1)
        chart = charts[0]
        self.assertEqual(chart.get('mode'), 'standard')
        self.assertIn('mapped-course', chart.get('issues', []))
        self.assertTrue(chart['valid'])
        paths = inserted.get('paths', {})
        self.assertIn('audio_url', paths)
        self.assertTrue(paths['audio_url'].endswith('.t3u8'))

    def test_scan_imports_dan_chart_in_mvp_mode(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)
        audio_path = songs_dir / "dan.ogg"
        audio_path.write_bytes(b"dan-audio")
        tja_path = songs_dir / "dan_chart.tja"
        tja_path.write_text("\n".join([
            "TITLE:Trial Dan",
            "WAVE:dan.ogg",
            "COURSE:Dan",
            "LEVEL:8",
            "#START",
            "1111,",
            "#EXAM1 0 1 2",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan()

        self.assertEqual(summary['inserted'], 1)
        self.assertEqual(summary['errors'], 0)
        self.assertEqual(len(db.songs.inserted), 1)
        chart = db.songs.inserted[0]['charts'][0]
        self.assertEqual(chart.get('course'), 'oni')
        self.assertEqual(chart.get('canonical_course'), 'oni')
        self.assertEqual(chart.get('mode'), 'standard')
        self.assertTrue(chart.get('valid'))
        self.assertIn('mapped-course', chart.get('issues', []))

    def test_concurrent_upsert_same_chart(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path(self._tmp_dir()),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        chart = ChartRecord(
            course="oni",
            raw_course="Oni",
            normalised="oni",
            level=9,
            branch=False,
            valid=True,
            issues=[],
        )
        record = self._make_record(charts=[chart])
        key = compute_group_key(record)
        document = scanner._build_song_document(key, [record])
        charts_payload = list(document['charts'])

        def worker():
            local_summary = {'inserted': 0, 'updated': 0, 'errors': 0}
            scanner._upsert_song_document(key, [record], document, charts_payload, set(), local_summary)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(db.songs._docs), 1)
        charts = db.songs._docs[0].get('charts', [])
        self.assertEqual(len(charts), 1)
        self.assertEqual(db.songs._docs[0]['group_key'], key)
        self.assertIn('scanner_stable_id', db.songs._docs[0])
        self.assertTrue(db.songs._docs[0]['scanner_stable_id'])

    def test_upsert_updates_legacy_document_without_duplication(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path(self._tmp_dir()),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        chart = ChartRecord(
            course="oni",
            raw_course="Oni",
            normalised="oni",
            level=5,
            branch=False,
            valid=True,
            issues=[],
        )
        record = self._make_record(charts=[chart])
        key = compute_group_key(record)
        legacy_doc = {
            '_id': 101,
            'group_key': key,
            'title': 'Legacy Song',
            'charts': [],
        }
        db.songs.insert_one(dict(legacy_doc))
        document = scanner._build_song_document(key, [record])
        charts_payload = list(document['charts'])
        summary = {'inserted': 0, 'updated': 0, 'errors': 0}

        song_id = scanner._upsert_song_document(key, [record], document, charts_payload, {key}, summary)

        self.assertIsNotNone(song_id)
        self.assertEqual(len(db.songs._docs), 1)
        stored = db.songs._docs[0]
        self.assertEqual(stored['group_key'], key)
        self.assertEqual(stored['scanner_stable_id'], document['scanner_stable_id'])
        self.assertEqual(len(db.songs.inserted), 1)
        self.assertEqual(summary['errors'], 0)

    def test_upsert_update_document_has_no_conflicting_paths(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path(self._tmp_dir()),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        chart = ChartRecord(
            course="oni",
            raw_course="Oni",
            normalised="oni",
            level=6,
            branch=False,
            valid=True,
            issues=[],
        )
        record = self._make_record(charts=[chart])
        key = compute_group_key(record)
        document = scanner._build_song_document(key, [record])
        charts_payload = list(document['charts'])
        summary = {'inserted': 0, 'updated': 0, 'errors': 0}

        with mock.patch.object(db.songs, 'update_one', wraps=db.songs.update_one) as spy_update:
            scanner._upsert_song_document(key, [record], document, charts_payload, set(), summary)

        for call in spy_update.call_args_list:
            if len(call.args) < 2:
                continue
            update_doc = call.args[1]
            if '$set' in update_doc and '$setOnInsert' in update_doc:
                overlap = set(update_doc['$set']).intersection(update_doc['$setOnInsert'])
                self.assertFalse(overlap)

    def test_repeated_upsert_preserves_title_and_updates_charts(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path(self._tmp_dir()),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        chart = ChartRecord(
            course="oni",
            raw_course="Oni",
            normalised="oni",
            level=5,
            branch=False,
            valid=True,
            issues=[],
            total_notes=100,
        )
        record = self._make_record(charts=[chart])
        key = compute_group_key(record)
        document = scanner._build_song_document(key, [record])
        charts_payload = list(document['charts'])
        summary = {'inserted': 0, 'updated': 0, 'errors': 0}

        song_id = scanner._upsert_song_document(key, [record], document, charts_payload, set(), summary)
        self.assertIsNotNone(song_id)
        stored = db.songs._docs[0]
        original_title = stored['title']

        document2 = scanner._build_song_document(key, [record])
        document2['title'] = 'Modified Title'
        charts_payload2 = list(document2['charts'])
        charts_payload2[0]['total_notes'] = charts_payload2[0].get('total_notes', 0) + 25
        summary2 = {'inserted': 0, 'updated': 0, 'errors': 0}

        song_id_second = scanner._upsert_song_document(
            key,
            [record],
            document2,
            charts_payload2,
            {key},
            summary2,
        )

        self.assertEqual(song_id, song_id_second)
        self.assertEqual(db.songs._docs[0]['title'], original_title)
        charts_after = db.songs._docs[0]['charts']
        self.assertTrue(charts_after)
        self.assertEqual(charts_after[0].get('total_notes'), charts_payload2[0]['total_notes'])

    def test_write_error_code_40_logged_and_handled(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path(self._tmp_dir()),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        chart = ChartRecord(
            course="oni",
            raw_course="Oni",
            normalised="oni",
            level=4,
            branch=False,
            valid=True,
            issues=[],
        )
        record = self._make_record(charts=[chart])
        key = compute_group_key(record)
        document = scanner._build_song_document(key, [record])
        charts_payload = list(document['charts'])
        summary = {'inserted': 0, 'updated': 0, 'errors': 0}

        class _FakeWriteError(Exception):
            def __init__(self, code):
                super().__init__('write-error')
                self.code = code

        original_update = db.songs.update_one
        call_counter = {'count': 0}

        def _patched_update(filter_doc, update_doc, upsert=False, array_filters=None):
            call_counter['count'] += 1
            if call_counter['count'] == 4:
                raise songs_scanner.WriteError(40)
            return original_update(filter_doc, update_doc, upsert=upsert, array_filters=array_filters)

        with mock.patch.object(songs_scanner, 'WriteError', _FakeWriteError):
            with mock.patch.object(db.songs, 'update_one', side_effect=_patched_update):
                with self.assertLogs('songs_scanner', level='ERROR') as logs:
                    result = scanner._upsert_song_document(
                        key,
                        [record],
                        document,
                        charts_payload,
                        set(),
                        summary,
                    )

        self.assertIsNone(result)
        self.assertEqual(summary['errors'], 1)
        self.assertTrue(any('write-error-40: conflict at path' in entry for entry in logs.output))
    def test_scanner_seeds_legacy_stable_ids_on_startup(self):
        db = _DummyDB()
        tmp_dir = Path(self._tmp_dir())
        legacy_doc = {
            '_id': 33,
            'group_key': 'legacy-group',
            'title': 'Legacy Tune',
            'charts': [],
            'paths': {'tja_url': '/songs/legacy-group/main.tja'},
        }
        db.songs.insert_one(dict(legacy_doc))
        db.songs._docs[0].pop('scanner_stable_id', None)

        scanner = SongScanner(
            db=db,
            songs_dir=tmp_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        stored = db.songs._docs[0]
        self.assertIn('scanner_stable_id', stored)
        self.assertTrue(stored['scanner_stable_id'])
        expected = songs_scanner._make_deterministic_song_id([
            songs_scanner._normalise_song_fs_path('/songs/legacy-group/main.tja'),
            'Legacy Tune',
            '',
            '',
        ])
        self.assertEqual(stored['scanner_stable_id'], expected)
        self.assertGreater(scanner._metrics._counters['songs_seeded_legacy_total'], 0)

    def test_build_song_document_has_stable_id(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path(self._tmp_dir()),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        chart = ChartRecord(
            course="oni",
            raw_course="Oni",
            normalised="oni",
            level=9,
            branch=False,
            valid=True,
            issues=[],
        )
        record = self._make_record(charts=[chart])
        key = compute_group_key(record)

        doc_a = scanner._build_song_document(key, [record])
        doc_b = scanner._build_song_document(key, [record])

        self.assertIn('scanner_stable_id', doc_a)
        self.assertTrue(doc_a['scanner_stable_id'])
        self.assertEqual(doc_a['scanner_stable_id'], doc_b['scanner_stable_id'])

    def test_get_next_song_id_monotonic(self):
        tmp_dir = Path(self._tmp_dir())
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=tmp_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        allocated = [scanner._get_next_song_id() for _ in range(5)]

        self.assertEqual(allocated, [1, 2, 3, 4, 5])
        counter_docs = [doc for doc in db.counters._docs if doc.get('_id') == 'songs']
        self.assertEqual(counter_docs[0]['seq'], 5)

    def test_get_next_song_id_respects_existing_records(self):
        tmp_dir = Path(self._tmp_dir())
        db = _DummyDB()
        db.songs.insert_one({'id': 25})
        db.seq.update_one({'name': 'songs'}, {'$set': {'value': 10}})
        db.counters._docs[0]['seq'] = 0

        scanner = SongScanner(
            db=db,
            songs_dir=tmp_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        next_id = scanner._get_next_song_id()

        self.assertEqual(next_id, 26)
        counter_doc = db.counters.find_one({'_id': 'songs'})
        self.assertIsNotNone(counter_doc)
        self.assertEqual(counter_doc['seq'], 26)

    def test_get_next_song_id_clamps_after_stale_increment(self):
        tmp_dir = Path(self._tmp_dir())
        db = _DummyDB()
        db.songs.insert_one({'id': 40})
        db.seq.update_one({'name': 'songs'}, {'$set': {'value': 30}})
        db.counters._docs[0]['seq'] = 0

        scanner = SongScanner(
            db=db,
            songs_dir=tmp_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        original_update_one = db.counters.update_one
        call_count = {'value': 0}

        def flaky_update(filter_, update, upsert=False):
            call_count['value'] += 1
            if call_count['value'] == 1:
                raise RuntimeError('simulated failure')
            return original_update_one(filter_, update, upsert)

        db.counters.update_one = flaky_update  # type: ignore[assignment]

        try:
            next_id = scanner._get_next_song_id()
        finally:
            db.counters.update_one = original_update_one  # type: ignore[assignment]

        self.assertEqual(next_id, 41)
        counter_doc = db.counters.find_one({'_id': 'songs'})
        self.assertIsNotNone(counter_doc)
        self.assertEqual(counter_doc['seq'], 41)

    def test_concurrent_id_allocation_has_no_gaps(self):
        songs_dir = Path(self._tmp_dir())
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        worker_count = 4
        total_records = 24
        tasks = queue.Queue()
        assigned_ids: List[int] = []
        assigned_lock = threading.Lock()
        exceptions: List[BaseException] = []

        for index in range(total_records):
            chart = ChartRecord(
                course="oni",
                raw_course="Oni",
                normalised="oni",
                level=7,
                branch=False,
                valid=True,
                issues=[],
            )
            record = self._make_record(
                title=f"Concurrent {index}",
                normalized_title=f"concurrent-{index}",
                relative_path=f"Pack/concurrent_{index}.tja",
                tja_url=f"/songs/Pack/concurrent_{index}.tja",
                audio_url=f"/songs/Pack/concurrent_{index}.ogg",
                audio_path=f"Pack/concurrent_{index}.ogg",
                audio_hash=f"hash-{index}",
                tja_hash=f"tja-hash-{index}",
                fingerprint=f"fp-{index}",
                charts=[chart],
            )
            key = compute_group_key(record)
            document = scanner._build_song_document(key, [record])
            charts_payload = list(document['charts'])
            tasks.put((key, record, document, charts_payload))

        for _ in range(worker_count):
            tasks.put(None)

        class _LogCapture(logging.Handler):
            def __init__(self):
                super().__init__()
                self.records: List[logging.LogRecord] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.records.append(record)

        log_handler = _LogCapture()
        log_handler.setLevel(logging.DEBUG)
        songs_scanner.LOGGER.addHandler(log_handler)

        def worker() -> None:
            local_summary = {'inserted': 0, 'updated': 0, 'errors': 0}
            while True:
                item = tasks.get()
                try:
                    if item is None:
                        break
                    key, record, document, charts_payload = item
                    song_id = scanner._upsert_song_document(
                        key,
                        [record],
                        document,
                        charts_payload,
                        set(),
                        local_summary,
                    )
                    if song_id is not None:
                        with assigned_lock:
                            assigned_ids.append(song_id)
                except BaseException as exc:  # pragma: no cover - surfaced after threads join
                    exceptions.append(exc)
                finally:
                    tasks.task_done()

        threads = [threading.Thread(target=worker) for _ in range(worker_count)]
        for thread in threads:
            thread.start()

        tasks.join()
        for thread in threads:
            thread.join()

        songs_scanner.LOGGER.removeHandler(log_handler)

        if exceptions:
            raise exceptions[0]

        duplicate_logs = [record for record in log_handler.records if 'DuplicateKeyError' in record.getMessage()]
        self.assertFalse(duplicate_logs, f"DuplicateKeyError logs found: {[r.getMessage() for r in duplicate_logs]}")

        self.assertEqual(len(db.songs._docs), total_records)
        ids = sorted(doc.get('id') for doc in db.songs._docs if doc.get('id') is not None)
        self.assertEqual(ids, list(range(1, total_records + 1)))
        self.assertEqual(len(assigned_ids), total_records)
        self.assertEqual(sorted(assigned_ids), ids)
        counter_value = next(doc['seq'] for doc in db.counters._docs if doc.get('_id') == 'songs')
        self.assertEqual(counter_value, total_records)

    def test_scan_merges_charts_for_shared_wave(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        track_dir = songs_dir / "Pack"
        track_dir.mkdir(parents=True, exist_ok=True)
        audio_path = track_dir / "shared.ogg"
        audio_path.write_bytes(b"audio-bytes")
        chart_one = track_dir / "easy.tja"
        chart_two = track_dir / "oni.tja"
        chart_one.write_text("\n".join([
            "TITLE:Shared Song",
            "WAVE:shared.ogg",
            "COURSE:Easy",
            "LEVEL:3",
            "#START",
            "1,",
            "#END",
        ]), encoding="utf-8")
        chart_two.write_text("\n".join([
            "TITLE:Shared Song",
            "WAVE:shared.ogg",
            "COURSE:Oni",
            "LEVEL:8",
            "#START",
            "1,",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)
        self.assertEqual(summary['inserted'], 1)
        self.assertEqual(len(db.songs._docs), 1)
        charts = db.songs._docs[0]['charts']
        courses = {chart['canonical_course'] for chart in charts}
        self.assertEqual(courses, {'easy', 'oni'})

    def test_upsert_retries_on_duplicate_key(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path(self._tmp_dir()),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        chart = ChartRecord(
            course="easy",
            raw_course="Easy",
            normalised="easy",
            level=3,
            branch=False,
            valid=True,
            issues=[],
        )
        record = self._make_record(charts=[chart])
        key = compute_group_key(record)
        document = scanner._build_song_document(key, [record])
        charts_payload = list(document['charts'])

        original_update = db.songs.update_one
        original_find_and_update = db.songs.find_one_and_update
        call_state = {'update_duplicates': 0, 'retry_invocations': 0}

        class FakeDuplicate(Exception):
            pass

        def flaky_update(filter_, update, upsert=False, **kwargs):
            if upsert and call_state['update_duplicates'] == 0:
                call_state['update_duplicates'] += 1
                raise FakeDuplicate()
            return original_update(filter_, update, upsert=upsert, **kwargs)

        def tracking_find_one_and_update(*args, **kwargs):
            call_state['retry_invocations'] += 1
            return original_find_and_update(*args, **kwargs)

        summary = {'inserted': 0, 'updated': 0, 'errors': 0}

        with mock.patch('songs_scanner.DuplicateKeyError', FakeDuplicate):
            db.songs.update_one = flaky_update
            db.songs.find_one_and_update = tracking_find_one_and_update
            try:
                song_id = scanner._upsert_song_document(key, [record], document, charts_payload, set(), summary)
            finally:
                db.songs.update_one = original_update
                db.songs.find_one_and_update = original_find_and_update

        self.assertIsNotNone(song_id)
        self.assertEqual(summary['errors'], 0)
        self.assertEqual(len(db.songs._docs), 1)
        self.assertGreaterEqual(call_state['retry_invocations'], 1)

    def test_repeat_scan_keeps_song_count(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)
        audio_path = songs_dir / "loop.ogg"
        audio_path.write_bytes(b"loop")
        tja_path = songs_dir / "loop.tja"
        tja_path.write_text("\n".join([
            "TITLE:Loop Song",
            "WAVE:loop.ogg",
            "COURSE:Normal",
            "LEVEL:5",
            "#START",
            "1,",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        first_summary = scanner.scan(full=True)
        self.assertEqual(first_summary['inserted'], 1)
        self.assertEqual(len(db.songs._docs), 1)
        first_charts = list(db.songs._docs[0]['charts'])

        second_summary = scanner.scan(full=True)
        self.assertEqual(second_summary['inserted'], 0)
        self.assertEqual(len(db.songs._docs), 1)
        second_charts = db.songs._docs[0]['charts']
        self.assertEqual(len(second_charts), len(first_charts))
        def _strip_updated_at(charts):
            return [
                {key: value for key, value in chart.items() if key != 'updatedAt'}
                for chart in charts
            ]

        self.assertEqual(_strip_updated_at(second_charts), _strip_updated_at(first_charts))

    def test_course_alias_normalization(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = Path(tmp_dir) / "alias.tja"
        tja_path.write_text("\n".join([
            "TITLE:Alias Test",
            "WAVE:dummy.ogg",
            "COURSE:Kara-Kuchi",
            "LEVEL:4",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)
        courses = {course.canonical: course for course in parsed.courses}

        self.assertIn("normal", courses)
        self.assertEqual(courses["normal"].stars, 4)

    def test_parse_tja_handles_comments_and_placeholders(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = Path(tmp_dir) / "comments.tja"
        tja_path.write_text("\n".join([
            "\ufeffTITLE:Comment Test",
            "WAVE:dummy.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "...",
            "1,0 // inline comment",
            "200; semicolon comment",
            "; full line comment",
            ",,,",
            "#END",
        ]), encoding="utf-8-sig")

        parsed = parse_tja(tja_path)

        self.assertEqual(len(parsed.courses), 1)
        course = parsed.courses[0]
        self.assertEqual(course.start_blocks, 1)
        self.assertEqual(course.end_blocks, 1)
        self.assertEqual(course.total_notes, 2)
        self.assertEqual(course.hit_notes, 2)
        self.assertEqual(course.first_note_preview, "1,0")

    def test_parse_tja_preserves_metadata_with_comment_markers(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = Path(tmp_dir) / "markers.tja"
        tja_path.write_text("\n".join([
            "TITLE:Semicolon;Title",
            "WAVE:http://cdn.example.com/song.ogg",
            "COURSE:Oni",
            "LEVEL:5",
            "#START",
            "1 // comment",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)

        self.assertEqual(parsed.title, "Semicolon;Title")
        self.assertEqual(parsed.wave, "http://cdn.example.com/song.ogg")
        course = parsed.courses[0]
        self.assertEqual(course.total_notes, 1)

    def test_parse_tja_allows_safe_directives_between_measures(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = Path(tmp_dir) / "directives.tja"
        tja_path.write_text("\n".join([
            "TITLE:Directive Test",
            "WAVE:dummy.ogg",
            "COURSE:Oni",
            "LEVEL:6",
            "#START",
            "1110,",
            "#BPMCHANGE 72.5",
            "#MEASURE 3/4",
            "2220,",
            "#SCROLL 0.75",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)
        self.assertEqual(len(parsed.courses), 1)
        course = parsed.courses[0]
        self.assertEqual(course.start_blocks, 1)
        self.assertEqual(course.end_blocks, 1)
        self.assertEqual(course.total_notes, 6)
        self.assertEqual(course.hit_notes, 6)

    def test_parse_tja_maps_numeric_and_taste_aliases(self):
        tmp_dir = Path(self._tmp_dir())
        tja_path = Path(tmp_dir) / "aliases.tja"
        tja_path.write_text("\n".join([
            "TITLE:Alias Test",
            "WAVE:dummy.ogg",
            "COURSE:0",
            "LEVEL:1",
            "#START",
            "1",
            "#END",
            "COURSE:辛口",
            "LEVEL:4",
            "#START",
            "1",
            "#END",
            "COURSE:4",
            "LEVEL:9",
            "#START",
            "1",
            "#END",
            "COURSE:7",
            "LEVEL:1",
            "#START",
            "1",
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)
        courses = {course.canonical: course for course in parsed.courses}
        self.assertIn("easy", courses)
        self.assertIn("normal", courses)
        self.assertIn("uraoni", courses)
        self.assertEqual(parsed.skipped_charts, 1)

    def test_resolve_course_downcasts_tower_to_oni(self):
        tmp_dir = Path(self._tmp_dir())

        oni_path = Path(tmp_dir) / "tower" / "tower.tja"
        oni_path.parent.mkdir(parents=True, exist_ok=True)
        oni_path.write_text("\n".join([
            "TITLE:Tower Oni",
            "WAVE:dummy.ogg",
            "COURSE:Tower",
            "LEVEL:8",
            "#START",
            "1",
            "#END",
        ]), encoding="utf-8")

        easy_path = Path(tmp_dir) / "Tower Ama" / "chart.tja"
        easy_path.parent.mkdir(parents=True, exist_ok=True)
        easy_path.write_text("\n".join([
            "TITLE:Tower Easy",
            "WAVE:dummy.ogg",
            "COURSE:Tower",
            "LEVEL:2",
            "#START",
            "1",
            "#END",
        ]), encoding="utf-8")

        normal_path = Path(tmp_dir) / "Tower" / "Tower Kara.tja"
        normal_path.parent.mkdir(parents=True, exist_ok=True)
        normal_path.write_text("\n".join([
            "TITLE:Tower Normal",
            "WAVE:dummy.ogg",
            "COURSE:Tower",
            "LEVEL:4",
            "#START",
            "1",
            "#END",
        ]), encoding="utf-8")

        oni_course = parse_tja(oni_path).courses[0]
        easy_course = parse_tja(easy_path).courses[0]
        normal_course = parse_tja(normal_path).courses[0]

        self.assertEqual(oni_course.canonical, "oni")
        self.assertEqual(easy_course.canonical, "oni")
        self.assertEqual(normal_course.canonical, "oni")
        self.assertIn("mapped-course", oni_course.issues)
        self.assertIn("mapped-course", easy_course.issues)
        self.assertIn("mapped-course", normal_course.issues)

    def test_scanner_merges_multiple_tja_into_single_song(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)
        audio_path = songs_dir / "song.ogg"
        audio_path.write_bytes(b"audio-bytes")

        easy_tja = songs_dir / "easy.tja"
        easy_tja.write_text("\n".join([
            "TITLE:Merge Easy",
            "WAVE:song.ogg",
            "COURSE:Easy",
            "LEVEL:3",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        oni_tja = songs_dir / "oni.tja"
        oni_tja.write_text("\n".join([
            "TITLE:Merge Oni",
            "WAVE:song.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "2,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        self.assertEqual(len(db.songs.inserted), 1)
        inserted = db.songs.inserted[0]
        self.assertEqual(inserted['title'], 'Merge Easy')
        self.assertIn('charts', inserted)
        courses = {chart['canonical_course']: chart for chart in inserted['charts']}
        self.assertIn('easy', courses)
        self.assertIn('oni', courses)
        self.assertEqual(inserted.get('valid_chart_count'), 2)
        self.assertEqual(inserted.get('genre'), 'Unsorted')
        self.assertTrue(all(chart.get('total_notes', 0) > 0 for chart in inserted['charts']))

        # Second scan should not duplicate charts
        followup_summary = scanner.scan(full=False)
        self.assertEqual(followup_summary['updated'], 0)
        self.assertEqual(followup_summary['skipped'], 2)
        existing = db.songs._docs[0]
        self.assertEqual(len(existing['charts']), 2)

    def test_scanner_marks_duplicate_courses(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)
        audio_path = songs_dir / "dup.ogg"
        audio_path.write_bytes(b"duplicate-audio")

        first_tja = songs_dir / "oni_a.tja"
        first_tja.write_text("\n".join([
            "TITLE:Duplicate Oni",
            "WAVE:dup.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        second_tja = songs_dir / "oni_b.tja"
        second_tja.write_text("\n".join([
            "TITLE:Duplicate Oni",
            "WAVE:dup.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        inserted = db.songs.inserted[0]
        self.assertEqual(len(inserted['charts']), 1)
        self.assertIn('duplicate_course', inserted.get('import_issues', []))
        self.assertEqual(inserted['charts'][0]['course'], 'oni')
        self.assertEqual(inserted['charts'][0]['canonical_course'], 'oni')
        self.assertIn('duplicate-course', inserted['charts'][0]['issues'])

    def test_scanner_groups_tower_flavour_files_into_single_song(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        pack_dir = songs_dir / "Tower Pack"
        pack_dir.mkdir(parents=True, exist_ok=True)
        audio_path = pack_dir / "tower.ogg"
        audio_path.write_bytes(b"tower-audio")

        ama_tja = pack_dir / "Tower Ama.tja"
        ama_tja.write_text("\n".join([
            "TITLE:Tower Ama",
            "WAVE:tower.ogg",
            "COURSE:Tower",
            "LEVEL:2",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        kara_tja = pack_dir / "Tower Kara.tja"
        kara_tja.write_text("\n".join([
            "TITLE:Tower Kara",
            "WAVE:tower.ogg",
            "COURSE:Tower",
            "LEVEL:4",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        inserted = db.songs.inserted[0]
        courses = {chart['canonical_course'] for chart in inserted['charts']}
        self.assertEqual(courses, {'oni'})
        self.assertIn('duplicate_course', inserted.get('import_issues', []))
        chart = inserted['charts'][0]
        self.assertIn('mapped-course', chart.get('issues', []))

    def test_scanner_skips_unknown_courses(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        pack_dir = songs_dir / "Custom Pack"
        pack_dir.mkdir(parents=True, exist_ok=True)

        audio_path = pack_dir / "shared.ogg"
        audio_path.write_bytes(b"shared-audio")

        alpha_tja = pack_dir / "alpha.tja"
        alpha_tja.write_text("\n".join([
            "TITLE:Unknown Alpha",
            "WAVE:shared.ogg",
            "COURSE:Custom Alpha",
            "LEVEL:5",
            "#START",
            "1111,",
            "#END",
        ]), encoding="utf-8")

        beta_tja = pack_dir / "beta.tja"
        beta_tja.write_text("\n".join([
            "TITLE:Unknown Beta",
            "WAVE:shared.ogg",
            "COURSE:Custom Beta",
            "LEVEL:7",
            "#START",
            "2222,",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        inserted = db.songs.inserted[0]
        self.assertNotIn('duplicate_course', inserted.get('import_issues', []))
        self.assertIn('no-courses', inserted.get('import_issues', []))
        self.assertIn('no-valid-course', inserted.get('import_issues', []))
        self.assertEqual(inserted['charts'], [])
        self.assertEqual(scanner._metrics._counters['tja_skipped_charts_total'], 2)

    def test_scanner_atomic_upsert_same_chart_twice(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)

        audio_path = songs_dir / "shared.ogg"
        audio_path.write_bytes(b"shared-audio")

        tja_path = songs_dir / "oni.tja"
        tja_path.write_text("\n".join([
            "TITLE:Concurrent Oni",
            "WAVE:shared.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        first_summary = scanner.scan(full=True)
        self.assertEqual(first_summary['inserted'], 1)

        second_scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        second_summary = second_scanner.scan(full=True)
        self.assertEqual(second_summary['inserted'], 0)

        docs = list(db.songs.find())
        self.assertEqual(len(docs), 1)
        charts = docs[0].get('charts', [])
        self.assertEqual(len(charts), 1)
        self.assertEqual(charts[0]['course'], 'oni')
        self.assertEqual(charts[0]['canonical_course'], 'oni')

    def test_scanner_atomic_upsert_merges_distinct_courses(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)

        audio_path = songs_dir / "shared.ogg"
        audio_path.write_bytes(b"shared-audio")

        easy_tja = songs_dir / "easy.tja"
        easy_tja.write_text("\n".join([
            "TITLE:Concurrent Easy",
            "WAVE:shared.ogg",
            "COURSE:Easy",
            "LEVEL:3",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        oni_tja = songs_dir / "oni.tja"
        oni_tja.write_text("\n".join([
            "TITLE:Concurrent Oni",
            "WAVE:shared.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        first_summary = scanner.scan(full=True)
        self.assertEqual(first_summary['inserted'], 1)

        second_scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )
        second_scanner.scan(full=True)

        docs = list(db.songs.find())
        self.assertEqual(len(docs), 1)
        courses = sorted(chart['canonical_course'] for chart in docs[0].get('charts', []))
        self.assertEqual(courses, ['easy', 'oni'])

    def test_scanner_repeated_scan_is_idempotent(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)

        audio_path = songs_dir / "shared.ogg"
        audio_path.write_bytes(b"shared-audio")

        easy_tja = songs_dir / "easy.tja"
        easy_tja.write_text("\n".join([
            "TITLE:Idempotent Easy",
            "WAVE:shared.ogg",
            "COURSE:Easy",
            "LEVEL:3",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        oni_tja = songs_dir / "oni.tja"
        oni_tja.write_text("\n".join([
            "TITLE:Idempotent Oni",
            "WAVE:shared.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        first_summary = scanner.scan(full=True)
        self.assertEqual(first_summary['inserted'], 1)

        docs_after_first = list(db.songs.find())
        charts_after_first = sum(len(doc.get('charts', [])) for doc in docs_after_first)

        second_summary = scanner.scan(full=False)

        docs_after_second = list(db.songs.find())
        charts_after_second = sum(len(doc.get('charts', [])) for doc in docs_after_second)

        self.assertEqual(len(docs_after_second), len(docs_after_first))
        self.assertEqual(charts_after_second, charts_after_first)
        self.assertEqual(second_summary['inserted'], 0)
        self.assertEqual(second_summary['updated'], 0)

    def test_scanner_handles_realistic_tower_taste_pair(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        track_dir = songs_dir / "Taiko Tower 01"
        track_dir.mkdir(parents=True, exist_ok=True)

        audio_path = track_dir / "Metronome Track 1.ogg"
        audio_path.write_bytes(b"tower-audio-bytes")

        ama_tja = track_dir / "Taiko Tower 1 Ama-kuchi.tja"
        ama_tja.write_text("\n".join([
            "//TJADB Project",
            "TITLE:Taiko Tower 1 Ama-kuchi",
            "SUBTITLE:--Metronome Track 1",
            "BPM:70",
            "WAVE:Metronome Track 1.ogg",
            "OFFSET:-3.531",
            "DEMOSTART:3.531",
            "LIFE:5",
            "",
            "COURSE:Tower",
            "LEVEL:1",
            "SCOREINIT:3500",
            "SCOREDIFF:610",
            "",
            "",
            "#START",
            "",
            "",
            "1110,",
            "#BPMCHANGE 72.5",
            "1110,",
            "#BPMCHANGE 75",
            "1110,",
            "#BPMCHANGE 77.5",
            "2220,",
            "#BPMCHANGE 80",
            "1110,",
            "#BPMCHANGE 82.5",
            "1110,",
            "#BPMCHANGE 85",
            "1110,",
            "#BPMCHANGE 87.5",
            "2220,",
            "#BPMCHANGE 90",
            "1011,",
            "#BPMCHANGE 92.5",
            "1011,",
            "#BPMCHANGE 95",
            "1011,",
            "#BPMCHANGE 97.5",
            "1,",
            "#BPMCHANGE 100",
            "1011,",
            "#BPMCHANGE 102.5",
            "1011,",
            "#BPMCHANGE 105",
            "1022,",
            "#BPMCHANGE 107.5",
            "2,",
            "#BPMCHANGE 110",
            "1011,",
            "#BPMCHANGE 112.5",
            "1011,",
            "#BPMCHANGE 115",
            "1011,",
            "#BPMCHANGE 117.5",
            "1,",
            "#BPMCHANGE 120",
            "1011,",
            "#BPMCHANGE 122.5",
            "1011,",
            "#BPMCHANGE 125",
            "1022,",
            "#BPMCHANGE 127.5",
            "2,",
            "#BPMCHANGE 130",
            "1110,",
            "#BPMCHANGE 132.5",
            "2220,",
            "#BPMCHANGE 135",
            "1011,",
            "#BPMCHANGE 137.5",
            "1,",
            "#BPMCHANGE 140",
            "1110,",
            "#BPMCHANGE 142.5",
            "2220,",
            "#BPMCHANGE 145",
            "1011,",
            "#BPMCHANGE 147.5",
            "500000000000000000000000000008000000000000000000,",
            "#MEASURE 2/4",
            "#BPMCHANGE 150",
            "3,",
            "",
            "",
            "#END",
            "",
        ]), encoding="utf-8")

        kara_tja = track_dir / "Taiko Tower 1 Kara-kuchi.tja"
        kara_tja.write_text("\n".join([
            "//TJADB Project",
            "TITLE:Taiko Tower 1 Kara-kuchi",
            "SUBTITLE:--Metronome Track 1",
            "BPM:70",
            "WAVE:Metronome Track 1.ogg",
            "OFFSET:-3.531",
            "DEMOSTART:3.531",
            "LIFE:5",
            "",
            "COURSE:Tower",
            "LEVEL:1",
            "SCOREINIT:2300",
            "SCOREDIFF:500",
            "",
            "",
            "#START",
            "",
            "",
            "1011100010000000,",
            "#BPMCHANGE 72.5",
            "1011100010000000,",
            "#BPMCHANGE 75",
            "1011100010111000,",
            "#BPMCHANGE 77.5",
            "1011100030000000,",
            "#BPMCHANGE 80",
            "1011100010000000,",
            "#BPMCHANGE 82.5",
            "1011100010000000,",
            "#BPMCHANGE 85",
            "1011100010111000,",
            "#BPMCHANGE 87.5",
            "2022200030000000,",
            "#BPMCHANGE 90",
            "1011100010000000,",
            "#BPMCHANGE 92.5",
            "1011100010000000,",
            "#BPMCHANGE 95",
            "1011100010111000,",
            "#BPMCHANGE 97.5",
            "1011100030000000,",
            "#BPMCHANGE 100",
            "1011100010000000,",
            "#BPMCHANGE 102.5",
            "1011100010000000,",
            "#BPMCHANGE 105",
            "1011100010111000,",
            "#BPMCHANGE 107.5",
            "2022200030000000,",
            "#BPMCHANGE 110",
            "1011100020000000,",
            "#BPMCHANGE 112.5",
            "11221000,",
            "#BPMCHANGE 115",
            "1110100020000000,",
            "#BPMCHANGE 117.5",
            "11103000,",
            "#BPMCHANGE 120",
            "1011100020000000,",
            "#BPMCHANGE 122.5",
            "11221000,",
            "#BPMCHANGE 125",
            "1110100020000000,",
            "#BPMCHANGE 127.5",
            "11103000,",
            "#BPMCHANGE 130",
            "10101110,",
            "#BPMCHANGE 132.5",
            "22201110,",
            "#BPMCHANGE 135",
            "10101110,",
            "#BPMCHANGE 137.5",
            "22203000,",
            "#BPMCHANGE 140",
            "10101110,",
            "#BPMCHANGE 142.5",
            "11102220,",
            "#BPMCHANGE 145",
            "10101110,",
            "#BPMCHANGE 147.5",
            "500000000000000000000000000008000000000000000000,",
            "#MEASURE 2/4",
            "#BPMCHANGE 150",
            "3,",
            "",
            "",
            "#END",
            "",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        inserted = db.songs.inserted[0]
        self.assertIn('duplicate_course', inserted.get('import_issues', []))
        self.assertEqual(inserted.get('valid_chart_count'), 1)

        self.assertEqual(len(inserted['charts']), 1)
        chart = inserted['charts'][0]
        self.assertEqual(chart['course'], 'oni')
        self.assertEqual(chart['canonical_course'], 'oni')
        self.assertTrue(chart['valid'])
        self.assertIn('duplicate-course', chart.get('issues', []))
        self.assertIn('mapped-course', chart.get('issues', []))
        self.assertGreater(chart['hit_notes'], 0)
        self.assertEqual(chart['total_notes'], chart['hit_notes'])
        self.assertTrue(chart.get('first_note_preview', '').startswith(('1110', '1011')))

    def test_determine_group_key_prefers_audio_hash_and_folder(self):
        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=Path("/unused"),
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        record_with_hash = TjaImportRecord(
            relative_path="pack/oni.tja",
            relative_dir="Pack/OniCourse",
            tja_url="/songs/pack/oni.tja",
            dir_url="/songs/pack/",
            audio_url="/songs/audio.ogg",
            audio_path="pack/audio.ogg",
            audio_hash="deadbeef",
            audio_mtime_ns=None,
            audio_size=None,
            music_type=None,
            diagnostics=[],
            title="Oni Title",
            title_ja=None,
            subtitle="",
            subtitle_ja=None,
            locale={},
            offset=0.0,
            preview=0.0,
            fingerprint="",
            tja_hash="hash",
            wave="audio.ogg",
            song_id=None,
            genre=None,
            category_id=0,
            category_title="Unsorted",
            charts=[],
            import_issues=[],
            normalized_title="oni title",
        )

        key_with_hash = scanner._determine_group_key(record_with_hash)
        self.assertEqual(key_with_hash, "audio:deadbeef:pack")

        record_missing_audio = TjaImportRecord(
            relative_path="pack/oni.tja",
            relative_dir="Pack/OniCourse",
            tja_url="/songs/pack/oni.tja",
            dir_url="/songs/pack/",
            audio_url=None,
            audio_path=None,
            audio_hash=None,
            audio_mtime_ns=None,
            audio_size=None,
            music_type=None,
            diagnostics=[],
            title="Fallback Title",
            title_ja=None,
            subtitle="",
            subtitle_ja=None,
            locale={},
            offset=0.0,
            preview=0.0,
            fingerprint="",
            tja_hash="hash2",
            wave=None,
            song_id=None,
            genre=None,
            category_id=0,
            category_title="Unsorted",
            charts=[],
            import_issues=[],
            normalized_title="",
        )

        key_missing_audio = scanner._determine_group_key(record_missing_audio)
        self.assertTrue(key_missing_audio.startswith("missing:pack:fallback title:"))
        missing_suffix = key_missing_audio.split(":")[-1]
        self.assertEqual(len(missing_suffix), 32)

    def test_compute_group_key_normalises_variants(self):
        base_kwargs = dict(
            relative_path="Pack%20Name/Sub/Filename.tja",
            relative_dir="Pack%20Name\\Sub",
            tja_url="/songs/Pack%20Name/Sub/Filename.tja",
            dir_url="/songs/Pack%20Name/Sub/",
            audio_url="/songs/audio.ogg",
            audio_path="Pack%20Name/Sub/audio.ogg",
            audio_hash="deadbeef",
            audio_mtime_ns=None,
            audio_size=None,
            music_type=None,
            diagnostics=[],
            title="Normalize Test",
            title_ja=None,
            subtitle="",
            subtitle_ja=None,
            locale={},
            offset=0.0,
            preview=0.0,
            fingerprint="fp",
            tja_hash="hash",
            wave="audio.ogg",
            song_id=None,
            genre=None,
            category_id=0,
            category_title="Unsorted",
            charts=[],
            import_issues=[],
            normalized_title="normalize test",
        )
        record_a = TjaImportRecord(**base_kwargs)
        variant_kwargs = dict(base_kwargs)
        variant_kwargs.update(
            {
                'relative_path': "pack name//sub//filename.tja",
                'relative_dir': "PACK%20NAME/sub\\\\",
            }
        )
        record_b = TjaImportRecord(**variant_kwargs)

        key_a = compute_group_key(record_a)
        key_b = compute_group_key(record_b)

        self.assertEqual(key_a, "audio:deadbeef:pack name")
        self.assertEqual(key_a, key_b)

    def test_compute_group_key_folder_token_consistency(self):
        base_record = self._make_record()
        variants = [
            self._make_record(dir_url="songs\\PACK\\", relative_dir="Pack\\"),
            self._make_record(dir_url="http://example.com/songs/Pack%20/", relative_dir=" pack "),
            self._make_record(dir_url="Pack ", relative_dir="PACK////"),
        ]
        keys = {compute_group_key(base_record)}
        keys.update(compute_group_key(record) for record in variants)
        self.assertEqual(len(keys), 1)
        key = keys.pop()
        self.assertTrue(key.startswith("audio:hash123:pack"))

    def test_compute_group_key_missing_audio_dirty_inputs(self):
        record = self._make_record(
            audio_hash=None,
            dir_url="file:///Pack%20Folder\\",
            relative_dir=" Pack Folder\\",
            relative_path="Pack Folder\\Chart.tja",
            normalized_title="Dirty Title",
            title="Dirty Title",
        )
        key = compute_group_key(record)
        self.assertTrue(key.startswith("missing:pack folder:dirty title:"))
        suffix = key.split(":")[-1]
        self.assertEqual(len(suffix), 32)

    def test_scanner_normalizes_alias_courses_and_genre_fallback(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        track_dir = songs_dir / "Taiko Tower 01"
        track_dir.mkdir(parents=True, exist_ok=True)
        audio_path = track_dir / "tower.ogg"
        audio_path.write_bytes(b"tower-audio")
        tja_path = track_dir / "tower.tja"
        tja_path.write_text("\n".join([
            "TITLE:Tower Mix",
            "WAVE:tower.ogg",
            "COURSE:Tower",
            "LEVEL:7",
            "#START",
            "...",
            "1,0",
            "#END",
            "COURSE:Ama-kuchi",
            "LEVEL:2",
            "#START",
            "1,0",
            "#END",
            "COURSE:Kara-kuchi",
            "LEVEL:4",
            "#START",
            "1,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        inserted = db.songs.inserted[0]
        courses = {chart['canonical_course']: chart for chart in inserted['charts']}
        self.assertIn('oni', courses)
        self.assertIn('easy', courses)
        self.assertIn('normal', courses)
        self.assertTrue(courses['oni']['valid'])
        self.assertTrue(courses['easy']['valid'])
        self.assertTrue(courses['normal']['valid'])
        self.assertNotIn('unknown-course', inserted.get('import_issues', []))
        self.assertEqual(inserted.get('genre'), 'Taiko Tower 01')
        self.assertEqual(inserted.get('category_id'), 0)
        self.assertEqual(inserted.get('valid_chart_count'), 3)

    def test_scanner_flags_empty_chart(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)
        tja_path = songs_dir / "empty.tja"
        tja_path.write_text("\n".join([
            "TITLE:Empty Chart",
            "WAVE:missing.ogg",
            "COURSE:Oni",
            "LEVEL:5",
            "#START",
            "0,0",
            "#END",
        ]), encoding="utf-8")

        db = _DummyDB()
        scanner = SongScanner(
            db=db,
            songs_dir=songs_dir,
            songs_baseurl="/songs/",
            ignore_globs=None,
        )

        summary = scanner.scan(full=True)

        self.assertEqual(summary['inserted'], 1)
        inserted = db.songs.inserted[0]
        chart = inserted['charts'][0]
        self.assertFalse(chart['valid'])
        self.assertIn('empty-chart', chart['issues'])
        self.assertIn('empty-chart', inserted['import_issues'])
        self.assertEqual(chart.get('total_notes'), 0)
        self.assertEqual(chart.get('hit_notes'), 0)
        issues = db.import_issues._docs
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['reason'], 'empty_chart')
        self.assertEqual(issues[0]['path'], 'empty.tja')
        self.assertEqual(issues[0]['course_raw'], 'Oni')
        self.assertEqual(issues[0].get('first_note_preview'), '0,0')

    def _tmp_dir(self):
        return tempfile.mkdtemp()


if __name__ == "__main__":
    unittest.main()
