from pathlib import Path
import sys
import tempfile
import unittest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from songs_scanner import SongScanner, TjaImportRecord, parse_tja


class _MemoryCollection:
    def __init__(self):
        self._docs = []

    def create_index(self, *args, **kwargs):
        return None

    def _matches(self, doc, filter_):
        if not filter_:
            return True
        for key, expected in filter_.items():
            value = self._resolve_key(doc, key)
            if isinstance(expected, dict):
                if '$in' in expected and value not in expected['$in']:
                    return False
                if '$nin' in expected and value in expected['$nin']:
                    return False
            else:
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
        matches = [doc for doc in self._docs if self._matches(doc, filter_ or {})]
        if sort:
            for key, direction in reversed(sort):
                reverse = direction < 0
                matches.sort(key=lambda doc, k=key: self._resolve_key(doc, k), reverse=reverse)
        if not matches:
            return None
        return self._project(matches[0], projection or {})

    def find(self, filter_=None, projection=None):
        for doc in list(self._docs):
            if self._matches(doc, filter_ or {}):
                yield self._project(doc, projection or {})

    def insert_one(self, document):
        self._docs.append(dict(document))

    def update_one(self, filter_, update, upsert=False):
        for doc in self._docs:
            if self._matches(doc, filter_ or {}):
                if '$set' in update:
                    doc.update(update['$set'])
                return
        if upsert and '$set' in update:
            new_doc = dict(update['$set'])
            if filter_:
                for key, value in filter_.items():
                    if isinstance(value, dict):
                        continue
                    new_doc[key] = value
            self._docs.append(new_doc)

    def delete_many(self, filter_):
        self._docs = [doc for doc in self._docs if not self._matches(doc, filter_ or {})]


class _SeqCollection(_MemoryCollection):
    def __init__(self):
        super().__init__()
        self._docs = [{'name': 'songs', 'value': 0}]

    def find_one(self, filter_=None, projection=None):
        return super().find_one(filter_, projection)

    def update_one(self, filter_, update, upsert=False):
        for doc in self._docs:
            if self._matches(doc, filter_ or {}):
                if '$set' in update:
                    doc.update(update['$set'])
                return
        if upsert:
            super().update_one(filter_, update, upsert=True)


class _SongsCollection(_MemoryCollection):
    def __init__(self):
        super().__init__()
        self.inserted = []

    def insert_one(self, document):
        copy = dict(document)
        self.inserted.append(copy)
        super().insert_one(copy)


class _DummyDB:
    def __init__(self):
        self.seq = _SeqCollection()
        self.songs = _SongsCollection()
        self.categories = _MemoryCollection()
        self.song_scanner_state = _MemoryCollection()
        self.import_issues = _MemoryCollection()


class TestSongsScanner(unittest.TestCase):
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
        self.assertEqual(courses["Oni"].stars, 8)
        self.assertTrue(courses["Oni"].branch)
        self.assertEqual(courses["Hard"].stars, 5)

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
        self.assertEqual(third_summary['updated'], 1)
        self.assertEqual(third_summary['skipped'], 0)

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

        self.assertIn("Normal", courses)
        self.assertEqual(courses["Normal"].stars, 4)

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
        self.assertEqual(course.total_notes, 4)
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
        self.assertGreaterEqual(course.total_notes, 8)
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
        self.assertIn("Easy", courses)
        self.assertIn("Normal", courses)
        self.assertIn("UraOni", courses)
        unknown_courses = [course for course in parsed.courses if course.canonical == "Unknown"]
        self.assertTrue(unknown_courses)
        self.assertIn("unknown_course_numeric", unknown_courses[0].issues)

    def test_resolve_course_uses_path_markers_for_tower(self):
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

        self.assertEqual(oni_course.canonical, "Oni")
        self.assertEqual(easy_course.canonical, "Easy")
        self.assertEqual(normal_course.canonical, "Normal")

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
        courses = {chart['course']: chart for chart in inserted['charts']}
        self.assertIn('Easy', courses)
        self.assertIn('Oni', courses)
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
        self.assertEqual(inserted['charts'][0]['course'], 'Oni')

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
        courses = {chart['course'] for chart in inserted['charts']}
        self.assertIn('Easy', courses)
        self.assertIn('Normal', courses)
        self.assertNotIn('duplicate_course', inserted.get('import_issues', []))

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
        self.assertEqual(key_with_hash, "audio:deadbeef:Pack")

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
        self.assertTrue(key_missing_audio.startswith("missing:Pack:"))
        self.assertIn("fallback title", key_missing_audio)

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
        courses = {chart['course']: chart for chart in inserted['charts']}
        self.assertIn('Oni', courses)
        self.assertIn('Easy', courses)
        self.assertIn('Normal', courses)
        self.assertTrue(courses['Oni']['valid'])
        self.assertTrue(courses['Easy']['valid'])
        self.assertTrue(courses['Normal']['valid'])
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
        issues = db.import_issues._docs
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['reason'], 'empty_chart')
        self.assertEqual(issues[0]['path'], 'empty.tja')
        self.assertEqual(issues[0]['course_raw'], 'Oni')
        self.assertEqual(issues[0].get('after_start_token'), '0,0')

    def _tmp_dir(self):
        return tempfile.mkdtemp()


if __name__ == "__main__":
    unittest.main()
