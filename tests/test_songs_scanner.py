from pathlib import Path
import sys
import tempfile
import unittest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from songs_scanner import SongScanner, parse_tja


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
            "#END",
        ]), encoding="utf-8")

        parsed = parse_tja(tja_path)
        courses = {course.canonical: course for course in parsed.courses}

        self.assertIn("Normal", courses)
        self.assertEqual(courses["Normal"].stars, 4)

    def test_scanner_merges_multiple_tja_into_single_song(self):
        tmp_dir = Path(self._tmp_dir())
        songs_dir = tmp_dir / "songs"
        songs_dir.mkdir(parents=True, exist_ok=True)
        audio_path = songs_dir / "song.ogg"
        audio_path.write_bytes(b"audio-bytes")

        easy_tja = songs_dir / "easy.tja"
        easy_tja.write_text("\n".join([
            "TITLE:Merge Test",
            "WAVE:song.ogg",
            "COURSE:Easy",
            "LEVEL:3",
            "#START",
            "#END",
        ]), encoding="utf-8")

        oni_tja = songs_dir / "oni.tja"
        oni_tja.write_text("\n".join([
            "TITLE:Merge Test",
            "WAVE:song.ogg",
            "COURSE:Oni",
            "LEVEL:7",
            "#START",
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
        self.assertIn('charts', inserted)
        courses = {chart['course']: chart for chart in inserted['charts']}
        self.assertIn('Easy', courses)
        self.assertIn('Oni', courses)
        self.assertEqual(inserted.get('valid_chart_count'), 2)

    def _tmp_dir(self):
        return tempfile.mkdtemp()


if __name__ == "__main__":
    unittest.main()
