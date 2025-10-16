from pathlib import Path
import sys
import tempfile
import unittest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from songs_scanner import SongScanner, parse_tja


class _DummyCollection:
    def find_one(self, *args, **kwargs):
        return None

    def find(self, *args, **kwargs):
        return []

    def update_one(self, *args, **kwargs):
        return None

    def insert_one(self, *args, **kwargs):
        return None


class _DummyDB:
    def __init__(self):
        self.seq = _DummyCollection()
        self.songs = _DummyCollection()
        self.categories = _DummyCollection()


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
        self.assertEqual(parsed.courses["oni"].stars, 8)
        self.assertTrue(parsed.courses["oni"].branch)
        self.assertEqual(parsed.courses["hard"].stars, 5)

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

        class _SeqCollection(_DummyCollection):
            def __init__(self):
                self.value = 0

            def find_one(self, *args, **kwargs):
                if args and args[0].get('name') == 'songs':
                    return {'name': 'songs', 'value': self.value}
                return None

            def update_one(self, *args, **kwargs):
                update = args[1]
                self.value = update.get('$set', {}).get('value', self.value)

        class _SongsCollection(_DummyCollection):
            def __init__(self):
                self.inserted = []

            def insert_one(self, document):
                self.inserted.append(document.copy())

        collecting_db = _DummyDB()
        collecting_db.seq = _SeqCollection()
        collecting_db.songs = _SongsCollection()

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

    def _tmp_dir(self):
        return tempfile.mkdtemp()


if __name__ == "__main__":
    unittest.main()
