import unittest
from pathlib import Path

from songs_scanner import SongScanner, parse_tja


class _DummyCollection:
    def find_one(self, *args, **kwargs):
        return None

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
                "SUBTITLE:Artist",
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
        self.assertEqual(parsed.subtitle, "Artist")
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

    def _tmp_dir(self):
        import tempfile

        return tempfile.mkdtemp()


if __name__ == "__main__":
    unittest.main()
