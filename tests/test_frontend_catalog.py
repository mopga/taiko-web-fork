from pathlib import Path


def test_no_details_calls_in_catalog():
    loader_path = Path(__file__).resolve().parents[1] / 'public/src/js/loader.js'
    contents = loader_path.read_text(encoding='utf-8')
    assert 'const USE_DETAILS_IN_CATALOG = 0' in contents
    assert 'details-batcher disabled for catalog' in contents
    assert 'api/songs/details' not in contents
    # Manual smoke checklist (no automated coverage available):
    # 1. Load the catalog and verify the Network tab shows no api/songs/details?ids requests.
    # 2. Scroll through catalog sections and ensure the Network tab remains free of api/songs/details calls.
