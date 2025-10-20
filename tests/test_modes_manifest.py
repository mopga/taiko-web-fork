import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from modes_manifest import build_modes_manifest, determine_category_mode, MODE_DEFINITIONS


def test_determine_category_mode_from_explicit_field():
    category = {"title": "Taiko Towers", "mode": "tower"}

    assert determine_category_mode(category) == "tower"


def test_determine_category_mode_from_title_hint():
    category = {"title": "Dan Dojo"}

    assert determine_category_mode(category) == "dandojo"


def test_build_modes_manifest_aggregates_categories():
    categories = [
        {"title": "J-POP"},
        {"title": "Taiko Towers", "mode": "tower"},
        {"title": "Dan Dojo", "mode": "dandojo"},
    ]

    manifest = build_modes_manifest(categories)

    assert manifest["status"] == "ok"
    modes = {entry["key"]: entry for entry in manifest["modes"]}
    assert set(modes.keys()) == set(MODE_DEFINITIONS.keys())
    assert modes["standard"]["categories"] == ["J-POP"]
    assert modes["tower"]["categories"] == ["Taiko Towers"]
    assert modes["dandojo"]["categories"] == ["Dan Dojo"]
