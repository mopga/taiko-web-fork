import json
from pathlib import Path


def _load_artifact_templates():
    package_json = Path('standalone/electron/package.json')
    payload = json.loads(package_json.read_text(encoding='utf-8'))
    build = payload.get('build', {})
    win = (build.get('win') or {}).get('artifactName')
    mac = (build.get('mac') or {}).get('artifactName')
    return {
        'win': win,
        'mac': mac,
    }


def _render_artifact_name(template: str, version: str, ext: str) -> str:
    return template.replace('${version}', version).replace('${ext}', ext)


def _glob_pattern(template: str, ext: str) -> str:
    return template.replace('${version}', '*').replace('${ext}', ext)


def test_artifact_templates_include_version_and_ext_placeholders():
    templates = _load_artifact_templates()
    for platform, template in templates.items():
        assert template, f"missing artifactName for {platform}"
        assert '${version}' in template
        assert '${ext}' in template


def test_rendered_artifact_names_follow_template():
    templates = _load_artifact_templates()
    sample_version = '1.2.3'
    rendered = {
        platform: _render_artifact_name(template, sample_version, 'exe' if platform == 'win' else 'dmg')
        for platform, template in templates.items()
    }
    assert rendered['win'].endswith('.exe')
    assert sample_version in rendered['win']
    assert sample_version in rendered['mac']


def test_dist_glob_pattern_matches_when_files_present(tmp_path, monkeypatch):
    templates = _load_artifact_templates()
    dist_root = Path('standalone/electron/dist')
    if not dist_root.exists():
        return

    expected_exts = {
        'win': {'exe', 'zip'},
        'mac': {'dmg', 'zip'},
    }

    for platform, template in templates.items():
        for ext in expected_exts.get(platform, set()):
            pattern = _glob_pattern(template, ext)
            matches = list(dist_root.glob(pattern))
            assert matches, f"expected at least one {platform} artifact for pattern {pattern}"
