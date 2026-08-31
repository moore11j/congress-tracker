from app.services.seo_snapshots import _member_slugify, normalize_snapshot_key


def test_member_snapshot_slugs_match_underscore_member_urls():
    assert _member_slugify("Nancy Pelosi") == "NANCY_PELOSI"
    assert normalize_snapshot_key("member", "Nancy Pelosi") == "NANCY_PELOSI"


def test_member_snapshot_slug_removes_punctuation_without_creating_hyphen_urls():
    assert _member_slugify("Mary-Jane O'Neil") == "MARYJANE_ONEIL"
