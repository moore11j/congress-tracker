"""Complete-submission boundary behavior must survive the scanning optimization."""
import random
import re

import pytest

from app.clients import direct_sources
from test_direct_sec_collection import META, submission


def regex_sections(text, opening, closing):
    return iter(re.findall(re.escape(opening) + r"(.*?)" + re.escape(closing), text, re.S))


@pytest.mark.parametrize("opening,closing", [("<DOCUMENT>", "</DOCUMENT>"), ("<XML>", "</XML>")])
def test_nested_incomplete_and_repeated_boundaries_match_old_scan(opening, closing):
    rng = random.Random(617)
    tokens = [opening, closing, "", "content", "\n", "\u2003", "<OTHER>"]
    cases = ["", opening, closing, opening + closing, opening * 2 + closing * 2]
    cases += ["".join(rng.choices(tokens, k=30)) for _ in range(500)]
    for text in cases:
        assert list(direct_sources._submission_sections(text, opening, closing)) == list(regex_sections(text, opening, closing))


def outcome(raw):
    try:
        return ("accepted", direct_sources.parse_13f_submission(raw, META))
    except Exception as exc:
        return ("rejected", type(exc).__name__, str(exc), getattr(exc, "details", None))


def submission_cases():
    raw = submission()
    yield raw
    # Preserve the old regular expression's Unicode whitespace trimming.
    for whitespace in ["", " \t\n\r", "\u2003\u0085\x1c", "\u00a0"]:
        w = whitespace.encode()
        yield raw.replace(b"<XML>", b"<XML>" + w).replace(b"</XML>", w + b"</XML>")
    for suffix in [
        b"<DOCUMENT><FILENAME>empty.xml\n<XML></XML></DOCUMENT>",
        b"<DOCUMENT><FILENAME>empty.xml\n<XML> \n </XML></DOCUMENT>",
        b"<DOCUMENT><FILENAME>bad.xml\n<XML><broken></XML></DOCUMENT>",
        b"<DOCUMENT><FILENAME>../bad.xml\n<XML><extra/></XML></DOCUMENT>",
        b"<DOCUMENT><FILENAME>../ignored.xml\nNo XML</DOCUMENT>",
        b"<DOCUMENT><FILENAME>incomplete.xml\n<XML><extra/>",
        b"<DOCUMENT><FILENAME>first.xml\n<XML><extra/></XML><XML><broken></XML></DOCUMENT>",
    ]:
        yield raw + suffix
    # Duplicate cover/table and truncated envelopes must keep their rejection behavior.
    for block in re.findall(b"<DOCUMENT>.*?</DOCUMENT>", raw, re.S):
        yield raw + block
    yield raw.replace(b"</DOCUMENT>", b"", 1)
    yield raw.replace(b"<XML>", b"<xml>", 1)
    yield raw.replace(b"<value>100</value>", b"<value>-1</value>", 1)
    yield raw.replace(b"<tableValueTotal>300", b"<tableValueTotal>301", 1)


@pytest.mark.parametrize("raw", list(submission_cases()))
def test_full_result_or_rejection_matches_regex_scan(raw, monkeypatch):
    candidate = outcome(raw)
    monkeypatch.setattr(direct_sources, "_submission_sections", regex_sections)
    assert outcome(raw) == candidate


def test_xml_trim_matches_original_capture():
    for whitespace in ["", " \t\n\r", "\u2003\u0085\x1c", "\u00a0"]:
        text = "<XML>" + whitespace + "<root> value </root>" + whitespace + "</XML>"
        assert next(direct_sources._submission_sections(text, "<XML>", "</XML>")).strip() == re.search(r"<XML>\s*(.*?)\s*</XML>", text, re.S)[1]
