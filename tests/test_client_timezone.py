"""Stored timestamps are UTC. The pages must render them in the reader's zone.

A UTC instant reaches the browser as an ISO string with an offset. Cutting the
clock out of that string with slice() prints UTC on the page, which is what put
"16:29:00" on a timeline the reader lives at 09:29:00 in. Naming a fixed zone
in toLocaleString does the same thing to everyone outside that zone. Both are
banned; window.frshtyTime is the one render layer.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
TEMPLATES = sorted((ROOT / "templates").glob("*.html"))
SCRIPTS = sorted((ROOT / "static").glob("*.js"))

BANNED = [
    (re.compile(r"toISOString\(\)\s*\.\s*slice\("),
     "toISOString().slice() prints UTC; use window.frshtyTime instead"),
    (re.compile(r"\.slice\(\s*11\s*[,)]"),
     "slicing an ISO string at index 11 prints the UTC clock"),
    (re.compile(r"\.slice\(\s*0\s*,\s*19\s*\)"),
     "slicing an ISO string to 19 chars prints the UTC stamp"),
    (re.compile(r"""replace\(\s*['"]T['"]\s*,\s*['"] ['"]\s*\)"""),
     "swapping the T of an ISO string prints UTC"),
    (re.compile(r"""timeZone:\s*['"][A-Za-z]+/"""),
     "a hard-coded IANA zone is not the reader's zone"),
]


# The billing grid models a week as UTC-midnight Date objects standing for
# calendar days, never for instants, so reading their UTC parts back out is the
# whole point. Nothing else earns a waiver.
ALLOWED = {("templates/billing.html", "fmtDate")}


def _offending(path, text):
    name = path.relative_to(ROOT).as_posix()
    hits = []
    for number, line in enumerate(text.splitlines(), 1):
        if any(f == name and token in line for f, token in ALLOWED):
            continue
        for pattern, reason in BANNED:
            if pattern.search(line):
                hits.append((number, reason, line.strip()[:110]))
    return hits


def test_no_page_formats_a_timestamp_in_utc():
    failures = []
    for path in TEMPLATES + SCRIPTS:
        for number, reason, line in _offending(path, path.read_text()):
            failures.append(f"{path.relative_to(ROOT)}:{number} {reason}\n    {line}")
    assert not failures, "\n".join(failures)


def test_every_page_that_loads_the_app_loads_the_time_helper():
    missing = [
        path.relative_to(ROOT).as_posix()
        for path in TEMPLATES
        if "frshty-nav.js" in path.read_text()
        and "frshty-time.js" not in path.read_text()
    ]
    assert not missing, f"pages without the time helper: {missing}"
