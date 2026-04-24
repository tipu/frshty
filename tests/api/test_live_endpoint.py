"""Unit tests for the /api/jobs/{id}/live endpoint offset/UTF-8 semantics.

Imports the raw _trim_to_utf8_boundary helper directly rather than spinning
up the FastAPI app — that's where the non-obvious logic lives. A couple of
integration tests for the endpoint's 404/200 behavior are kept separate.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import frshty  # noqa: E402

_trim = frshty._trim_to_utf8_boundary


class TestUtf8Boundary:
    def test_empty(self):
        assert _trim(b"") == b""

    def test_ascii_only(self):
        assert _trim(b"hello") == b"hello"

    def test_complete_2byte_sequence(self):
        # £ = U+00A3 = 0xC2 0xA3
        assert _trim(b"\xc2\xa3") == b"\xc2\xa3"

    def test_split_2byte_sequence_trims_leading_byte(self):
        assert _trim(b"abc\xc2") == b"abc"

    def test_complete_3byte_sequence(self):
        # € = U+20AC = 0xE2 0x82 0xAC
        assert _trim(b"\xe2\x82\xac") == b"\xe2\x82\xac"

    def test_split_3byte_sequence_1_of_3(self):
        assert _trim(b"x\xe2") == b"x"

    def test_split_3byte_sequence_2_of_3(self):
        assert _trim(b"x\xe2\x82") == b"x"

    def test_complete_4byte_sequence_emoji(self):
        # 🎉 = U+1F389 = 0xF0 0x9F 0x8E 0x89
        assert _trim(b"\xf0\x9f\x8e\x89") == b"\xf0\x9f\x8e\x89"

    def test_split_4byte_sequence_trims(self):
        assert _trim(b"ok\xf0\x9f\x8e") == b"ok"

    def test_preserves_prefix_when_trimming(self):
        # Plain ASCII followed by an incomplete multi-byte suffix
        assert _trim(b"line1\nline2\n\xe2\x82") == b"line1\nline2\n"
