"""Regression: _extract_ticket must handle lowercase branch prefixes."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_extract_ticket_case_insensitive():
    from features.timesheet import _extract_ticket
    # Lowercase branch (common in Bitbucket/GitHub) must still resolve
    assert _extract_ticket("dev-432-browser-login-token-flow") == "DEV-432"
    # Prefixed lowercase variant
    assert _extract_ticket("jwd-dev-446-email-verification-status") == "DEV-446"
    # Mixed case
    assert _extract_ticket("Dev-123-something") == "DEV-123"
    # Already-uppercase still works
    assert _extract_ticket("DEV-789-foo") == "DEV-789"
    # Nothing matches
    assert _extract_ticket("no-ticket-here") == ""
    # Multiple matches: returns first (leftmost ticket-like token)
    assert _extract_ticket("dev-1-merge-DEV-2") == "DEV-1"


if __name__ == "__main__":
    test_extract_ticket_case_insensitive()
    print("test_extract_ticket_case_insensitive: PASS")
