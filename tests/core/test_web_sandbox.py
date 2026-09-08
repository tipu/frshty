import pytest

from web.origin import origin_is_opaque
from web.sandbox import DATA, PAGE, policy_for


class TestSandboxPolicy:
    @pytest.mark.parametrize("name", [
        "report.html", "REPORT.HTML", "change-explainer.htm", "a.b.html",
    ])
    def test_a_page_may_run_its_own_scripts(self, name):
        assert policy_for(name) == PAGE
        assert "allow-scripts" in policy_for(name)

    @pytest.mark.parametrize("name", [
        "shot.png", "notes.md", "data.json", "clip.webm", "diagram.svg",
        "report.html.txt", "", "htm", "html",
    ])
    def test_everything_else_runs_nothing(self, name):
        assert policy_for(name) == DATA
        assert policy_for(name) == "sandbox"

    def test_no_policy_grants_the_board_origin(self):
        for policy in (DATA, PAGE):
            assert policy.split()[0] == "sandbox"
            assert "allow-same-origin" not in policy

    def test_a_page_policy_covers_links_dialogs_and_downloads(self):
        for token in ("allow-scripts", "allow-modals", "allow-popups",
                      "allow-popups-to-escape-sandbox", "allow-downloads"):
            assert token in PAGE

    def test_a_page_cannot_reach_the_board(self):
        for directive in ("connect-src blob: data:", "frame-src 'none'",
                          "object-src 'none'"):
            assert directive in PAGE
        assert "allow-forms" not in PAGE
        assert "http" not in PAGE


class TestWebSocketOrigin:
    @pytest.mark.parametrize("origin", ["null", "NULL", " null "])
    def test_a_document_with_no_origin_of_its_own_is_refused(self, origin):
        assert origin_is_opaque(origin) is True

    @pytest.mark.parametrize("origin", [
        "", "https://personal.frshty.localhost", "https://personal.frshty.local",
        "https://personal.frshty.danialjaffry.com", "http://127.0.0.1:7100",
        "null.example", "https://null",
    ])
    def test_every_real_client_is_let_through(self, origin):
        assert origin_is_opaque(origin) is False
