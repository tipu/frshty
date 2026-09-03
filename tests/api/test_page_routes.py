"""Every HTML page route renders.

web/pages.py reads a template file off disk and rewrites its <title>. Nothing
else exercised these routes, so a renamed or deleted template, or a template
that lost its title tag, reached production as a 500 or an untitled tab. These
tests walk the app's own route table, so a page added later is covered without
touching this file.
"""
import ast
import sys
from pathlib import Path

import pytest

import core.log as log
import core.state as state
import web.pages as pages


PLACEHOLDERS = {"repo": "myrepo", "pr_id": "1", "idx": "0", "key": "PROJ-1"}


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")
    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        if "frshty" in sys.modules:
            frshty = sys.modules["frshty"]
        else:
            import frshty
    finally:
        sys.argv = saved_argv
    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config({
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {"root": tmp_path, "tickets_dir": "tickets",
                      "ticket_layout": "flat", "base_branch": "main"},
        "features": {}, "pr": {}, "slack": {},
        "_config_path": tmp_path / "config.toml", "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    })
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def _page_routes():
    """Every path web/pages.py registers with @router.get, read from the source.

    Reading the decorators keeps collection free of an app import, and a page
    added to web/pages.py later is picked up without editing this file.
    """
    tree = ast.parse(Path(pages.__file__).read_text())
    routes = []
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for d in node.decorator_list:
            if not (isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
                    and d.func.attr == "get" and getattr(d.func.value, "id", "") == "router"):
                continue
            if d.args and isinstance(d.args[0], ast.Constant):
                routes.append(d.args[0].value)
    return sorted(set(routes))


def _page_paths():
    """The registered paths with their path parameters filled in."""
    paths = []
    for route in _page_routes():
        url = route
        for name, value in PLACEHOLDERS.items():
            url = url.replace("{" + name + "}", value)
        assert "{" not in url, f"no placeholder for a path parameter in {route}"
        paths.append(url)
    return paths


class TestEveryPageRenders:
    def test_the_route_table_is_not_empty(self, client):
        assert len(_page_paths()) >= 20

    @pytest.mark.parametrize("path", _page_paths())
    def test_page_returns_html(self, client, path):
        r = client.get(path)
        assert r.status_code == 200, f"{path} -> {r.status_code}"
        assert "text/html" in r.headers["content-type"]
        assert "<title>" in r.text

    @pytest.mark.parametrize("path", _page_paths())
    def test_page_title_carries_the_instance_name(self, client, path):
        r = client.get(path)
        title = r.text.split("<title>", 1)[1].split("</title>", 1)[0]
        assert title.startswith("Test - "), f"{path} title not rewritten: {title!r}"


class TestTemplateFilesExist:
    def test_every_template_named_in_pages_is_on_disk(self):
        tree = ast.parse(Path(pages.__file__).read_text())
        names = [n.args[0].value for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_template"
                 and n.args and isinstance(n.args[0], ast.Constant)]
        assert names, "no _template calls found; the scan is broken"
        missing = [n for n in names if not (pages.TEMPLATES_DIR / n).is_file()]
        assert missing == [], f"templates referenced but absent: {missing}"


class TestTitleRewrite:
    def test_bare_frshty_title_becomes_dashboard(self):
        assert pages._rewrite_title("<title>frshty</title>") == "<title>Test - Dashboard</title>"

    def test_suffixed_title_keeps_the_page_name(self):
        assert (pages._rewrite_title("<title>Tickets — frshty</title>")
                == "<title>Test - Tickets</title>")

    def test_only_the_first_title_is_rewritten(self):
        out = pages._rewrite_title("<title>frshty</title><title>frshty</title>")
        assert out == "<title>Test - Dashboard</title><title>frshty</title>"

    def test_no_title_tag_is_left_alone(self):
        assert pages._rewrite_title("<h1>hello</h1>") == "<h1>hello</h1>"
