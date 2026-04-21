from unittest.mock import patch, MagicMock

from features.ticket_systems import make_ticket_system, _adf_to_text, JiraTicketSystem, LinearTicketSystem


class TestMakeTicketSystem:
    def test_jira(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"base_url": "http://j"}}
        ts = make_ticket_system(config)
        assert isinstance(ts, JiraTicketSystem)

    def test_linear(self):
        config = {"job": {"ticket_system": "linear"}, "linear": {}}
        ts = make_ticket_system(config)
        assert isinstance(ts, LinearTicketSystem)

    def test_unknown(self):
        config = {"job": {"ticket_system": "unknown"}}
        assert make_ticket_system(config) is None

    def test_empty(self):
        config = {"job": {}}
        assert make_ticket_system(config) is None


class TestAdfToText:
    def test_none_input(self):
        assert _adf_to_text(None) == ""

    def test_string_input(self):
        assert _adf_to_text("not a dict") == ""

    def test_empty_doc(self):
        assert _adf_to_text({"type": "doc", "content": []}) == ""

    def test_paragraph(self):
        adf = {"type": "doc", "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Hello world"}]}
        ]}
        assert "Hello world" in _adf_to_text(adf)

    def test_heading(self):
        adf = {"type": "doc", "content": [
            {"type": "heading", "content": [{"type": "text", "text": "Title"}]}
        ]}
        assert "Title" in _adf_to_text(adf)

    def test_code_block(self):
        adf = {"type": "doc", "content": [
            {"type": "codeBlock", "content": [{"type": "text", "text": "x = 1"}]}
        ]}
        result = _adf_to_text(adf)
        assert "```" in result
        assert "x = 1" in result

    def test_ordered_list(self):
        adf = {"type": "doc", "content": [
            {"type": "orderedList", "content": [
                {"type": "listItem", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "first"}]}]},
                {"type": "listItem", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "second"}]}]},
            ]}
        ]}
        result = _adf_to_text(adf)
        assert "1. first" in result
        assert "2. second" in result

    def test_bullet_list(self):
        adf = {"type": "doc", "content": [
            {"type": "bulletList", "content": [
                {"type": "listItem", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "item"}]}]},
            ]}
        ]}
        result = _adf_to_text(adf)
        assert "- item" in result

    def test_table(self):
        adf = {"type": "doc", "content": [
            {"type": "table", "content": [
                {"type": "tableRow", "content": [
                    {"type": "tableCell", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "cell"}]}]}
                ]}
            ]}
        ]}
        assert "cell" in _adf_to_text(adf)

    def test_nested_structure(self):
        adf = {"type": "doc", "content": [
            {"type": "paragraph", "content": [
                {"type": "text", "text": "Hello "},
                {"type": "text", "text": "world"},
            ]}
        ]}
        result = _adf_to_text(adf)
        assert "Hello world" in result


class TestJiraFetchTickets:
    def test_missing_credentials_returns_empty(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"base_url": ""}}
        ts = JiraTicketSystem(config)
        assert ts.fetch_tickets() == []

    def test_missing_board_and_jql_returns_empty(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"base_url": "http://j", "user": "u", "token": "t"}}
        ts = JiraTicketSystem(config)
        assert ts.fetch_tickets() == []

    def test_board_fetch_normalizes(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"base_url": "http://j", "user": "u", "token": "t", "board_id": 1}}
        ts = JiraTicketSystem(config)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"issues": [{
            "key": "PROJ-1",
            "fields": {
                "summary": "Test ticket",
                "status": {"name": "In Progress"},
                "description": None,
                "attachment": [],
                "issuelinks": [],
                "subtasks": [],
                "timeoriginalestimate": 3600,
            }
        }]}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            tickets = ts.fetch_tickets()
        assert len(tickets) == 1
        assert tickets[0]["key"] == "PROJ-1"
        assert tickets[0]["status"] == "In Progress"
        assert tickets[0]["estimate_seconds"] == 3600

    def test_non_200_returns_empty(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"base_url": "http://j", "user": "u", "token": "t", "board_id": 1}}
        ts = JiraTicketSystem(config)
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            assert ts.fetch_tickets() == []


class TestLinearFetchTickets:
    def test_missing_token_returns_empty(self):
        config = {"job": {"ticket_system": "linear"}, "linear": {"assignee_email": "a@b.com"}}
        ts = LinearTicketSystem(config)
        assert ts.fetch_tickets() == []

    def test_missing_email_returns_empty(self):
        config = {"job": {"ticket_system": "linear"}, "linear": {"token": "tok"}}
        ts = LinearTicketSystem(config)
        assert ts.fetch_tickets() == []

    def test_graphql_normalizes(self):
        config = {"job": {"ticket_system": "linear"}, "linear": {"token": "tok", "assignee_email": "a@b.com"}}
        ts = LinearTicketSystem(config)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"issues": {"nodes": [{
            "identifier": "LIN-1",
            "title": "Linear ticket",
            "state": {"name": "In Progress"},
            "description": "desc",
            "url": "http://linear.app/1",
            "project": None,
            "parent": None,
            "attachments": {"nodes": []},
            "relations": {"nodes": []},
            "children": {"nodes": []},
        }]}}}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            tickets = ts.fetch_tickets()
        assert len(tickets) == 1
        assert tickets[0]["key"] == "LIN-1"
        assert tickets[0]["summary"] == "Linear ticket"


class TestJiraFetchComments:
    def _make_ts(self):
        config = {"job": {"ticket_system": "jira"},
                  "jira": {"base_url": "http://j", "user": "u", "token": "t", "board_id": 1}}
        return JiraTicketSystem(config)

    def test_missing_credentials_returns_empty(self):
        config = {"job": {"ticket_system": "jira"}, "jira": {"base_url": ""}}
        ts = JiraTicketSystem(config)
        assert ts.fetch_comments("PROJ-1") == []

    def test_missing_key_returns_empty(self):
        assert self._make_ts().fetch_comments("") == []

    def test_normalizes(self):
        ts = self._make_ts()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"comments": [{
            "id": "10001",
            "author": {"displayName": "Alice"},
            "body": {"type": "doc", "content": [
                {"type": "paragraph", "content": [{"type": "text", "text": "First"}]}
            ]},
            "created": "2026-04-20T18:00:00.000+0000",
        }, {
            "id": "10002",
            "author": {"displayName": "Bob"},
            "body": {"type": "doc", "content": [
                {"type": "paragraph", "content": [{"type": "text", "text": "Second"}]}
            ]},
            "created": "2026-04-21T09:00:00.000+0000",
        }]}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            comments = ts.fetch_comments("PROJ-1")
        assert len(comments) == 2
        assert comments[0] == {"id": "10001", "author": "Alice", "body": "First\n\n",
                               "created_at": "2026-04-20T18:00:00.000+0000"}
        assert comments[1]["author"] == "Bob"
        call_url = mock_client.get.call_args[0][0]
        assert "/rest/api/3/issue/PROJ-1/comment" in call_url

    def test_non_200_returns_empty(self):
        ts = self._make_ts()
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            assert ts.fetch_comments("PROJ-1") == []

    def test_exception_returns_empty(self):
        ts = self._make_ts()
        with patch("features.ticket_systems.httpx.Client", side_effect=Exception("boom")):
            assert ts.fetch_comments("PROJ-1") == []


class TestLinearFetchComments:
    def _make_ts(self):
        config = {"job": {"ticket_system": "linear"},
                  "linear": {"token": "tok", "assignee_email": "a@b.com"}}
        return LinearTicketSystem(config)

    def test_missing_token_returns_empty(self):
        config = {"job": {"ticket_system": "linear"}, "linear": {"assignee_email": "a@b.com"}}
        assert LinearTicketSystem(config).fetch_comments("LIN-1") == []

    def test_missing_key_returns_empty(self):
        assert self._make_ts().fetch_comments("") == []

    def test_normalizes(self):
        ts = self._make_ts()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"issue": {"comments": {"nodes": [
            {"id": "c1", "body": "hi", "createdAt": "2026-04-20T00:00:00Z",
             "user": {"name": "Alice"}},
            {"id": "c2", "body": "reply", "createdAt": "2026-04-21T00:00:00Z",
             "user": None},
        ]}}}}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            comments = ts.fetch_comments("LIN-1")
        assert len(comments) == 2
        assert comments[0] == {"id": "c1", "author": "Alice", "body": "hi",
                               "created_at": "2026-04-20T00:00:00Z"}
        assert comments[1]["author"] == ""

    def test_non_200_returns_empty(self):
        ts = self._make_ts()
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            assert ts.fetch_comments("LIN-1") == []

    def test_missing_issue_returns_empty(self):
        ts = self._make_ts()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"issue": None}}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        with patch("features.ticket_systems.httpx.Client", return_value=mock_client):
            assert ts.fetch_comments("LIN-1") == []
