import httpx

import core.log as log
from core import external_log
from core.config import resolve_env


def make_ticket_system(config: dict):
    system = config["job"].get("ticket_system", "")
    if system == "jira":
        return JiraTicketSystem(config)
    if system == "linear":
        return LinearTicketSystem(config)
    if system == "prd":
        return PRDTicketSystem(config)
    return None


class PRDTicketSystem:
    def __init__(self, config: dict):
        self.config = config

    def fetch_tickets(self) -> list[dict]:
        import core.state as state
        all_tickets = state.list_tickets()
        results = []
        for key, ts in all_tickets.items():
            if ts.get("source") != "prd":
                continue
            if ts.get("status") in ("done", "obsolete"):
                continue
            results.append({
                "key": key,
                "summary": ts.get("summary", ""),
                "description": ts.get("description", ""),
                "status": "",
                "url": ts.get("url", ""),
                "attachments": [],
                "related": [],
                "subtasks": [],
                "estimate_seconds": 0,
            })
        return results

    def fetch_comments(self, ticket_key: str) -> list[dict]:
        return []

    def get_comments(self, ticket_key: str) -> list[dict]:
        return []

    def ticket_url(self, key: str) -> str:
        raise NotImplementedError("PRD tickets have no external ticket URL; implement ticket_url for PRDTicketSystem")


class JiraTicketSystem:
    def __init__(self, config: dict):
        jira = config.get("jira", {})
        self.base_url = jira.get("base_url", "")
        self.user = resolve_env(config, "jira", "user_env")
        self.token = resolve_env(config, "jira", "token_env")
        self.board_id = jira.get("board_id")
        self.account_id = jira.get("user_account_id", "")
        self.jql = jira.get("jql", "")

    def ticket_url(self, key: str) -> str:
        if not self.base_url or not key:
            return ""
        return f"{self.base_url.split('/rest')[0]}/browse/{key}"

    def fetch_tickets(self) -> list[dict]:
        if not self.base_url or not self.user or not self.token:
            return []
        if not self.board_id and not self.jql:
            return []
        with external_log.client("jira", auth=(self.user, self.token), timeout=30) as client:
            if self.board_id:
                url = f"{self.base_url}/rest/agile/1.0/board/{self.board_id}/issue?maxResults=100"
                resp = client.get(url)
                if resp.status_code != 200:
                    return []
                issues = resp.json().get("issues", [])
                if self.account_id:
                    issues = [i for i in issues if (i.get("fields", {}).get("assignee") or {}).get("accountId") == self.account_id]
            else:
                url = f"{self.base_url}/rest/api/3/search/jql?jql={self.jql}&maxResults=20&fields=key,summary,status,description,attachment,issuelinks,parent,subtasks,timeoriginalestimate,updated,issuetype"
                resp = client.get(url)
                if resp.status_code != 200:
                    return []
                issues = resp.json().get("issues", [])
            results = []
            for i in issues:
                fields = i.get("fields", {})
                if not fields:
                    continue
                status = fields.get("status", {})

                attachments = []
                for a in fields.get("attachment", []):
                    attachments.append({"filename": a.get("filename", ""), "url": a.get("content", ""), "mime": a.get("mimeType", "")})

                related = []
                for link in fields.get("issuelinks", []):
                    rel_type = link.get("type", {}).get("outward", "relates to")
                    if link.get("outwardIssue"):
                        ri = link["outwardIssue"]
                        related.append({"key": ri.get("key", ""), "summary": ri.get("fields", {}).get("summary", ""), "relation": rel_type})
                    elif link.get("inwardIssue"):
                        ri = link["inwardIssue"]
                        rel_type = link.get("type", {}).get("inward", "relates to")
                        related.append({"key": ri.get("key", ""), "summary": ri.get("fields", {}).get("summary", ""), "relation": rel_type})

                parent = fields.get("parent")
                parent_info = None
                if parent:
                    parent_info = {"key": parent.get("key", ""), "summary": parent.get("fields", {}).get("summary", "")}

                subtasks = []
                for st_item in fields.get("subtasks", []):
                    subtasks.append({"key": st_item.get("key", ""), "summary": st_item.get("fields", {}).get("summary", "")})

                issuetype = fields.get("issuetype") or {}
                issue_type_name = issuetype.get("name", "") if isinstance(issuetype, dict) else str(issuetype)

                results.append({
                    "key": i.get("key", ""),
                    "summary": fields.get("summary", ""),
                    "status": status.get("name", "") if isinstance(status, dict) else str(status),
                    "description": _adf_to_text(fields.get("description")),
                    "url": f"{self.base_url.split('/rest')[0]}/browse/{i.get('key', '')}",
                    "updated_at": fields.get("updated", ""),
                    "attachments": attachments,
                    "related": related,
                    "parent": parent_info,
                    "subtasks": subtasks,
                    "estimate_seconds": fields.get("timeoriginalestimate", 0) or 0,
                    "issue_type": issue_type_name,
                })
            return results

    def fetch_comments(self, ticket_key: str) -> list[dict]:
        if not self.base_url or not self.user or not self.token or not ticket_key:
            return []
        url = f"{self.base_url}/rest/api/3/issue/{ticket_key}/comment?maxResults=100&orderBy=created"
        try:
            with external_log.client("jira", auth=(self.user, self.token), timeout=30) as client:
                resp = client.get(url)
                if resp.status_code != 200:
                    return []
                results = []
                for c in resp.json().get("comments", []):
                    author_info = c.get("author") or {}
                    results.append({
                        "id": str(c.get("id", "")),
                        "author_id": author_info.get("accountId", ""),
                        "author_name": author_info.get("displayName", ""),
                        "body": _adf_to_text(c.get("body")),
                        "created_at": c.get("created"),
                        "updated_at": c.get("updated"),
                    })
                return results
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            log.emit("jira_fetch_comments_failed", f"Failed to fetch comments for {ticket_key}: {e}", meta={"ticket": ticket_key})
            return []

    def get_comments(self, ticket_key: str) -> list[dict]:
        return self.fetch_comments(ticket_key)


class LinearTicketSystem:
    def __init__(self, config: dict):
        linear = config.get("linear", {})
        self.token = resolve_env(config, "linear", "token_env")
        self.email = linear.get("assignee_email", "")
        self.states = linear.get("states", ["In Progress", "Prioritized"])

    def ticket_url(self, key: str) -> str:
        if not self.token or not key:
            return ""
        query = 'query { issue(id: "%s") { url } }' % key
        with external_log.client("linear", timeout=30, transport=httpx.HTTPTransport(retries=2)) as client:
            resp = client.post("https://api.linear.app/graphql",
                json={"query": query},
                headers={"Authorization": self.token, "Content-Type": "application/json"})
            if resp.status_code != 200:
                return ""
            issue = (resp.json().get("data") or {}).get("issue") or {}
            return issue.get("url", "")

    def fetch_tickets(self) -> list[dict]:
        if not self.token or not self.email:
            return []
        states_str = "[" + ", ".join(f'"{s}"' for s in self.states) + "]"
        query = '''
        query {
          issues(
            filter: { assignee: { email: { eq: "%s" } } state: { name: { in: %s } } }
            first: 20 orderBy: updatedAt
          ) { nodes { identifier title state { name } description url updatedAt
              project { name description }
              parent { identifier title description }
              attachments { nodes { title url } }
              relations { nodes { type relatedIssue { identifier title description url } } }
              children { nodes { identifier title state { name } } }
          } }
        }
        ''' % (self.email, states_str)
        with external_log.client("linear", timeout=30, transport=httpx.HTTPTransport(retries=2)) as client:
            resp = client.post("https://api.linear.app/graphql",
                json={"query": query},
                headers={"Authorization": self.token, "Content-Type": "application/json"})
            if resp.status_code != 200:
                return []
            nodes = resp.json().get("data", {}).get("issues", {}).get("nodes", [])
            results = []
            for n in nodes:
                attachments = [{"filename": a.get("title", ""), "url": a.get("url", "")} for a in n.get("attachments", {}).get("nodes", [])]
                related = [{"key": r["relatedIssue"]["identifier"], "summary": r["relatedIssue"]["title"], "relation": r.get("type", "related")} for r in n.get("relations", {}).get("nodes", []) if r.get("relatedIssue")]
                subtasks = [{"key": c["identifier"], "summary": c["title"]} for c in n.get("children", {}).get("nodes", [])]
                results.append({
                    "key": n["identifier"],
                    "summary": n["title"],
                    "status": n["state"]["name"],
                    "description": n.get("description", ""),
                    "url": n.get("url", ""),
                    "updated_at": n.get("updatedAt", ""),
                    "project": n.get("project"),
                    "parent": n.get("parent"),
                    "attachments": attachments,
                    "related": related,
                    "subtasks": subtasks,
                })
            return results

    def fetch_state_history(self, ticket_key: str) -> list[dict]:
        if not self.token or not ticket_key:
            return []
        query = '''
        query {
          issue(id: "%s") {
            history(first: 50) {
              nodes { createdAt fromState { name } toState { name } actor { id name email } }
            }
          }
        }
        ''' % ticket_key
        try:
            with external_log.client("linear", timeout=30, transport=httpx.HTTPTransport(retries=2)) as client:
                resp = client.post("https://api.linear.app/graphql",
                    json={"query": query},
                    headers={"Authorization": self.token, "Content-Type": "application/json"})
                if resp.status_code != 200:
                    return []
                issue = (resp.json().get("data") or {}).get("issue") or {}
                nodes = (issue.get("history") or {}).get("nodes", [])
                results = []
                for n in nodes:
                    to_state = n.get("toState")
                    if not to_state:
                        continue
                    from_state = n.get("fromState") or {}
                    actor = n.get("actor") or {}
                    results.append({
                        "created_at": n.get("createdAt", ""),
                        "from_state": from_state.get("name", ""),
                        "to_state": to_state.get("name", ""),
                        "actor_email": actor.get("email", "") if actor else "",
                        "actor_name": actor.get("name", "") if actor else "",
                    })
                return results
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            log.emit("linear_fetch_history_failed", f"Failed to fetch history for {ticket_key}: {e}", meta={"ticket": ticket_key})
            return []

    def fetch_comments(self, ticket_key: str) -> list[dict]:
        if not self.token or not ticket_key:
            return []
        query = '''
        query {
          issue(id: "%s") {
            comments(first: 100) {
              nodes { id body createdAt updatedAt user { id name } }
            }
          }
        }
        ''' % ticket_key
        try:
            with external_log.client("linear", timeout=30, transport=httpx.HTTPTransport(retries=2)) as client:
                resp = client.post("https://api.linear.app/graphql",
                    json={"query": query},
                    headers={"Authorization": self.token, "Content-Type": "application/json"})
                if resp.status_code != 200:
                    return []
                issue = (resp.json().get("data") or {}).get("issue") or {}
                nodes = (issue.get("comments") or {}).get("nodes", [])
                results = []
                for n in nodes:
                    user_info = n.get("user") or {}
                    results.append({
                        "id": str(n.get("id", "")),
                        "author_id": user_info.get("id", ""),
                        "author_name": user_info.get("name", ""),
                        "body": n.get("body", ""),
                        "created_at": n.get("createdAt"),
                        "updated_at": n.get("updatedAt"),
                    })
                return results
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            log.emit("linear_fetch_comments_failed", f"Failed to fetch comments for {ticket_key}: {e}", meta={"ticket": ticket_key})
            return []

    def get_comments(self, ticket_key: str) -> list[dict]:
        return self.fetch_comments(ticket_key)


def _adf_to_text(adf) -> str:
    if not adf or not isinstance(adf, dict):
        return ""
    return _adf_node_text(adf)


def _adf_node_text(node: dict) -> str:
    ntype = node.get("type", "")
    children = node.get("content", [])

    if ntype == "text":
        return node.get("text", "")
    if ntype in ("paragraph", "heading"):
        return "".join(_adf_node_text(c) for c in children) + "\n\n"
    if ntype == "codeBlock":
        code = "".join(_adf_node_text(c) for c in children)
        return f"\n```\n{code}\n```\n\n"
    if ntype in ("orderedList", "bulletList"):
        items = []
        for i, c in enumerate(children, 1):
            prefix = f"{i}. " if ntype == "orderedList" else "- "
            items.append(prefix + _adf_node_text(c).strip())
        return "\n".join(items) + "\n\n"
    if ntype == "listItem":
        return "".join(_adf_node_text(c) for c in children)
    if ntype in ("doc", "tableCell", "tableRow", "tableHeader"):
        return "".join(_adf_node_text(c) for c in children)
    if ntype == "table":
        return "".join(_adf_node_text(c) for c in children) + "\n"
    if children:
        return "".join(_adf_node_text(c) for c in children)
    return ""
