import httpx

from core.config import resolve_env


def make_ticket_system(config: dict):
    system = config["job"].get("ticket_system", "")
    if system == "jira":
        return JiraTicketSystem(config)
    if system == "linear":
        return LinearTicketSystem(config)
    return None


class JiraTicketSystem:
    def __init__(self, config: dict):
        jira = config.get("jira", {})
        self.base_url = jira.get("base_url", "")
        self.user = resolve_env(config, "jira", "user_env")
        self.token = resolve_env(config, "jira", "token_env")
        self.board_id = jira.get("board_id")
        self.account_id = jira.get("user_account_id", "")
        self.jql = jira.get("jql", "")

    def fetch_tickets(self) -> list[dict]:
        if not self.base_url or not self.user or not self.token:
            return []
        if not self.board_id and not self.jql:
            return []
        with httpx.Client(auth=(self.user, self.token), timeout=30) as client:
            if self.board_id:
                url = f"{self.base_url}/rest/agile/1.0/board/{self.board_id}/issue?maxResults=100"
                resp = client.get(url)
                if resp.status_code != 200:
                    return []
                issues = resp.json().get("issues", [])
                if self.account_id:
                    issues = [i for i in issues if (i.get("fields", {}).get("assignee") or {}).get("accountId") == self.account_id]
            else:
                url = f"{self.base_url}/rest/api/3/search/jql?jql={self.jql}&maxResults=20&fields=key,summary,status,description,attachment,issuelinks,parent,subtasks,timeoriginalestimate"
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

                results.append({
                    "key": i.get("key", ""),
                    "summary": fields.get("summary", ""),
                    "status": status.get("name", "") if isinstance(status, dict) else str(status),
                    "description": _adf_to_text(fields.get("description")),
                    "url": f"{self.base_url.split('/rest')[0]}/browse/{i.get('key', '')}",
                    "attachments": attachments,
                    "related": related,
                    "parent": parent_info,
                    "subtasks": subtasks,
                    "estimate_seconds": fields.get("timeoriginalestimate", 0) or 0,
                })
            return results


class LinearTicketSystem:
    def __init__(self, config: dict):
        linear = config.get("linear", {})
        self.token = resolve_env(config, "linear", "token_env")
        self.email = linear.get("assignee_email", "")

    def fetch_tickets(self) -> list[dict]:
        if not self.token or not self.email:
            return []
        query = '''
        query {
          issues(
            filter: { assignee: { email: { eq: "%s" } } state: { name: { in: ["In Progress", "Prioritized"] } } }
            first: 20 orderBy: updatedAt
          ) { nodes { identifier title state { name } description url
              project { name description }
              parent { identifier title description }
              attachments { nodes { title url } }
              relations { nodes { type relatedIssue { identifier title description url } } }
              children { nodes { identifier title state { name } } }
          } }
        }
        ''' % self.email
        with httpx.Client(timeout=30) as client:
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
                    "project": n.get("project"),
                    "parent": n.get("parent"),
                    "attachments": attachments,
                    "related": related,
                    "subtasks": subtasks,
                })
            return results


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
