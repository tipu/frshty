"""In-memory Jira/Linear implementation for testing."""


class MockJiraTicketSystem:
    """In-memory implementation of Jira/Linear ticket system."""

    def __init__(self, config: dict):
        self.config = config
        self.tickets = {}  # ticket_key -> ticket object
        self.ticket_counter = 0
        self.comments = {}  # ticket_key -> [comment objects]

    def create_ticket(self, title: str, description: str) -> str:
        """Test helper: Create a ticket and return its key."""
        self.ticket_counter += 1
        ticket_key = f"TEST-{self.ticket_counter}"
        self.tickets[ticket_key] = {
            "key": ticket_key,
            "summary": title,
            "description": description,
            "status": "To Do",
            "url": f"https://jira.example.com/browse/{ticket_key}",
            "assignee": None,
            "attachments": [],
            "related": [],
            "subtasks": [],
            "parent": None,
        }
        self.comments[ticket_key] = []
        return ticket_key

    async def fetch_tickets(self) -> list[dict]:
        """Fetch all new tickets (status=To Do)."""
        return [
            ticket for ticket in self.tickets.values()
            if ticket["status"] == "To Do"
        ]

    async def get_ticket(self, key: str) -> dict:
        """Get a specific ticket."""
        return self.tickets.get(key) or {}

    async def update_status(self, key: str, status: str) -> None:
        """Update ticket status."""
        if key in self.tickets:
            self.tickets[key]["status"] = status

    async def add_comment(self, key: str, comment: str) -> None:
        """Add comment to ticket."""
        if key not in self.comments:
            self.comments[key] = []
        self.comments[key].append({
            "body": comment,
            "author": "frshty",
        })

    async def get_comments(self, key: str) -> list[dict]:
        """Get ticket comments."""
        return self.comments.get(key, [])

    async def fetch_comments(self, key: str) -> list[dict]:
        """Legacy method: fetch comments."""
        return await self.get_comments(key)

    def update_assignee(self, key: str, assignee: str) -> None:
        """Test helper: update assignee."""
        if key in self.tickets:
            self.tickets[key]["assignee"] = assignee
