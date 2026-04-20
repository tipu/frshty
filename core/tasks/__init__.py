"""Importing this package registers all task definitions."""
from core.tasks import registry  # noqa: F401
from core.tasks import preconditions  # noqa: F401
from core.tasks import tickets  # noqa: F401
from core.tasks import billing  # noqa: F401
from core.tasks import polls  # noqa: F401
from core.tasks import slack  # noqa: F401
from core.tasks import routes  # noqa: F401
