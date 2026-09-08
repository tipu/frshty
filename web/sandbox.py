"""Isolation for browser contexts the board does not control.

Artifacts and ticket documents are files an agent wrote, and the board serves
them from its own origin. A page among them needs its own scripts, dialogs,
downloads, and links that open a third-party site outside the sandbox. Every
other kind is data the browser renders, so it runs nothing.

A page policy carries no allow-same-origin, so the page cannot hold the board's
origin, and it pins three directives so the page cannot reach the board
sideways: connect-src stops its own fetch and WebSocket while still allowing
the blob and data URLs it made itself, frame-src stops it embedding a document
that would carry no policy of its own, and object-src stops the same trick
through object and embed. A board artifact is a self-contained page; it has
nothing on the network to fetch.

A sandboxed page sends `Origin: null`, which is why web.origin refuses that
value on every guarded route and on both WebSocket handshakes.
"""

DATA = "sandbox"
PAGE = ("sandbox allow-scripts allow-modals allow-popups "
        "allow-popups-to-escape-sandbox allow-downloads; "
        "connect-src blob: data:; frame-src 'none'; object-src 'none'")

_PAGE_SUFFIXES = (".html", ".htm")


def policy_for(path: str) -> str:
    return PAGE if path.lower().endswith(_PAGE_SUFFIXES) else DATA
