"""Isolation for browser contexts the board does not control.

Artifacts and ticket documents are files an agent wrote, and the board serves
them from its own origin. A page among them needs its own scripts, dialogs,
downloads, and links that open a third-party site outside the sandbox. Every
other kind is data the browser renders, so it runs nothing.

Audio and video are the exception to the sandbox. A browser opens a media file
as a media document that loads the file again from its own URL, and a sandboxed
media document never loads it: the player stays at 0:00 with no length. A media
policy lets the document load media from the board's origin and nothing else.

A page policy carries no allow-same-origin, so the page cannot hold the board's
origin, and it pins three directives so the page cannot reach the board
sideways: connect-src stops its own fetch and WebSocket while still allowing
the blob and data URLs it made itself, frame-src stops it embedding a document
that would carry no policy of its own, and object-src stops the same trick
through object and embed. A board artifact is a self-contained page; it has
nothing on the network to fetch.

`origin_is_opaque` guards the WebSocket routes, which CORS does not cover: a
handshake carries an Origin header the server must check itself. A sandboxed
document has no origin of its own and sends `null`, which the board's own pages
never do. It is not a general cross-origin check. Three hostnames reach this
board and Caddy rewrites Host to the canonical one for two of them, so the
request's Host header does not identify the browser's origin; naming the
board's browser-facing hostnames is operator configuration this module does not
have.
"""

DATA = "sandbox"
PAGE = ("sandbox allow-scripts allow-modals allow-popups "
        "allow-popups-to-escape-sandbox allow-downloads; "
        "connect-src blob: data:; frame-src 'none'; object-src 'none'")
MEDIA = "default-src 'none'; media-src 'self'"

_PAGE_SUFFIXES = (".html", ".htm")
_MEDIA_SUFFIXES = (".mp4", ".m4v", ".webm", ".mov", ".mkv", ".ogv", ".ogg",
                   ".mp3", ".m4a", ".aac", ".wav", ".oga", ".opus", ".flac")


def policy_for(path: str) -> str:
    lowered = path.lower()
    if lowered.endswith(_PAGE_SUFFIXES):
        return PAGE
    if lowered.endswith(_MEDIA_SUFFIXES):
        return MEDIA
    return DATA


def origin_is_opaque(origin: str) -> bool:
    return origin.strip().lower() == "null"
