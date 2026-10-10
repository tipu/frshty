"""Print generated names for code, tests, docs and examples.

frshty is public and runs for several organizations. A client, company or
person name never goes into this repository. Run this script every time you
need a name, and use what it prints.

Usage:
    python3 scripts/fake_name.py              # one of each kind
    python3 scripts/fake_name.py company 3    # three company names
    kinds: company, person, key
"""
import random
import sys

ONSETS = ("b", "br", "c", "d", "dr", "f", "g", "gl", "h", "k", "l", "m", "n", "p", "pr", "r", "s", "st", "t", "tr", "v", "z")
VOWELS = ("a", "e", "i", "o", "u", "ai", "ea", "io", "ou")
CODAS = ("", "", "l", "n", "r", "s", "x", "nd", "rk", "st")
SUFFIXES = ("co", "labs", "works", "corp", "ware", "ly")


def _word(syllables: int) -> str:
    return "".join(random.choice(ONSETS) + random.choice(VOWELS) for _ in range(syllables)) + random.choice(CODAS)


def generate(kind: str) -> str:
    if kind == "company":
        return (_word(2) + random.choice(SUFFIXES)).capitalize()
    if kind == "person":
        return f"{_word(1).capitalize()} {_word(2).capitalize()}"
    if kind == "key":
        return _word(2) + random.choice(SUFFIXES)
    raise SystemExit(f"unknown kind {kind!r}; use company, person or key")


def main(args: list[str]) -> int:
    if not args:
        for kind in ("company", "person", "key"):
            print(f"{kind}: {generate(kind)}")
        return 0
    for _ in range(int(args[1]) if len(args) > 1 else 1):
        print(generate(args[0]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
