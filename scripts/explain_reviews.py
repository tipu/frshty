"""Add the Claude and Codex "is this a valid concern" explainers, and the summary that
merges them, to every stored review comment.

New reviews get the explainer when the pipeline writes them. This script
fills it into the reviews stored before that. It asks each model once for
each distinct comment that has no explainer from it yet, so a second run only fills what the
first one missed.

Run a dry run first. It prints how many comments each review would send.

    python3 scripts/explain_reviews.py --config config/myproject.toml
    python3 scripts/explain_reviews.py --config config/myproject.toml --apply

Narrow the run to one review with --only <repo>/<branch dir>. Add --redo to
drop the explainers a review already has and ask again, for example after the
prompt changed.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import core.llm as llm
import core.log as log
import core.state as state
from core.config import load_config
from features.reviewer import EXPLAINERS, SUMMARY_FIELD, explain_key, explain_review
from services import review_store


def _flag(name: str) -> str:
    if name not in sys.argv:
        return ""
    index = sys.argv.index(name)
    return sys.argv[index + 1] if index + 1 < len(sys.argv) else ""


def _missing(branch_dir: Path, redo: bool = False) -> int:
    keys = set()
    done = set()
    for f in branch_dir.glob("queued_comments*.json"):
        try:
            comments = json.loads(f.read_text())
        except (OSError, ValueError):
            continue
        for c in comments if isinstance(comments, list) else []:
            if isinstance(c, dict) and c.get("body"):
                for field in (*EXPLAINERS, SUMMARY_FIELD):
                    (done if c.get(field) else keys).add((field, *explain_key(c)))
    return len(keys | done) if redo else len(keys - done)


def _clear(branch_dir: Path) -> None:
    with review_store.queue_lock(branch_dir):
        for f in branch_dir.glob("queued_comments*.json"):
            try:
                comments = json.loads(f.read_text())
            except (OSError, ValueError):
                continue
            if not isinstance(comments, list):
                continue
            for c in comments:
                if isinstance(c, dict):
                    for field in (*EXPLAINERS, SUMMARY_FIELD):
                        c.pop(field, None)
            f.write_text(json.dumps(comments, indent=2))


def main() -> int:
    config_path = _flag("--config")
    if not config_path:
        print("usage: explain_reviews.py --config <instance.toml> [--only <repo>/<branch>] "
              "[--redo] [--apply]", file=sys.stderr)
        return 2
    apply = "--apply" in sys.argv
    redo = "--redo" in sys.argv
    only = _flag("--only")
    config = load_config(config_path)
    state.init(config["_state_dir"])
    log.init(config["_state_dir"], config["job"]["key"])
    llm.configure(config)
    reviews_dir = Path(config["_state_dir"]) / "reviews"
    branch_dirs = sorted(d for d in reviews_dir.glob("*/*")
                         if d.is_dir() and any(d.glob("queued_comments*.json")))
    total = 0
    failed = 0
    for branch_dir in branch_dirs:
        if only and str(branch_dir.relative_to(reviews_dir)) != only:
            continue
        if redo and apply:
            _clear(branch_dir)
        missing = _missing(branch_dir, redo and not apply)
        if not missing:
            continue
        wt = branch_dir / "worktree"
        worktree = wt if (wt / ".git").exists() else None
        if not apply:
            print(f"{branch_dir.relative_to(reviews_dir)}: {missing} explainer(s) and summaries to write")
            total += missing
            continue
        filled = explain_review(branch_dir, worktree)
        left = _missing(branch_dir)
        failed += left
        total += filled
        print(f"{branch_dir.relative_to(reviews_dir)}: explained {filled}, still missing {left}",
              flush=True)
    print(f"{'wrote' if apply else 'would write'} {total} explainer(s), {failed} still missing")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
