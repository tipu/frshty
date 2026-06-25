import json
import multiprocessing
import re
import shlex
import subprocess
import threading

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocket

import core.config as cfg
import core.log as log
import core.state as state
import core.terminal as terminal
import features.reviewer as reviewer
from core.claude_runner import run_haiku, extract_json
from core.config import get_repos
from features.platforms import make_platform
from features.ticket_systems import make_ticket_system
from services import review_store
from web.state import _config


router = APIRouter()


def _run_review(config_path: str, full_repo: str, pr_id: int, url: str):
    from web.state import ensure_path
    ensure_path()
    review_config = cfg.load_config(config_path)
    state.init(review_config["_state_dir"])
    log.init(review_config["_state_dir"], review_config["job"]["key"])
    platform = make_platform(review_config)
    repo_short = full_repo.split("/")[-1]
    platform._repo_cache[repo_short] = full_repo
    branch = platform.get_pr_branch(repo_short, pr_id) or f"pr-{pr_id}"
    pr = {
        "id": pr_id,
        "repo": repo_short,
        "full_repo": full_repo,
        "url": url,
        "branch": branch,
        "base": "",
        "title": "",
        "author": "",
        "created_on": "",
        "updated_on": "",
    }
    base_url = review_config["_base_url"]
    log.emit("review_started", f"Manual review: {full_repo} PR #{pr_id}",
        links={"pr": url, "detail": f"{base_url}/reviews/{pr['repo']}/{pr_id}"},
        meta={"repo": pr["repo"], "pr_id": pr_id})
    pending_dir = review_config["_state_dir"] / "reviews" / pr["repo"] / f"pending-{pr_id}"
    result = reviewer.review_pr(review_config, platform, pr)
    import shutil
    shutil.rmtree(pending_dir, ignore_errors=True)
    if result:
        review_state = state.load("reviews")
        review_state[f"{pr['repo']}/{pr_id}"] = {"reviewed": True, "branch": pr["branch"], "last_updated": pr.get("updated_on")}
        state.save("reviews", review_state)
        issues = result.get("issues", [])
        log.emit("review_complete", f"Review done: {result.get('verdict', 'unknown')}, {len(issues)} issues",
            links={"pr": url, "detail": f"{base_url}/reviews/{pr['repo']}/{pr_id}"},
            meta={"repo": pr["repo"], "pr_id": pr_id, "verdict": result.get("verdict"), "issue_count": len(issues)})
    else:
        log.emit("cycle_error", f"Manual review failed for {full_repo} PR #{pr_id}",
            meta={"repo": pr["repo"], "pr_id": pr_id})


@router.post("/api/reviews/submit")
def api_submit_review(body: dict):
    url = body.get("url", "").strip()
    if not url:
        return JSONResponse({"error": "url required"}, status_code=400)
    m = re.match(r"https?://github\.com/([^/]+/[^/]+)/pull/(\d+)", url)
    if not m:
        m = re.match(r"https?://bitbucket\.org/([^/]+/[^/]+)/pull-requests/(\d+)", url)
    if not m:
        return JSONResponse({"error": "invalid PR url (expected github.com/.../pull/N or bitbucket.org/.../pull-requests/N)"}, status_code=400)
    full_repo = m.group(1)
    repo_short = full_repo.split("/")[-1]
    pr_id = int(m.group(2))
    pending_dir = _config["_state_dir"] / "reviews" / repo_short / f"pending-{pr_id}"
    pending_dir.mkdir(parents=True, exist_ok=True)
    (pending_dir / "queued_comments.json").write_text(json.dumps([{"pr_id": pr_id, "repo": repo_short, "pr_url": url, "status": "pending"}]))
    (pending_dir / "review.json").write_text(json.dumps({"verdict": "", "status": "reviewing"}))
    multiprocessing.Process(
        target=_run_review, args=(str(_config["_config_path"]), full_repo, pr_id, url), daemon=True
    ).start()
    return {"status": "started", "repo": full_repo, "pr_id": pr_id}


@router.post("/api/reviews/rerun-ticket/{ticket_key}")
def api_rerun_ticket_review(ticket_key: str):
    reviews_dir = _config["_state_dir"] / "reviews"
    if not reviews_dir.exists():
        return JSONResponse({"error": "No reviews found"}, status_code=404)

    prs = []
    for repo_dir in reviews_dir.iterdir():
        if not repo_dir.is_dir():
            continue
        repo = repo_dir.name
        for branch_dir in repo_dir.iterdir():
            if not branch_dir.is_dir():
                continue
            branch = branch_dir.name
            if ticket_key.lower() not in branch.lower():
                continue
            queued_file = branch_dir / "queued_comments.json"
            if queued_file.exists():
                try:
                    queued = json.loads(queued_file.read_text())
                    if queued:
                        first = queued[0]
                        pr_id = first.get("pr_id")
                        pr_url = first.get("pr_url", "")
                        if pr_id:
                            prs.append({
                                "repo": repo,
                                "id": pr_id,
                                "branch": branch,
                                "url": pr_url,
                                "head_sha": ""
                            })
                except (json.JSONDecodeError, IndexError, KeyError, TypeError):
                    pass

    if not prs:
        return JSONResponse({"error": f"No pending reviews found for {ticket_key}"}, status_code=404)

    reviewer.review_ticket_prs(_config, ticket_key, prs)
    return {"status": "review_started", "ticket": ticket_key, "pr_count": len(prs)}


@router.put("/api/settings")
def api_settings(body: dict):
    for feature, enabled in body.get("features", {}).items():
        cfg.save_feature_toggle(_config, feature, enabled)
    return {"status": "ok", "features": _config.get("features", {})}


@router.get("/api/reviews")
def api_reviews_list():
    reviews_dir = _config["_state_dir"] / "reviews"
    if not reviews_dir.exists():
        return []
    candidates = []
    for repo_dir in sorted(reviews_dir.iterdir()):
        if not repo_dir.is_dir():
            continue
        for branch_dir in repo_dir.iterdir():
            queued = branch_dir / "queued_comments.json"
            review = branch_dir / "review.json"
            if not queued.exists():
                continue
            comments = json.loads(queued.read_text())
            review_data = json.loads(review.read_text()) if review.exists() else {}
            first_comment = comments[0] if comments else {}
            candidates.append({
                "repo": repo_dir.name,
                "branch": branch_dir.name,
                "verdict": review_data.get("verdict", ""),
                "reviewed_at": review_data.get("date", ""),
                "total_comments": len(comments),
                "pending": sum(1 for c in comments if c.get("status") == "pending"),
                "pr_url": first_comment.get("pr_url") or review_data.get("pr_url", ""),
                "pr_id": first_comment.get("pr_id") or review_data.get("pr_id", 0),
                "created_on": first_comment.get("created_at", ""),
            })
    platform = make_platform(_config)
    job_platform = _config.get("job", {}).get("platform", "")
    if job_platform == "bitbucket":
        my_id = _config.get("bitbucket", {}).get("user_account_id", "")
    else:
        my_id = _config.get("github", {}).get("user", "") or "@me"
    from concurrent.futures import ThreadPoolExecutor
    import shutil

    def check_open(r):
        if not r["pr_id"]:
            return None
        info = platform.get_pr_info(r["repo"], r["pr_id"])
        state_val = info.get("state", "UNKNOWN")
        if state_val in ("MERGED", "DECLINED", "CLOSED", "SUPERSEDED", "DELETED"):
            branch_dir = reviews_dir / r["repo"] / r["branch"]
            if branch_dir.is_dir():
                shutil.rmtree(branch_dir, ignore_errors=True)
                log.emit("review_removed", f"Removed review for {r['repo']}#{r['pr_id']} ({state_val.lower()})",
                    meta={"repo": r["repo"], "pr_id": r["pr_id"], "state": state_val})
            return None
        if state_val != "OPEN":
            return None
        r["updated_on"] = info["updated_on"]
        r["author"] = info.get("author", "")
        r["is_mine"] = bool(my_id) and info.get("author_id", "") == my_id
        approvers = info.get("approvers") or []
        r["approved_by_me"] = bool(my_id) and my_id in approvers
        r["title"] = info.get("title", "")
        return r
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = [r for r in pool.map(check_open, candidates) if r]
    seen = {}
    for r in results:
        key = (r["repo"], r["pr_id"])
        if key not in seen or r.get("reviewed_at", "") > seen[key].get("reviewed_at", ""):
            seen[key] = r
    return list(seen.values())


@router.get("/api/reviews/{repo}/{pr_id}/comments")
def api_review_comments(repo: str, pr_id: int):
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    return found[1] if found else []


@router.get("/api/reviews/{repo}/{pr_id}/info")
def api_review_info(repo: str, pr_id: int):
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return {}
    branch_dir, comments, _ = found
    review_json = branch_dir / "review.json"
    review_data = json.loads(review_json.read_text()) if review_json.exists() else {}
    branch = review_data.get("source_branch", "")
    result = {
        "summary": review_data.get("summary", ""),
        "verdict": review_data.get("verdict", ""),
        "branch": branch,
        "author": review_data.get("author", ""),
        "date": review_data.get("date", ""),
        "pr_url": (comments[0].get("pr_url", "") if comments else review_data.get("pr_url", "")),
        "pr_title": comments[0].get("pr_title", "") if comments else "",
    }
    m = re.search(r"[A-Za-z]+-\d+", branch)
    ticket_key = m.group().upper() if m else ""
    ticket_url = ""
    if ticket_key:
        ticket = state.load("tickets").get(ticket_key) or {}
        ticket_url = ticket.get("url", "")
        if not ticket_url:
            ts = make_ticket_system(_config)
            if ts:
                ticket_url = ts.ticket_url(ticket_key)
    result["ticket_key"] = ticket_key
    result["ticket_url"] = ticket_url
    platform = make_platform(_config)
    try:
        pr_info = platform.get_pr_info(repo, pr_id)
        result["pr_description"] = pr_info.get("description", "")
        result["pr_title"] = pr_info.get("title", result["pr_title"])
    except Exception as e:
        log.emit("get_pr_info_failed", f"Failed to get PR info: {e}", meta={"repo": repo, "pr_id": pr_id})
    return result


def _resolve_ticket_goal(branch: str, repo: str, pr_id: int) -> str:
    m = re.search(r"[A-Za-z]+-\d+", branch or "")
    key = m.group().upper() if m else ""
    if key:
        t = state.load("tickets").get(key) or {}
        parts = [t.get("summary", ""), t.get("description", "")]
        ac = (t.get("acceptance_criteria_json") or {}).get("source_text", "")
        if ac:
            parts.append(ac)
        goal = "\n".join(p for p in parts if p).strip()
        if goal:
            return goal[:4000]
    try:
        info = make_platform(_config).get_pr_info(repo, pr_id) or {}
        return f"{info.get('title', '')}\n{info.get('description', '')}".strip()[:4000]
    except Exception:
        return ""


def _split_diff_by_file(diff_text: str, max_files: int = 40, per_file_chars: int = 2500) -> dict:
    files: dict[str, list[str]] = {}
    current = None
    for line in (diff_text or "").split("\n"):
        if line.startswith("+++ b/"):
            current = line[6:]
            files[current] = []
        elif current is not None:
            files[current].append(line)
    out = {}
    for p, ls in files.items():
        if not p or p == "/dev/null":
            continue
        out[p] = "\n".join(ls)[:per_file_chars]
        if len(out) >= max_files:
            break
    return out


def _generate_file_summaries(goal: str, per_file: dict) -> dict:
    listing = "\n\n".join(f"### {p}\n{d}" for p, d in per_file.items())[:25000]
    prompt = (
        "You are reviewing a pull request. The goal of the work:\n"
        f"{goal or '(no ticket goal available; infer it from the changes)'}\n\n"
        "For each changed file below, write ONE concise, specific, technical sentence on how "
        "that file contributes to the goal. No fluff, no restating the filename.\n\n"
        f"{listing}\n\n"
        'Reply with JSON only, mapping each exact file path to its sentence: '
        '{"path/to/file": "one sentence", ...}'
    )
    raw = run_haiku(prompt)
    parsed = extract_json(raw) if raw else None
    if isinstance(parsed, dict):
        return {k: str(v) for k, v in parsed.items() if k in per_file and v}
    return {}


@router.get("/api/reviews/{repo}/{pr_id}/file-summaries")
def api_review_file_summaries(repo: str, pr_id: int):
    """Per-file 'how it contributes to the ticket goal' blurbs, generated once
    from the ticket goal + the diff and cached next to the review."""
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return {}
    branch_dir, _comments, _ = found
    cache = branch_dir / "file_summaries.json"
    if cache.exists():
        try:
            return json.loads(cache.read_text())
        except (OSError, ValueError):
            pass

    review_json = branch_dir / "review.json"
    review_data = json.loads(review_json.read_text()) if review_json.exists() else {}
    branch = review_data.get("source_branch", "")

    diff_path = branch_dir / "diff.txt"
    diff_text = diff_path.read_text() if diff_path.exists() else ""
    if not diff_text:
        platform = make_platform(_config)
        review_store.populate_repo_cache(platform, _config["_state_dir"], repo)
        diff_text = platform.get_pr_diff(repo, pr_id) or ""
    per_file = _split_diff_by_file(diff_text)
    if not per_file:
        return {}

    goal = _resolve_ticket_goal(branch, repo, pr_id)
    summaries = _generate_file_summaries(goal, per_file)
    try:
        cache.write_text(json.dumps(summaries, indent=2))
    except OSError:
        pass
    return summaries


@router.post("/api/reviews/{repo}/{pr_id}/discuss")
def api_start_discuss(repo: str, pr_id: int, body: dict):
    idx = body.get("idx", "general")
    file_path = body.get("file", "")
    line = body.get("line", "")
    severity = body.get("severity", "")
    comment_body = body.get("body", "")

    session_id = f"discuss-{repo}-{pr_id}-{idx}"

    context = f"You are helping review a pull request in {repo} (PR #{pr_id}).\n\n"
    if idx != "general" and file_path:
        context += f"The reviewer flagged an issue:\nFile: {file_path}\n"
        if line:
            context += f"Line: {line}\n"
        if severity:
            context += f"Severity: {severity}\n"
        context += f"\nReview comment:\n{comment_body}\n\nRead the file and help discuss this issue."
    else:
        context += "Help the reviewer understand and discuss this PR."

    review_dir = _config["_state_dir"] / "reviews" / repo
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    matched_dir = found[0] if found else None
    cwd = str(found[2]) if found and found[2] else None
    if not cwd and matched_dir:
        review_json = matched_dir / "review.json"
        branch = None
        if review_json.exists():
            branch = json.loads(review_json.read_text()).get("source_branch")
        if branch:
            repos = get_repos(_config)
            matching = [r for r in repos if r["name"] == repo]
            if matching:
                repo_path = matching[0]["path"]
                wt = matched_dir / "worktree"
                wt.parent.mkdir(parents=True, exist_ok=True)
                subprocess.run(["git", "fetch", "origin", branch], cwd=str(repo_path), capture_output=True, timeout=60)
                subprocess.run(["git", "worktree", "prune"], cwd=str(repo_path), capture_output=True, timeout=60)
                subprocess.run(["git", "worktree", "add", str(wt), branch], cwd=str(repo_path), capture_output=True, timeout=60)
                if (wt / ".git").exists():
                    cwd = str(wt)
    if not cwd:
        platform = make_platform(_config)
        review_store.populate_repo_cache(platform, _config["_state_dir"], repo)
        worktree = review_dir / f"pr-{pr_id}" / "worktree"
        platform.ensure_pr_worktree(repo, pr_id, worktree)
        if (worktree / ".git").exists():
            cwd = str(worktree)
    if not cwd:
        cwd = str(review_dir)

    terminal.kill_terminal(session_id)
    terminal.ensure_session(session_id, cwd)
    terminal.send_keys(session_id, f"claude --dangerously-skip-permissions --append-system-prompt {shlex.quote(context)}")

    return {"session_id": session_id}


@router.websocket("/ws/discuss/{session_id}")
async def ws_discuss(websocket: WebSocket, session_id: str):
    from web.state import multi_apply_host, multi_reset
    tokens = multi_apply_host(websocket.headers.get("host"))
    try:
        await terminal.terminal_handler(websocket, session_id, _config)
    finally:
        multi_reset(tokens)


@router.get("/api/reviews/{repo}/{pr_id}/diff")
def api_review_diff(repo: str, pr_id: int):
    platform = make_platform(_config)
    review_store.populate_repo_cache(platform, _config["_state_dir"], repo)
    diff = platform.get_pr_diff(repo, pr_id)
    return {"diff": diff or ""}


@router.get("/api/reviews/{repo}/{pr_id}/walkthrough/{idx}")
def api_review_walkthrough(repo: str, pr_id: int, idx: int):
    from features.reviewer import build_walkthrough_context
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return JSONResponse({"error": "review not found"}, status_code=404)
    branch_dir, comments, worktree = found
    content_comments = [c for c in comments if c.get("body")]
    total = len(content_comments)
    if total == 0:
        return JSONResponse({"error": "no comments"}, status_code=404)
    idx = max(0, min(idx, total - 1))
    comment = content_comments[idx]

    cache_file = branch_dir / "walkthrough_cache.json"
    if cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text())
            if idx < len(cache):
                cached = cache[idx]
                return {"comment": comment, "explanation": cached.get("explanation", ""),
                        "snippets": cached.get("snippets", []),
                        "file": comment.get("path", ""), "total": total, "idx": idx}
        except (json.JSONDecodeError, OSError):
            pass

    ctx = build_walkthrough_context(comment, worktree)
    return {"comment": comment, "explanation": ctx["explanation"], "snippets": ctx["snippets"],
            "file": comment.get("path", ""), "total": total, "idx": idx}


_walkthrough_inflight: set[tuple[str, int]] = set()
_walkthrough_inflight_lock = threading.Lock()


@router.post("/api/reviews/{repo}/{pr_id}/walkthrough/preprocess")
def api_walkthrough_preprocess(repo: str, pr_id: int):
    from concurrent.futures import ThreadPoolExecutor
    from features.reviewer import build_walkthrough_context

    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return JSONResponse({"error": "review not found"}, status_code=404)
    branch_dir, comments, worktree = found
    content_comments = [c for c in comments if c.get("body")]
    if not content_comments:
        return JSONResponse({"error": "no comments"}, status_code=404)

    cache_file = branch_dir / "walkthrough_cache.json"
    if cache_file.exists():
        return {"status": "ok", "count": len(content_comments), "cached": True}

    key = (repo, pr_id)
    with _walkthrough_inflight_lock:
        if key in _walkthrough_inflight:
            return {"status": "in_progress", "count": len(content_comments), "cached": False}
        _walkthrough_inflight.add(key)

    def preprocess_in_background():
        try:
            def compute_context(comment):
                return build_walkthrough_context(comment, worktree)

            with ThreadPoolExecutor(max_workers=5) as pool:
                contexts = list(pool.map(compute_context, content_comments))
            cache_file.write_text(json.dumps(contexts, indent=2))
        except Exception as e:
            log.emit("preprocess_comments_failed", f"Failed to preprocess comments: {e}")
        finally:
            with _walkthrough_inflight_lock:
                _walkthrough_inflight.discard(key)

    threading.Thread(target=preprocess_in_background, daemon=True).start()
    return {"status": "ok", "count": len(content_comments), "cached": False}


@router.get("/api/reviews/{repo}/{pr_id}/bb-comments")
def api_bb_comments(repo: str, pr_id: int):
    platform = make_platform(_config)
    comments = platform.get_pr_comments(repo, pr_id)
    names = list({c["author_name"] for c in comments if c.get("author_name")})
    initials = []
    for name in names:
        parts = name.split()
        initials.append("".join(p[0].upper() for p in parts if p))
    return {"count": len(comments), "initials": initials}


@router.post("/api/reviews/{repo}/{pr_id}/comments/{idx}/submit")
def api_submit_comment(repo: str, pr_id: int, idx: int):
    platform = make_platform(_config)
    review_store.populate_repo_cache(platform, _config["_state_dir"], repo)
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return JSONResponse({"error": "not found"}, status_code=404)
    branch_dir, comments, _ = found
    queued = branch_dir / "queued_comments.json"
    if idx >= len(comments):
        return JSONResponse({"error": "invalid index"}, status_code=400)
    comment = comments[idx]
    remote_id = comment.get("remote_id")
    if remote_id:
        result = platform.edit_pr_comment(repo, pr_id, remote_id, comment["body"])
        if result.get("status") == "updated":
            comments[idx]["status"] = "submitted"
            queued.write_text(json.dumps(comments, indent=2))
            log.emit("review_comment_edited", f"Edited comment on {repo} PR #{pr_id}",
                links={"pr": comment.get("pr_url", "")},
                meta={"repo": repo, "pr_id": pr_id})
    else:
        result = platform.post_pr_comment(repo, pr_id, comment["body"], comment.get("path"), comment.get("line"))
        if result.get("status") == "posted":
            comments[idx]["status"] = "submitted"
            comments[idx]["remote_id"] = result.get("id")
            queued.write_text(json.dumps(comments, indent=2))
            log.emit("review_comment_submitted", f"Submitted comment on {repo} PR #{pr_id}",
                links={"pr": comment.get("pr_url", "")},
                meta={"repo": repo, "pr_id": pr_id})
    return result


@router.post("/api/reviews/{repo}/{pr_id}/comments/new")
def api_new_comment(repo: str, pr_id: int, body: dict):
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return JSONResponse({"error": "not found"}, status_code=404)
    branch_dir, comments, _ = found
    pr_url = comments[0].get("pr_url", "") if comments else ""
    if not pr_url:
        review_json = branch_dir / "review.json"
        if review_json.exists():
            pr_url = json.loads(review_json.read_text()).get("pr_url", "")
    new_comment = {
        "pr_id": pr_id,
        "repo": repo,
        "pr_url": pr_url,
        "path": body.get("path"),
        "line": body.get("line"),
        "severity": body.get("severity", "suggestion"),
        "persona": "manual",
        "body": body.get("body", ""),
        "status": "draft",
    }
    comments.append(new_comment)
    (branch_dir / "queued_comments.json").write_text(json.dumps(comments, indent=2))
    return {"status": "ok", "idx": len(comments) - 1}


@router.delete("/api/reviews/{repo}/{pr_id}/comments/{idx}")
def api_delete_comment(repo: str, pr_id: int, idx: int):
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return JSONResponse({"error": "not found"}, status_code=404)
    branch_dir, comments, _ = found
    if idx >= len(comments):
        return JSONResponse({"error": "invalid index"}, status_code=400)
    comments.pop(idx)
    (branch_dir / "queued_comments.json").write_text(json.dumps(comments, indent=2))
    return {"status": "ok"}


@router.put("/api/reviews/{repo}/{pr_id}/comments/{idx}")
def api_update_comment(repo: str, pr_id: int, idx: int, body: dict):
    found = review_store.find_review(_config["_state_dir"], repo, pr_id)
    if not found:
        return JSONResponse({"error": "not found"}, status_code=404)
    branch_dir, comments, _ = found
    if idx >= len(comments):
        return JSONResponse({"error": "invalid index"}, status_code=400)
    if "body" in body:
        comments[idx]["body"] = body["body"]
    if "line" in body:
        comments[idx]["line"] = body["line"]
    (branch_dir / "queued_comments.json").write_text(json.dumps(comments, indent=2))
    return {"status": "ok"}


@router.post("/api/reviews/{repo}/{pr_id}/comments/{idx}/simplify")
def api_simplify_comment(repo: str, pr_id: int, idx: int, body: dict):
    from features.reviewer import _simplify_body_with_context
    if "body" not in body:
        return JSONResponse({"error": "body required"}, status_code=400)

    comment_body = body["body"]
    file_path = body.get("path", "")
    line_num = body.get("line", 0)

    code_context = None
    if file_path:
        reviews_dir = _config["_state_dir"] / "reviews" / repo
        if reviews_dir.exists():
            for branch_dir in reviews_dir.iterdir():
                diff_file = branch_dir / "diff.txt"
                if diff_file.exists():
                    diff_text = diff_file.read_text()
                    code_context = review_store.extract_code_context(diff_text, file_path, line_num)
                    break

    simplified = _simplify_body_with_context(comment_body, code_context, file_path, line_num)
    return {"simplified": simplified}
