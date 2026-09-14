"""Watch autonomous loops that run outside this board, and steer them.

A loop here is a checkout that keeps its own controller database and runs one
session per iteration. loopwatch never installs anything on the loop's host: it
feeds services/loopwatch_probe.py to python3 over ssh, reads the JSON that comes
back, and renders one page holding every loop. Steering writes a human note into
the loop's own state document, which the next session reads as CURRENT STATE.

    python -m services.loopwatch poll
    python -m services.loopwatch steer games "stop repainting, ship one game"

The loops are named in config/loops.toml; config/loops.example.toml describes
the fields."""

import argparse
import getpass
import html
import json
import shlex
import subprocess
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "loops.toml"
PROBE_PATH = Path(__file__).resolve().parent / "loopwatch_probe.py"
DEFAULT_OUT = Path.home() / ".frshty" / "artifacts" / "loopwatch" / "index.html"
DEFAULT_DETAIL = 5
DEFAULT_SUMMARY = 15
DEFAULT_TIMEOUT = 180
CONNECT_TIMEOUT = 10
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=%d" % CONNECT_TIMEOUT)


# ---------------------------------------------------------------- configuration

def load_loops(path=CONFIG_PATH):
    """The loops this host watches, and every reason a line was refused.

    Errors are returned rather than raised so the page can show a broken entry
    instead of dropping it."""
    path = Path(path)
    if not path.exists():
        return [], ["no loop config at %s (copy config/loops.example.toml)" % path]
    try:
        with open(path, "rb") as handle:
            raw = tomllib.load(handle)
    except Exception as exc:
        return [], ["%s is unreadable: %s: %s" % (path, type(exc).__name__, exc)]
    loops, errors, seen = [], [], set()
    for index, entry in enumerate(raw.get("loops") or []):
        if not isinstance(entry, dict):
            errors.append("loops[%d] is not a table" % index)
            continue
        key = str(entry.get("key") or "").strip()
        repo = str(entry.get("repo") or "").strip()
        host = str(entry.get("host") or "").strip()
        run_as = str(entry.get("run_as") or "").strip()
        if not key:
            errors.append("loops[%d] has no key" % index)
            continue
        if key in seen:
            errors.append("loop %r is named twice" % key)
            continue
        if not repo or not repo.startswith("/"):
            errors.append("loop %r needs an absolute repo path" % key)
            continue
        if run_as and not host and run_as != getpass.getuser():
            errors.append("loop %r runs here as %s, so run_as=%r cannot be honoured"
                          % (key, getpass.getuser(), run_as))
            continue
        seen.add(key)
        loops.append({
            "key": key,
            "label": str(entry.get("label") or key),
            "host": host,
            "repo": repo,
            "run_as": run_as,
            "python": str(entry.get("python") or "python3").strip(),
            "cadence_minutes": entry.get("cadence_minutes"),
        })
    return loops, errors


def where(loop):
    return loop["host"] or "this host"


# ------------------------------------------------------------------- transport

def _wrap(loop, inner):
    if loop["run_as"] and loop["host"]:
        return "su -s /bin/sh %s -c %s" % (shlex.quote(loop["run_as"]), shlex.quote(inner))
    return inner


def probe_argv(loop, detail, summary):
    """The argv that reads one loop. The probe arrives on stdin, never on disk."""
    if not loop["host"]:
        return [loop["python"], "-", loop["repo"], str(detail), str(summary)]
    inner = "%s - %s %d %d" % (loop["python"], shlex.quote(loop["repo"]), detail, summary)
    return ["ssh"] + list(SSH_OPTIONS) + [loop["host"], _wrap(loop, inner)]


def steer_argv(loop, text):
    """The argv that writes a human note into one loop's state document."""
    note = loop["repo"].rstrip("/") + "/bin/note.sh"
    if not loop["host"]:
        return [note, text]
    inner = "%s %s" % (shlex.quote(note), shlex.quote(text))
    return ["ssh"] + list(SSH_OPTIONS) + [loop["host"], _wrap(loop, inner)]


def _run(argv, stdin_text=None, timeout=DEFAULT_TIMEOUT):
    return subprocess.run(argv, input=stdin_text, capture_output=True,
                          text=True, timeout=timeout)


def collect(loop, detail=DEFAULT_DETAIL, summary=DEFAULT_SUMMARY,
            timeout=DEFAULT_TIMEOUT):
    """Read one loop. A failure is returned as data, never raised."""
    argv = probe_argv(loop, detail, summary)
    started = time.time()
    try:
        done = _run(argv, stdin_text=PROBE_PATH.read_text(), timeout=timeout)
    except subprocess.TimeoutExpired:
        return _failed(loop, "no answer in %ds" % timeout, argv, started)
    except OSError as exc:
        return _failed(loop, "%s: %s" % (type(exc).__name__, exc), argv, started)
    try:
        snapshot = json.loads(done.stdout)
    except ValueError:
        detail_text = (done.stderr or done.stdout or "").strip().splitlines()
        return _failed(loop, "unreadable answer (exit %d): %s"
                       % (done.returncode, " / ".join(detail_text[-3:]) or "no output"),
                       argv, started)
    if not isinstance(snapshot, dict):
        return _failed(loop, "the probe answered with %s, not an object"
                       % type(snapshot).__name__, argv, started)
    snapshot.update({"key": loop["key"], "label": loop["label"], "where": where(loop),
                     "cadence_minutes": loop["cadence_minutes"],
                     "took_seconds": round(time.time() - started, 1),
                     "argv": argv})
    if not snapshot.get("ok"):
        snapshot.setdefault("error", "the probe reported no reason")
    return snapshot


def _failed(loop, error, argv, started):
    return {"ok": False, "error": error, "key": loop["key"], "label": loop["label"],
            "where": where(loop), "cadence_minutes": loop["cadence_minutes"],
            "took_seconds": round(time.time() - started, 1), "argv": argv,
            "runs": [], "halts": [], "active": [], "notes": [], "ticks": [],
            "counts": {}, "task_status": {}}


def steer(loop, text, timeout=DEFAULT_TIMEOUT):
    """Leave a note the loop's next session reads before it decides anything."""
    if not text.strip():
        raise ValueError("a note needs words")
    argv = steer_argv(loop, text)
    try:
        done = _run(argv, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "returncode": None, "stdout": "",
                "stderr": "%s: %s" % (type(exc).__name__, exc), "argv": argv}
    return {"ok": done.returncode == 0, "returncode": done.returncode,
            "stdout": done.stdout.strip(), "stderr": done.stderr.strip(),
            "argv": argv}


# --------------------------------------------------------------------- reading

def parse_stamp(text):
    if not text:
        return None
    try:
        return datetime.strptime(str(text)[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def age_seconds(now_text, then_text):
    now, then = parse_stamp(now_text), parse_stamp(then_text)
    if not now or not then:
        return None
    return int((now - then).total_seconds())


def human_span(seconds):
    if seconds is None:
        return "?"
    if seconds < 0:
        seconds = 0
    if seconds < 90:
        return "%ds" % seconds
    if seconds < 5400:
        return "%dm" % round(seconds / 60)
    if seconds < 172800:
        return "%.1fh" % (seconds / 3600)
    return "%.1fd" % (seconds / 86400)


def rollup(runs):
    """What the older runs add up to, counted rather than judged."""
    out = {
        "runs": len(runs), "first": None, "last": None, "span_seconds": None,
        "exit": {}, "role": {}, "kind": {}, "wrote": {}, "rejected": {},
        "cost_cents": 0, "seconds": 0, "objectives": {},
    }
    if not runs:
        return out
    ordered = sorted(runs, key=lambda r: r["id"])
    out["first"], out["last"] = ordered[0]["id"], ordered[-1]["id"]
    out["span_seconds"] = age_seconds(ordered[-1].get("started_at"),
                                      ordered[0].get("started_at"))
    for run in runs:
        for field, bucket in (("exit_reason", "exit"), ("role", "role"), ("kind", "kind")):
            value = run.get(field) or "unrecorded"
            out[bucket][value] = out[bucket].get(value, 0) + 1
        for table, wrote in (run.get("wrote") or {}).items():
            out["wrote"][table] = out["wrote"].get(table, 0) + wrote["count"]
        for reason in ((run.get("tick") or {}).get("rejected") or []):
            reason = str(reason)
            out["rejected"][reason] = out["rejected"].get(reason, 0) + 1
        out["cost_cents"] += run.get("cost_cents") or 0
        out["seconds"] += run.get("seconds") or 0
        objective = (run.get("objective") or "").strip()
        if objective:
            out["objectives"][objective] = out["objectives"].get(objective, 0) + 1
    return out


def attention(snapshot):
    """Facts worth the operator's eye, each one read off the snapshot.

    No ranking and no advice: every line here is a count or a clock."""
    if not snapshot.get("ok"):
        return [("bad", "unreachable: %s" % snapshot.get("error"))]
    out = []
    runs = snapshot.get("runs") or []
    now = snapshot.get("now")
    for halt in snapshot.get("halts") or []:
        out.append(("bad", "halt #%s [%s] %s" % (halt.get("id"), halt.get("severity"),
                                                 halt.get("reason"))))
    if snapshot.get("active"):
        run = snapshot["active"][0]
        out.append(("live", "run %s working, started %s (%s ago)"
                    % (run.get("id"), run.get("started_at"),
                       human_span(age_seconds(now, run.get("started_at"))))))
    elif runs:
        gap = age_seconds(now, runs[0].get("ended_at") or runs[0].get("started_at"))
        cadence = snapshot.get("cadence_minutes")
        level = "bad" if cadence and gap is not None and gap > cadence * 60 * 3 else "mut"
        out.append((level, "idle %s since run %s" % (human_span(gap), runs[0].get("id"))))
    ticks = snapshot.get("ticks") or []
    if ticks and ticks[-1].get("outcome") not in (None, "started"):
        out.append(("mut", "the controller's last tick started no run: %s"
                    % ticks[-1]["outcome"]))
    streak = 0
    for run in runs:
        if run.get("exit_reason") in (None, "done"):
            break
        streak += 1
    if streak:
        out.append(("bad" if streak > 1 else "warn",
                    "%d run%s in a row did not finish clean" % (streak, "" if streak == 1 else "s")))
    repeated = [(reason, n) for reason, n in rollup(runs)["rejected"].items() if n > 1]
    for reason, n in sorted(repeated, key=lambda item: -item[1])[:3]:
        out.append(("warn", "the controller refused the same thing %d times: %s" % (n, reason)))
    newest = (runs[0].get("objective") or "").strip() if runs else ""
    if newest:
        same = sum(1 for run in runs if (run.get("objective") or "").strip() == newest)
        if same > 1:
            out.append(("warn" if same < len(runs) else "bad",
                        "%d of the last %d runs took the same objective: %s"
                        % (same, len(runs), newest[:90])))
    charter = snapshot.get("charter") or {}
    if charter.get("sha_ok") is False:
        out.append(("bad", "the charter no longer matches its digest"))
    return out


# ------------------------------------------------------------------- rendering

def esc(value):
    return html.escape("" if value is None else str(value))


def _chip(label, value, level="mut"):
    return ('<span class="chip %s"><b>%s</b> %s</span>'
            % (level, esc(label), esc(value)))


def _counts_line(mapping, limit=8):
    items = sorted(mapping.items(), key=lambda item: -item[1])[:limit]
    return " ".join('<span class="tag">%s <b>%d</b></span>' % (esc(k), v) for k, v in items)


def _run_card(run, now):
    head = [
        '<span class="id">#%s</span>' % esc(run.get("id")),
        esc(run.get("role") or "?"),
        esc(run.get("kind") or "-"),
    ]
    exit_reason = run.get("exit_reason")
    level = "ok" if exit_reason == "done" else ("bad" if exit_reason else "warn")
    head.append('<span class="pill %s">%s</span>' % (level, esc(exit_reason or "running")))
    head.append('<span class="mut">%s ago, took %s</span>'
                % (esc(human_span(age_seconds(now, run.get("started_at")))),
                   esc(human_span(run.get("seconds")))))
    if run.get("cost_cents"):
        head.append('<span class="mut">%d cents</span>' % run["cost_cents"])
    parts = ['<article class="run"><div class="runhead">%s</div>' % " ".join(head)]
    if run.get("objective"):
        parts.append('<p class="obj">%s</p>' % esc(run["objective"]))
    if run.get("acceptance"):
        parts.append('<p class="mut small">accepted when: %s</p>' % esc(run["acceptance"]))

    tick = run.get("tick") or {}
    rejected = tick.get("rejected") or []
    if rejected:
        parts.append('<p class="bad small">the controller refused: %s</p>'
                     % esc("; ".join(str(r) for r in rejected)))

    wrote = run.get("wrote") or {}
    if wrote:
        rows = []
        for table in sorted(wrote):
            body = "".join("<li>%s</li>" % esc(line) for line in wrote[table]["rows"])
            more = wrote[table].get("hidden") or 0
            if more > 0:
                body += '<li class="mut">and %d more</li>' % more
            rows.append("<dt>%s <b>%d</b></dt><dd><ul>%s</ul></dd>"
                        % (esc(table), wrote[table]["count"], body))
        parts.append("<dl class=wrote>%s</dl>" % "".join(rows))
    else:
        parts.append('<p class="mut small">wrote nothing to the database</p>')

    narrative = run.get("narrative") or {}
    if narrative.get("final"):
        tools = narrative.get("tools") or {}
        meta = "%s turns, %s" % (esc(narrative.get("turns") or "?"),
                                _counts_line(tools, 6) or "no tools")
        if narrative.get("truncated"):
            meta += ' <span class="pill bad">transcript ends mid-run</span>'
        parts.append('<details class="said"><summary>what the session said '
                     '<span class="mut">(%s)</span></summary><pre>%s</pre></details>'
                     % (meta, esc(narrative["final"])))
    elif run.get("detail"):
        parts.append('<p class="warn small">the session left no transcript</p>')

    result = run.get("result")
    if isinstance(result, dict) and result.get("state_doc"):
        parts.append('<details><summary>state it wrote</summary><pre>%s</pre></details>'
                     % esc(result["state_doc"]))
    parts.append("</article>")
    return "".join(parts)


def _loop_section(snapshot):
    key = snapshot.get("key")
    parts = ['<section id="%s">' % esc(key)]
    parts.append('<h2>%s <span class="mut">%s &middot; %s</span></h2>'
                 % (esc(snapshot.get("label")), esc(snapshot.get("where")),
                    esc(snapshot.get("repo") or "")))

    lines = "".join('<li class="%s">%s</li>' % (level, esc(text))
                    for level, text in attention(snapshot))
    parts.append('<ul class="attention">%s</ul>' % lines)

    if not snapshot.get("ok"):
        parts.append('<pre class="bad">%s</pre>' % esc(" ".join(snapshot.get("argv") or [])))
        parts.append("</section>")
        return "".join(parts)

    now = snapshot.get("now")
    charter = snapshot.get("charter") or {}
    chips = [
        _chip("runs", snapshot.get("counts", {}).get("runs", "?")),
        _chip("read at", "%s UTC" % now),
        _chip("charter", "matches its digest" if charter.get("sha_ok") else "unverified",
              "ok" if charter.get("sha_ok") else "warn"),
    ]
    if snapshot.get("cadence_minutes"):
        chips.append(_chip("tick", "every %s min" % snapshot["cadence_minutes"]))
    if snapshot.get("task_status"):
        chips.append(_chip("tasks", ", ".join("%s %d" % (k, v) for k, v in
                                              sorted(snapshot["task_status"].items()))))
    parts.append('<div class="chips">%s</div>' % "".join(chips))
    totals = {name: n for name, n in (snapshot.get("counts") or {}).items()
              if name != "runs" and n}
    if totals:
        parts.append('<p class="mut small">database holds %s</p>' % _counts_line(totals, 10))
    objective = (charter.get("objective") or "").strip()
    if objective:
        head, _, rest = objective.partition("\n\n")
        parts.append("<blockquote>%s</blockquote>" % esc(head))
        if rest.strip():
            parts.append('<details><summary>the rest of the objective</summary>'
                         "<pre>%s</pre></details>" % esc(rest.strip()))

    runs = snapshot.get("runs") or []
    detail = [r for r in runs if r.get("detail")]
    older = [r for r in runs if not r.get("detail")]

    parts.append("<h3>steer it</h3>")
    parts.append('<p class="mut small">A note goes to the front of the state document. '
                 'The next session reads it as CURRENT STATE before it picks anything.</p>')
    parts.append("<pre>python -m services.loopwatch steer %s %s</pre>"
                 % (esc(key), esc('"say what to do instead"')))
    notes = snapshot.get("notes") or []
    if notes:
        rows = []
        for note in notes:
            read_by = note.get("read_by_run")
            rows.append("<tr><td>%s</td><td>%s</td><td>%s</td></tr>"
                        % (esc(note.get("at")), esc(note.get("text")),
                           "run %s" % esc(read_by) if read_by else
                           '<span class="warn">not read yet</span>'))
        parts.append('<table class="notes"><thead><tr><th>sent</th><th>note</th>'
                     "<th>first read by</th></tr></thead><tbody>%s</tbody></table>"
                     % "".join(rows))
    else:
        parts.append('<p class="mut small">no note has been sent to this loop.</p>')

    parts.append("<h3>the last %d iterations</h3>" % len(detail))
    parts.append("".join(_run_card(run, now) for run in detail) or
                 '<p class="mut">no run yet</p>')

    stats = rollup(older)
    parts.append("<h3>the %d runs before that</h3>" % stats["runs"])
    if not older:
        parts.append('<p class="mut">nothing older</p>')
    else:
        parts.append('<p>runs %s to %s, %s of wall clock, %d cents, '
                     "%s of session time.</p>"
                     % (esc(stats["first"]), esc(stats["last"]),
                        esc(human_span(stats["span_seconds"])), stats["cost_cents"],
                        esc(human_span(stats["seconds"]))))
        parts.append("<dl class=rollup>")
        parts.append("<dt>ended</dt><dd>%s</dd>" % _counts_line(stats["exit"]))
        parts.append("<dt>role</dt><dd>%s</dd>" % _counts_line(stats["role"]))
        parts.append("<dt>kind</dt><dd>%s</dd>" % _counts_line(stats["kind"]))
        parts.append("<dt>wrote</dt><dd>%s</dd>" % (_counts_line(stats["wrote"]) or "nothing"))
        if stats["rejected"]:
            parts.append("<dt>refused</dt><dd>%s</dd>" % _counts_line(stats["rejected"], 6))
        repeats = {o: n for o, n in stats["objectives"].items() if n > 1}
        if repeats:
            parts.append("<dt>same objective more than once</dt><dd>%s</dd>"
                         % _counts_line(repeats, 5))
        parts.append("</dl>")
        rows = []
        for run in older:
            rows.append("<tr><td>#%s</td><td>%s</td><td>%s</td><td>%s</td>"
                        "<td>%s</td><td>%s</td></tr>"
                        % (esc(run.get("id")), esc(run.get("role")), esc(run.get("kind")),
                           esc(run.get("exit_reason") or "running"),
                           esc(human_span(run.get("seconds"))),
                           esc(run.get("objective") or "")))
        parts.append('<table class="runs"><thead><tr><th>run</th><th>role</th>'
                     "<th>kind</th><th>ended</th><th>took</th><th>objective</th></tr>"
                     "</thead><tbody>%s</tbody></table>" % "".join(rows))

    state = snapshot.get("state") or {}
    if state.get("head"):
        parts.append('<details><summary>current state, version %s</summary><pre>%s</pre></details>'
                     % (esc(state.get("version")), esc(state["head"])))
    parts.append("</section>")
    return "".join(parts)


CSS = """
:root { color-scheme: light dark; --fg:#1a1a1a; --bg:#fbfaf8; --mut:#5d5d5d;
        --line:#dcd8d2; --code:#f1eee9; --bad:#a32020; --warn:#8a4b00;
        --ok:#1d6b32; --live:#1f4f9c; }
@media (prefers-color-scheme: dark) {
  :root { --fg:#e8e6e3; --bg:#17181a; --mut:#a6a29c; --line:#33353a; --code:#202226;
          --bad:#e06c6c; --warn:#e0a35c; --ok:#6fc98a; --live:#79a9f0; }
}
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--fg);
       font:16px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width:60rem; margin:0 auto; padding:2rem 1.1rem 4rem; }
h1 { font-size:1.5rem; margin:0 0 .2rem; }
h2 { font-size:1.2rem; margin:0 0 .6rem; }
h3 { font-size:1rem; margin:1.6rem 0 .5rem; padding-top:.7rem; border-top:1px solid var(--line); }
section { margin:2.4rem 0 0; padding-top:1.2rem; border-top:2px solid var(--line); }
p { margin:.45rem 0; }
.mut { color:var(--mut); } .bad { color:var(--bad); } .warn { color:var(--warn); }
.ok { color:var(--ok); } .live { color:var(--live); }
.small { font-size:.86rem; }
code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:.83em; }
pre { background:var(--code); border:1px solid var(--line); border-radius:6px;
      padding:.7rem .8rem; overflow-x:auto; white-space:pre-wrap; word-break:break-word; }
blockquote { margin:.8rem 0; padding:.1rem 0 .1rem .9rem; border-left:3px solid var(--line);
             color:var(--mut); white-space:pre-wrap; }
.chips { margin:.6rem 0; }
.chip, .tag { display:inline-block; border:1px solid var(--line); border-radius:999px;
              padding:.08rem .55rem; margin:.15rem .3rem .15rem 0; font-size:.8rem; }
.chip b, .tag b { font-weight:600; }
ul.attention { list-style:none; margin:.7rem 0; padding:0; }
ul.attention li { padding:.18rem 0 .18rem .9rem; border-left:3px solid currentColor; margin:.25rem 0; }
ul.attention li.mut { color:var(--mut); }
article.run { border:1px solid var(--line); border-radius:8px; padding:.7rem .85rem; margin:.7rem 0; }
.runhead { display:flex; flex-wrap:wrap; gap:.5rem; align-items:baseline; font-size:.9rem; }
.runhead .id { font-weight:700; font-size:1rem; }
.pill { border-radius:999px; padding:.03rem .5rem; font-size:.76rem; border:1px solid currentColor; }
.obj { margin:.45rem 0; }
dl.wrote, dl.rollup { margin:.5rem 0; display:grid; grid-template-columns:minmax(6rem,11rem) 1fr;
                      gap:.15rem .8rem; font-size:.88rem; }
dl.wrote dt, dl.rollup dt { color:var(--mut); }
dl.wrote dd, dl.rollup dd { margin:0; }
dl.wrote ul { margin:0; padding-left:1.1rem; }
details { margin:.45rem 0; }
summary { cursor:pointer; font-size:.88rem; }
table { border-collapse:collapse; width:100%; margin:.6rem 0; font-size:.85rem; }
th, td { text-align:left; vertical-align:top; padding:.35rem .5rem; border-bottom:1px solid var(--line); }
th { color:var(--mut); font-weight:600; }
table.runs td:last-child { color:var(--mut); }
.errors { border:1px solid var(--bad); border-radius:6px; padding:.5rem .8rem; }
@media (max-width:640px) {
  dl.wrote, dl.rollup { grid-template-columns:1fr; }
  dl.wrote dt, dl.rollup dt { margin-top:.4rem; }
}
"""


def render(snapshots, errors=(), generated_at=None):
    generated = generated_at or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    names = ", ".join("%s (%s)" % (s.get("label"), s.get("where")) for s in snapshots)
    body = [
        "<!doctype html><html lang=en><head><meta charset=utf-8>",
        '<meta name=viewport content="width=device-width, initial-scale=1">',
        "<title>loops</title><style>%s</style></head><body><main>" % CSS,
        "<h1>loops</h1>",
        '<p class="mut">polled %s &middot; %s</p>' % (esc(generated), esc(names or "nothing")),
    ]
    if errors:
        body.append('<div class="errors"><b>config</b><ul>%s</ul></div>'
                    % "".join("<li>%s</li>" % esc(e) for e in errors))
    for snapshot in snapshots:
        body.append(_loop_section(snapshot))
    body.append('<section><h3>how this page is made</h3><p class="mut small">'
                "One command reads every loop over ssh and writes this file. It installs "
                "nothing on the loop's host: the probe arrives on stdin and the controller "
                "database is opened read-only.</p>"
                "<pre>python -m services.loopwatch poll</pre></section>")
    body.append("</main></body></html>")
    return "".join(body)


# ------------------------------------------------------------------------- cli

def poll(args):
    loops, errors = load_loops(args.config)
    snapshots = [collect(loop, args.detail, args.summary, args.timeout) for loop in loops]
    for snapshot in snapshots:
        if not snapshot.get("ok"):
            print("%s: %s" % (snapshot["key"], snapshot.get("error")), file=sys.stderr)
    for error in errors:
        print("config: %s" % error, file=sys.stderr)
    if args.json:
        print(json.dumps(snapshots, indent=1))
        return 0 if all(s.get("ok") for s in snapshots) and not errors else 1
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(snapshots, errors))
    print(out)
    return 0 if snapshots and all(s.get("ok") for s in snapshots) and not errors else 1


def poll_forever(args):
    while True:
        poll(args)
        time.sleep(max(60, args.watch))


def steer_command(args):
    loops, errors = load_loops(args.config)
    for error in errors:
        print("config: %s" % error, file=sys.stderr)
    match = [loop for loop in loops if loop["key"] == args.key]
    if not match:
        print("no loop named %r in %s" % (args.key, args.config), file=sys.stderr)
        return 2
    result = steer(match[0], args.text, args.timeout)
    print(result["stdout"] or "(no output)")
    if result["stderr"]:
        print(result["stderr"], file=sys.stderr)
    return 0 if result["ok"] else 1


def build_parser():
    parser = argparse.ArgumentParser(prog="loopwatch", description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    sub = parser.add_subparsers(dest="command")

    poller = sub.add_parser("poll", help="read every loop and write the page")
    poller.add_argument("--out", default=str(DEFAULT_OUT))
    poller.add_argument("--detail", type=int, default=DEFAULT_DETAIL,
                        help="iterations shown in full (default %d)" % DEFAULT_DETAIL)
    poller.add_argument("--summary", type=int, default=DEFAULT_SUMMARY,
                        help="older iterations counted (default %d)" % DEFAULT_SUMMARY)
    poller.add_argument("--json", action="store_true", help="print the snapshots instead")
    poller.add_argument("--watch", type=int, default=0, metavar="SECONDS",
                        help="keep polling every SECONDS")
    poller.set_defaults(func=lambda args: poll_forever(args) if args.watch else poll(args))

    steerer = sub.add_parser("steer", help="tell one loop to do something else")
    steerer.add_argument("key")
    steerer.add_argument("text")
    steerer.set_defaults(func=steer_command)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
