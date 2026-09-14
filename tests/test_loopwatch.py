"""Covers services/loopwatch and services/loopwatch_probe.

Every test builds a throwaway loop checkout: a charter, a controller database
with the tables the probe discovers, and one run directory per iteration. No
test reads a real loop, and the only process a test starts is the probe itself.
"""
import hashlib
import json
import os
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services import loopwatch, loopwatch_probe  # noqa: E402

PROBE = ROOT / "services" / "loopwatch_probe.py"

SCHEMA = """
CREATE TABLE runs (
  id INTEGER PRIMARY KEY, task_id INTEGER, role TEXT, base_version INTEGER,
  result_version INTEGER, pid INTEGER, deadline_at TEXT, started_at TEXT,
  heartbeat_at TEXT, ended_at TEXT, exit_reason TEXT, cost_cents INTEGER DEFAULT 0);
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY, kind TEXT, objective TEXT, acceptance TEXT,
  status TEXT, attempts INTEGER DEFAULT 0, created_by_run INTEGER);
CREATE TABLE decisions (
  id INTEGER PRIMARY KEY, run_id INTEGER, title TEXT, rationale TEXT);
CREATE TABLE halts (
  id INTEGER PRIMARY KEY, reason TEXT, severity TEXT, opened_at TEXT, resolved_at TEXT);
CREATE TABLE state (
  version INTEGER PRIMARY KEY, doc TEXT, run_id INTEGER, created_at TEXT);
"""

CHARTER = """# Charter

## Objective

Ship one thing.

The rest of the objective.

## Money

Nothing.
"""


def build_loop(path, runs=3, note=None, exits=("done", "done", "done"),
               objectives=None, halt=False, wal=False, last_tick=None, turns=4):
    """A checkout shaped like the real loops, small enough to assert on."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "CHARTER.md").write_text(CHARTER)
    var = path / "var"
    var.mkdir(exist_ok=True)
    conn = sqlite3.connect(var / "controller.db")
    if wal:
        conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    version = 1
    if note:
        conn.execute("INSERT INTO state (version, doc, created_at) VALUES (?,?,?)",
                     (version, "HUMAN NOTE 2026-01-01 00:00:00. %s\n\nolder state" % note,
                      "2026-01-01 00:00:00"))
        version += 1
    for index in range(runs):
        run_id = index + 1
        objective = (objectives or ["objective %d" % run_id] * runs)[index]
        conn.execute(
            "INSERT INTO tasks (id, kind, objective, acceptance, status) VALUES (?,?,?,?,?)",
            (run_id, "planning", objective, "accepted when done", "done"))
        conn.execute(
            "INSERT INTO runs (id, task_id, role, base_version, result_version, "
            "started_at, heartbeat_at, ended_at, exit_reason, cost_cents) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run_id, run_id, "operator", version, version + 1,
             "2026-01-0%d 10:00:00" % run_id, "2026-01-0%d 10:05:00" % run_id,
             "2026-01-0%d 10:05:00" % run_id, exits[index], 10 * run_id))
        conn.execute("INSERT INTO decisions (run_id, title, rationale) VALUES (?,?,?)",
                     (run_id, "decision %d" % run_id, "because"))
        conn.execute("INSERT INTO decisions (run_id, title, rationale) VALUES (?,?,?)",
                     (run_id, "session cost", "twice"))
        conn.execute("INSERT INTO decisions (run_id, title, rationale) VALUES (?,?,?)",
                     (run_id, "session cost", "twice"))
        conn.execute("INSERT INTO state (version, doc, run_id, created_at) VALUES (?,?,?,?)",
                     (version + 1, "state after run %d" % run_id, run_id,
                      "2026-01-0%d 10:05:00" % run_id))
        version += 1
        run_dir = var / "controller-runs" / str(run_id)
        run_dir.mkdir(parents=True)
        (run_dir / "result.json").write_text(json.dumps(
            {"exit_reason": exits[index], "state_doc": "state after run %d" % run_id}))
        (run_dir / "claude.json").write_text("\n".join([
            json.dumps({"type": "assistant",
                        "message": {"content": [{"type": "text", "text": "thinking"},
                                                {"type": "tool_use", "name": "Bash"}]}}),
            json.dumps({"type": "result", "result": "did run %d" % run_id,
                        "num_turns": turns}),
        ]))
    if halt:
        conn.execute("INSERT INTO halts (reason, severity, opened_at) VALUES (?,?,?)",
                     ("something broke", "all", "2026-01-01 00:00:00"))
    lines = [json.dumps({"outcome": "started", "run_id": index + 1,
                         "rejected": ["task rejected: already open"]})
             for index in range(runs)]
    if last_tick:
        lines.append(json.dumps({"outcome": last_tick, "run_id": runs}))
    (var / "tick.log").write_text("\n".join(lines))
    conn.commit()
    conn.close()
    return path


def probe(repo, detail=2, summary=5):
    done = subprocess.run([sys.executable, "-", str(repo), str(detail), str(summary)],
                          input=PROBE.read_text(), capture_output=True, text=True)
    return json.loads(done.stdout)


def local_loop(repo, key="test"):
    return {"key": key, "label": key, "host": "", "repo": str(repo), "run_as": "",
            "python": sys.executable, "cadence_minutes": 15}


# ------------------------------------------------------------------- the probe

def test_probe_reads_runs_newest_first(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop"))
    assert snapshot["ok"] is True
    assert [run["id"] for run in snapshot["runs"]] == [3, 2, 1]
    assert [run["detail"] for run in snapshot["runs"]] == [True, True, False]
    newest = snapshot["runs"][0]
    assert newest["objective"] == "objective 3"
    assert newest["exit_reason"] == "done"
    assert newest["seconds"] == 300
    assert newest["narrative"]["final"] == "did run 3"
    assert newest["narrative"]["tools"] == {"Bash": 1}
    assert newest["result"]["state_doc"] == "state after run 3"
    assert newest["tick"]["rejected"] == ["task rejected: already open"]


def test_probe_folds_repeated_rows_and_counts_them(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop"))
    wrote = snapshot["runs"][0]["wrote"]["decisions"]
    assert wrote["count"] == 3
    assert wrote["rows"] == ["decision 3", "session cost ×2"]
    assert wrote["hidden"] == 0


def test_probe_counts_the_rows_it_left_out(tmp_path):
    repo = build_loop(tmp_path / "loop")
    conn = sqlite3.connect(repo / "var" / "controller.db")
    for index in range(20):
        conn.execute("INSERT INTO decisions (run_id, title, rationale) VALUES (3,?,?)",
                     ("extra %d" % index, "x"))
    conn.commit()
    conn.close()
    wrote = probe(repo)["runs"][0]["wrote"]["decisions"]
    assert wrote["count"] == 23
    assert len(wrote["rows"]) == 12
    shown = sum(2 if row.endswith("×2") else 1 for row in wrote["rows"])
    assert wrote["hidden"] == 23 - shown == 10


def test_probe_attributes_a_task_to_the_run_that_created_it(tmp_path):
    repo = build_loop(tmp_path / "loop")
    conn = sqlite3.connect(repo / "var" / "controller.db")
    conn.execute("INSERT INTO tasks (id, kind, objective, acceptance, status, "
                 "created_by_run) VALUES (99, 'planning', 'a new task', '', 'open', 3)")
    conn.commit()
    conn.close()
    snapshot = probe(repo)
    assert snapshot["runs"][0]["wrote"]["tasks"]["rows"] == ["a new task [open]"]


def test_probe_reports_the_charter_digest(tmp_path):
    repo = build_loop(tmp_path / "loop")
    assert probe(repo)["charter"]["sha_ok"] is None
    (repo / "CHARTER.md.sha256").write_text("0" * 64 + "  CHARTER.md\n")
    assert probe(repo)["charter"]["sha_ok"] is False
    digest = hashlib.sha256((repo / "CHARTER.md").read_bytes()).hexdigest()
    (repo / "CHARTER.md.sha256").write_text(digest)
    snapshot = probe(repo)
    assert snapshot["charter"]["sha_ok"] is True
    assert snapshot["charter"]["objective"].startswith("Ship one thing.")


def test_probe_names_the_run_that_first_read_a_note(tmp_path):
    repo = build_loop(tmp_path / "loop", note="turn left")
    notes = probe(repo)["notes"]
    assert len(notes) == 1
    assert notes[0]["text"] == "turn left"
    assert notes[0]["read_by_run"] == 1


def test_probe_reports_a_missing_checkout_rather_than_crashing(tmp_path):
    snapshot = probe(tmp_path / "nowhere")
    assert snapshot["ok"] is False
    assert "no such directory" in snapshot["error"]


def test_probe_reports_a_checkout_with_no_database(tmp_path):
    (tmp_path / "loop").mkdir()
    snapshot = probe(tmp_path / "loop")
    assert snapshot["ok"] is False
    assert "no controller database" in snapshot["error"]


def test_probe_survives_a_truncated_transcript(tmp_path):
    repo = build_loop(tmp_path / "loop")
    (repo / "var" / "controller-runs" / "3" / "claude.json").write_text(
        json.dumps({"type": "assistant",
                    "message": {"content": [{"type": "text", "text": "half a thought"}]}})
        + '\n{"type": "assis')
    narrative = probe(repo)["runs"][0]["narrative"]
    assert narrative["final"] == "half a thought"
    assert narrative["truncated"] is True


def test_probe_is_python_38_syntax():
    """The linode loop runs Python 3.8.2, so the probe must parse there."""
    done = subprocess.run(
        [sys.executable, "-c",
         "import ast,sys; ast.parse(open(sys.argv[1]).read(), feature_version=(3,8))",
         str(PROBE)], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr


# --------------------------------------------------------------- configuration

def write_config(tmp_path, body):
    path = tmp_path / "loops.toml"
    path.write_text(body)
    return path


def test_config_refuses_a_broken_entry_and_keeps_the_rest(tmp_path):
    config = write_config(tmp_path, """
[[loops]]
key = "good"
repo = "/srv/loop"

[[loops]]
key = "relative"
repo = "loop"

[[loops]]
repo = "/srv/other"

[[loops]]
key = "good"
repo = "/srv/twice"
""")
    loops, errors = loopwatch.load_loops(config)
    assert [loop["key"] for loop in loops] == ["good"]
    assert len(errors) == 3
    assert any("absolute repo path" in e for e in errors)
    assert any("has no key" in e for e in errors)
    assert any("named twice" in e for e in errors)


def test_config_refuses_run_as_it_cannot_honour_here(tmp_path):
    config = write_config(tmp_path, """
[[loops]]
key = "local"
repo = "/srv/loop"
run_as = "somebody-else"
""")
    loops, errors = loopwatch.load_loops(config)
    assert loops == []
    assert "cannot be honoured" in errors[0]


def test_config_reports_a_missing_file(tmp_path):
    loops, errors = loopwatch.load_loops(tmp_path / "absent.toml")
    assert loops == []
    assert "no loop config" in errors[0]


# ------------------------------------------------------------------- transport

def test_remote_argv_quotes_the_path_and_the_note():
    loop = {"key": "k", "label": "k", "host": "box", "repo": "/srv/a loop",
            "run_as": "tipu", "python": "python3", "cadence_minutes": None}
    command = loopwatch.probe_argv(loop, 5, 15)[-1]
    inner = shlex.split(command)
    assert inner[:3] == ["su", "-s", "/bin/sh"]
    assert shlex.split(inner[-1]) == ["python3", "-", "/srv/a loop", "5", "15"]

    note = "don't; rm -rf /"
    command = loopwatch.steer_argv(loop, note)[-1]
    assert shlex.split(shlex.split(command)[-1]) == ["/srv/a loop/bin/note.sh", note]


def test_local_argv_does_not_go_through_ssh():
    loop = {"key": "k", "label": "k", "host": "", "repo": "/srv/loop", "run_as": "",
            "python": "python3", "cadence_minutes": None}
    assert loopwatch.probe_argv(loop, 5, 15)[0] == "python3"
    assert loopwatch.steer_argv(loop, "hi") == ["/srv/loop/bin/note.sh", "hi"]


def test_collect_reads_a_local_loop(tmp_path):
    snapshot = loopwatch.collect(local_loop(build_loop(tmp_path / "loop")), 2, 5)
    assert snapshot["ok"] is True
    assert snapshot["key"] == "test"
    assert [run["id"] for run in snapshot["runs"]] == [3, 2, 1]


def test_collect_turns_an_unreachable_loop_into_data(tmp_path):
    loop = local_loop(tmp_path / "loop")
    loop["python"] = str(tmp_path / "no-such-python")
    snapshot = loopwatch.collect(loop, 2, 5)
    assert snapshot["ok"] is False
    assert snapshot["error"]
    assert snapshot["runs"] == []


def test_steer_passes_the_note_through_untouched(tmp_path):
    repo = build_loop(tmp_path / "loop")
    (repo / "bin").mkdir()
    note_sh = repo / "bin" / "note.sh"
    note_sh.write_text('#!/bin/sh\nprintf %s "$1" > "$(dirname "$0")/../seen.txt"\n')
    note_sh.chmod(0o755)
    text = "stop; do 'this' \"instead\"\nsecond line"
    result = loopwatch.steer(local_loop(repo), text)
    assert result["ok"] is True
    assert (repo / "seen.txt").read_text() == text


def test_steer_refuses_an_empty_note(tmp_path):
    with pytest.raises(ValueError):
        loopwatch.steer(local_loop(tmp_path / "loop"), "   ")


# --------------------------------------------------------------------- reading

def test_rollup_counts_what_the_older_runs_did(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop", runs=3,
                                exits=("failed", "done", "done")), detail=1, summary=5)
    stats = loopwatch.rollup([run for run in snapshot["runs"] if not run["detail"]])
    assert stats["runs"] == 2
    assert stats["first"] == 1 and stats["last"] == 2
    assert stats["exit"] == {"failed": 1, "done": 1}
    assert stats["wrote"]["decisions"] == 6
    assert stats["rejected"] == {"task rejected: already open": 2}
    assert stats["cost_cents"] == 30


def test_attention_names_a_failing_streak_and_a_repeated_objective(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop", runs=3,
                                exits=("done", "failed", "failed"),
                                objectives=["a", "same", "same"]), detail=1, summary=5)
    snapshot["cadence_minutes"] = 15
    lines = [text for _, text in loopwatch.attention(snapshot)]
    assert any("2 runs in a row did not finish clean" in line for line in lines)
    assert any("2 of the last 3 runs took the same objective" in line for line in lines)
    assert any("refused the same thing 3 times" in line for line in lines)


def test_attention_names_an_open_halt(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop", halt=True))
    levels = loopwatch.attention(snapshot)
    assert ("bad", "halt #1 [all] something broke") in levels


def test_attention_says_nothing_is_wrong_when_nothing_is(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop",
                                objectives=["a", "b", "c"]))
    snapshot["cadence_minutes"] = 15
    lines = [text for _, text in loopwatch.attention(snapshot)]
    assert not any("did not finish clean" in line for line in lines)
    assert not any("same objective" in line for line in lines)


def test_attention_reports_an_unreachable_loop(tmp_path):
    loop = local_loop(tmp_path / "loop")
    loop["python"] = str(tmp_path / "no-such-python")
    assert loopwatch.attention(loopwatch.collect(loop, 1, 1))[0][0] == "bad"


# ------------------------------------------------------------------- rendering

def test_render_shows_every_iteration_and_the_steer_command(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop"))
    snapshot.update({"key": "games", "label": "games", "where": "this host",
                     "cadence_minutes": 15})
    page = loopwatch.render([snapshot])
    assert "the last 2 iterations" in page
    assert "the 1 runs before that" in page
    assert "python -m services.loopwatch steer games" in page
    assert "did run 3" in page
    assert "task rejected: already open" in page
    assert "Ship one thing." in page
    assert "The rest of the objective." in page


def test_render_escapes_what_a_session_wrote(tmp_path):
    repo = build_loop(tmp_path / "loop")
    conn = sqlite3.connect(repo / "var" / "controller.db")
    conn.execute("UPDATE tasks SET objective='<script>alert(1)</script>' WHERE id=3")
    conn.commit()
    conn.close()
    snapshot = probe(repo)
    snapshot.update({"key": "k", "label": "k", "where": "this host"})
    page = loopwatch.render([snapshot])
    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page


def test_render_shows_a_loop_it_could_not_read(tmp_path):
    loop = local_loop(tmp_path / "loop", key="broken")
    loop["python"] = str(tmp_path / "no-such-python")
    page = loopwatch.render([loopwatch.collect(loop, 1, 1)],
                            errors=["loop 'x' has no key"])
    assert "unreachable" in page
    assert "loop &#x27;x&#x27; has no key" in page


def test_poll_writes_the_page_and_reports_a_bad_source(tmp_path):
    good = build_loop(tmp_path / "good")
    config = write_config(tmp_path, """
[[loops]]
key = "good"
repo = "%s"
python = "%s"

[[loops]]
key = "gone"
repo = "%s"
python = "%s"
""" % (good, sys.executable, tmp_path / "gone", sys.executable))
    out = tmp_path / "page" / "index.html"
    args = loopwatch.build_parser().parse_args(
        ["--config", str(config), "poll", "--out", str(out), "--detail", "2",
         "--summary", "5"])
    assert args.func(args) == 1
    page = out.read_text()
    assert "did run 3" in page
    assert "no such directory" in page


def test_probe_reads_a_wal_database_without_leaving_a_file(tmp_path):
    """The read-only open cannot create the -shm a WAL read needs, so the probe
    reads a copy instead. Nothing new may appear beside the live database."""
    repo = build_loop(tmp_path / "loop", wal=True)
    var = repo / "var"
    assert not (var / "controller.db-shm").exists()
    before = sorted(path.name for path in var.iterdir())
    snapshot = probe(repo)
    assert snapshot["ok"] is True
    assert [run["id"] for run in snapshot["runs"]] == [3, 2, 1]
    assert sorted(path.name for path in var.iterdir()) == before


def test_probe_reads_a_live_wal_database_without_touching_it(tmp_path):
    """A row that lives only in the write-ahead log must still be read, and the
    directory must look the same afterwards."""
    repo = build_loop(tmp_path / "loop", wal=True)
    var = repo / "var"
    holder = sqlite3.connect(var / "controller.db")
    holder.execute(
        "INSERT INTO runs (id, task_id, role, base_version, started_at, "
        "heartbeat_at, ended_at, exit_reason, cost_cents) VALUES "
        "(4, 3, 'operator', 4, '2026-01-04 10:00:00', '2026-01-04 10:01:00', "
        "'2026-01-04 10:01:00', 'done', 0)")
    holder.commit()
    try:
        assert (var / "controller.db-shm").exists()
        before = sorted(path.name for path in var.iterdir())
        snapshot = probe(repo)
        assert snapshot["ok"] is True
        assert [run["id"] for run in snapshot["runs"]] == [4, 3, 2, 1]
        assert sorted(path.name for path in var.iterdir()) == before
    finally:
        holder.close()


def test_collect_refuses_an_answer_that_is_not_an_object(tmp_path):
    fake = tmp_path / "fake-python"
    fake.write_text("#!/bin/sh\necho '[]'\n")
    fake.chmod(0o755)
    loop = local_loop(build_loop(tmp_path / "loop"))
    loop["python"] = str(fake)
    snapshot = loopwatch.collect(loop, 2, 5)
    assert snapshot["ok"] is False
    assert "not an object" in snapshot["error"]


def test_steer_reports_a_note_script_it_cannot_run(tmp_path):
    result = loopwatch.steer(local_loop(build_loop(tmp_path / "loop")), "turn left")
    assert result["ok"] is False
    assert "FileNotFoundError" in result["stderr"]


def test_attention_names_a_tick_that_started_no_run(tmp_path):
    snapshot = probe(build_loop(tmp_path / "loop", last_tick="worker_active"))
    lines = [text for _, text in loopwatch.attention(snapshot)]
    assert any("last tick started no run: worker_active" in line for line in lines)


def test_render_escapes_the_turn_count_from_a_transcript(tmp_path):
    repo = build_loop(tmp_path / "loop", turns="</span><img src=x onerror=alert(1)>")
    snapshot = probe(repo)
    snapshot.update({"key": "k", "label": "k", "where": "this host"})
    page = loopwatch.render([snapshot])
    assert "<img src=x onerror=alert(1)>" not in page
    assert "&lt;img src=x onerror=alert(1)&gt;" in page


def test_copy_aside_takes_every_journal(tmp_path):
    """A database copied without its rollback journal can hold pages no commit
    confirmed, so every journal travels with it. The -shm does not."""
    db = tmp_path / "controller.db"
    for suffix in ("", "-wal", "-journal", "-shm"):
        (tmp_path / ("controller.db" + suffix)).write_bytes(b"x" + suffix.encode())
    copied = loopwatch_probe.copy_aside(str(db))
    try:
        assert sorted(path.name for path in Path(copied).iterdir()) == [
            "controller.db", "controller.db-journal", "controller.db-wal"]
    finally:
        shutil.rmtree(copied, ignore_errors=True)


def test_probe_refuses_a_database_that_never_settles(tmp_path, monkeypatch, capsys):
    """A copy taken while the controller writes can hold halves of two
    databases. The probe reports that rather than reading it."""
    repo = build_loop(tmp_path / "loop")
    counter = {"n": 0}

    def never_settles(db_path):
        counter["n"] += 1
        return [("", counter["n"], counter["n"])]

    monkeypatch.setattr(loopwatch_probe, "stamp", never_settles)
    before = set(os.listdir(tempfile.gettempdir()))
    assert loopwatch_probe.main(["probe", str(repo)]) == 1
    answer = json.loads(capsys.readouterr().out)
    assert answer["ok"] is False
    assert "changed during every one of 5 copies" in answer["error"]
    assert set(os.listdir(tempfile.gettempdir())) - before == set()
