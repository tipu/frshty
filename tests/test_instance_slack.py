import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "instance_script_slack", Path(__file__).resolve().parent.parent / "scripts" / "instance.py")
instance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(instance)

TOKENS = {"aimyable": {"token": "tok-a", "cookie": "d=a"},
          "quillmeetings": {"token": "tok-q", "cookie": "d=q"}}


@pytest.fixture
def host(tmp_path):
    slack = tmp_path / "dev" / "slack_int"
    (slack / "messages" / "aimyable").mkdir(parents=True)
    (slack / "send.py").write_text("")
    (slack / "resolve_user.py").write_text("")
    (slack / "tokens.json").write_text(json.dumps(TOKENS))
    (slack / "tokens.json.bak").write_text(json.dumps(TOKENS))
    (slack / ".env").write_text("SLACK_INT_TOKEN=x\n")
    root = tmp_path / "containers" / "aimyable"
    (root / "state").mkdir(parents=True)
    (root / "ssh").mkdir()
    workspace = tmp_path / "ws"
    workspace.mkdir()
    config_path = root / "aimyable.toml"
    config_path.write_text("")
    return slack, root, workspace, config_path


def config(checkout, workspace, **extra):
    c = {"job": {"key": "aimyable"}, "workspace": {"root": str(workspace)},
         "container": {"slack_int": str(checkout)}}
    c.update(extra)
    return c


def test_workspace_instance_gets_only_its_own_token(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace, slack={"workspace": "aimyable",
                                        "messages_dir": str(slack / "messages" / "aimyable")},
               features={"slack": True})
    instance.write_slack_tokens(c, root)
    tokens = root / "slack" / "tokens.json"
    assert json.loads(tokens.read_text()) == {"aimyable": TOKENS["aimyable"]}
    assert tokens.stat().st_mode & 0o777 == 0o600
    binds = instance.mounts(c, config_path, root)
    assert (str(tokens), str(slack / "tokens.json"), True) in binds
    assert (str(slack / "send.py"), str(slack / "send.py"), True) in binds
    assert (str(slack / "resolve_user.py"), str(slack / "resolve_user.py"), True) in binds
    assert not any(host == str(slack / "tokens.json") for host, _, _ in binds)
    assert not any(inside == str(slack / "tokens.json.bak") for _, inside, _ in binds)
    assert instance.slack_int_view(c, binds) == (str(slack), True)


def test_covering_mount_without_workspace_hides_every_token(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace)
    c["container"]["mounts"] = [str(slack.parent)]
    instance.write_slack_tokens(c, root)
    tokens = root / "slack" / "tokens.json"
    assert json.loads(tokens.read_text()) == {}
    binds = instance.mounts(c, config_path, root)
    assert (str(tokens), str(slack / "tokens.json"), True) in binds
    assert (str(slack / "messages"), str(slack / "messages"), False) in binds
    assert (str(slack / "send.py"), str(slack / "send.py"), False) in binds
    seen = {inside for _, inside, _ in binds}
    assert str(slack / "tokens.json.bak") not in seen
    assert str(slack / ".env") not in seen
    secrets = (str(slack / "tokens.json"), str(slack / "tokens.json.bak"), str(slack / ".env"))
    assert not any(host in secrets for host, _, _ in binds)
    assert instance.slack_int_view(c, binds) == (str(slack), False)


def test_no_covering_mount_and_no_workspace_mounts_nothing(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace)
    instance.write_slack_tokens(c, root)
    binds = instance.mounts(c, config_path, root)
    assert not any(str(slack) in inside for _, inside, _ in binds)
    assert instance.slack_int_view(c, binds) is None


def test_missing_workspace_token_is_fatal(host):
    slack, root, workspace, _ = host
    c = config(slack, workspace, slack={"workspace": "clarivis"})
    with pytest.raises(SystemExit, match="no token for workspace 'clarivis'"):
        instance.write_slack_tokens(c, root)


def test_whole_checkout_mount_beside_a_workspace_is_refused(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace, slack={"workspace": "aimyable"})
    c["container"]["mounts"] = [str(slack)]
    instance.write_slack_tokens(c, root)
    with pytest.raises(SystemExit, match="mount a parent"):
        instance.mounts(c, config_path, root)
