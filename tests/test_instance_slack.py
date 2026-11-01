import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "instance_script_slack", Path(__file__).resolve().parent.parent / "scripts" / "instance.py")
instance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(instance)

TOKENS = {"apexco": {"token": "tok-a", "cookie": "d=a"},
          "quartzmeetings": {"token": "tok-q", "cookie": "d=q"}}


@pytest.fixture
def host(tmp_path):
    slack = tmp_path / "dev" / "slack_int"
    (slack / "messages" / "apexco").mkdir(parents=True)
    (slack / "send.py").write_text("")
    (slack / "resolve_user.py").write_text("")
    (slack / "tokens.json").write_text(json.dumps(TOKENS))
    (slack / "tokens.json.bak").write_text(json.dumps(TOKENS))
    (slack / ".env").write_text("SLACK_INT_TOKEN=x\n")
    root = tmp_path / "containers" / "apexco"
    (root / "state").mkdir(parents=True)
    (root / "ssh").mkdir()
    workspace = tmp_path / "ws"
    workspace.mkdir()
    config_path = root / "apexco.toml"
    config_path.write_text("")
    return slack, root, workspace, config_path


def config(checkout, workspace, **extra):
    c = {"job": {"key": "apexco"}, "workspace": {"root": str(workspace)},
         "container": {"slack_int": str(checkout)}}
    c.update(extra)
    return c


def test_workspace_instance_gets_only_its_own_token(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace, slack={"workspace": "apexco",
                                        "messages_dir": str(slack / "messages" / "apexco")},
               features={"slack": True})
    instance.write_slack_tokens(c, root)
    tokens = root / "slack" / "tokens.json"
    assert json.loads(tokens.read_text()) == {"apexco": TOKENS["apexco"]}
    assert tokens.stat().st_mode & 0o777 == 0o600
    binds = instance.mounts(c, config_path, root)
    assert (str(tokens), str(slack / "tokens.json"), True) in binds
    assert (str(slack / "send.py"), str(slack / "send.py"), True) in binds
    assert (str(slack / "resolve_user.py"), str(slack / "resolve_user.py"), True) in binds
    assert not any(host == str(slack / "tokens.json") for host, _, _ in binds)
    assert not any(inside == str(slack / "tokens.json.bak") for _, inside, _ in binds)
    assert instance.slack_int_views(c, binds) == [str(slack)]


def test_covering_mount_without_workspace_hides_every_token(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace)
    c["container"]["mounts"] = [str(slack.parent)]
    instance.write_slack_tokens(c, root)
    tokens = root / "slack" / "tokens.json"
    assert json.loads(tokens.read_text()) == {}
    binds = instance.mounts(c, config_path, root)
    assert (str(tokens), str(slack / "tokens.json"), True) in binds
    assert str(slack / "messages") not in {inside for _, inside, _ in binds}
    assert (str(slack / "send.py"), str(slack / "send.py"), True) in binds
    seen = {inside for _, inside, _ in binds}
    assert str(slack / "tokens.json.bak") not in seen
    assert str(slack / ".env") not in seen
    secrets = (str(slack / "tokens.json"), str(slack / "tokens.json.bak"), str(slack / ".env"))
    assert not any(host in secrets for host, _, _ in binds)
    assert instance.slack_int_views(c, binds) == [str(slack)]


def test_no_covering_mount_and_no_workspace_mounts_nothing(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace)
    instance.write_slack_tokens(c, root)
    binds = instance.mounts(c, config_path, root)
    assert not any(str(slack) in inside for _, inside, _ in binds)
    assert instance.slack_int_views(c, binds) == []


def test_missing_workspace_token_is_fatal(host):
    slack, root, workspace, _ = host
    c = config(slack, workspace, slack={"workspace": "cobalt"})
    with pytest.raises(SystemExit, match="no token for workspace 'cobalt'"):
        instance.write_slack_tokens(c, root)


def test_whole_checkout_mount_beside_a_workspace_is_refused(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace, slack={"workspace": "apexco"})
    c["container"]["mounts"] = [str(slack)]
    instance.write_slack_tokens(c, root)
    with pytest.raises(SystemExit, match="mount a parent"):
        instance.mounts(c, config_path, root)


def test_symlinked_parent_mount_gets_a_view_too(host, tmp_path):
    slack, root, workspace, config_path = host
    alias = tmp_path / "alias"
    alias.symlink_to(slack.parent)
    c = config(slack, workspace)
    c["container"]["mounts"] = [str(alias)]
    instance.write_slack_tokens(c, root)
    binds = instance.mounts(c, config_path, root)
    view = str(alias / "slack_int")
    assert instance.slack_int_views(c, binds) == [view]
    assert (str(root / "slack" / "tokens.json"), f"{view}/tokens.json", True) in binds
    assert f"{view}/tokens.json.bak" not in {inside for _, inside, _ in binds}


def test_aws_dir_is_mounted_at_home_aws_only_when_present(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace)
    aws_home = str(instance.HOME / ".aws")
    assert not any(inside == aws_home for _, inside, _ in instance.mounts(c, config_path, root))
    (root / "aws").mkdir()
    assert (str(root / "aws"), aws_home, False) in instance.mounts(c, config_path, root)


def test_secrets_dir_is_created_and_mounted_read_only(host):
    slack, root, workspace, config_path = host
    c = config(slack, workspace)
    assert not (root / "secrets").exists()
    binds = instance.mounts(c, config_path, root)
    assert (str(root / "secrets"), str(instance.HOME / ".secrets"), True) in binds
    assert (root / "secrets").stat().st_mode & 0o777 == 0o700


def test_claude_config_dir_is_mounted_and_named_for_the_aliases(host, tmp_path):
    slack, root, workspace, config_path = host
    account = tmp_path / ".team-claude"
    account.mkdir()
    c = config(slack, workspace, llm={"claude": {"config_dir": str(account)}})
    assert instance.claude_config_dir(c) == account
    assert (str(account), str(account), False) in instance.mounts(c, config_path, root)
    assert instance.claude_config_dir(config(slack, workspace)) is None


def test_opencode_binary_and_state_are_mounted_only_when_installed(host, tmp_path, monkeypatch):
    slack, root, workspace, config_path = host
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(instance, "HOME", home)
    c = config(slack, workspace)
    assert not any(inside == "/usr/local/bin/opencode"
                   for _, inside, _ in instance.mounts(c, config_path, root))
    binary = home / ".opencode" / "bin" / "opencode"
    binary.parent.mkdir(parents=True)
    binary.write_text("")
    (home / ".config" / "opencode").mkdir(parents=True)
    (home / ".local" / "share" / "opencode").mkdir(parents=True)
    binds = instance.mounts(c, config_path, root)
    assert (str(binary), "/usr/local/bin/opencode", True) in binds
    for name in (".config/opencode", ".local/share/opencode"):
        assert (str(home / name), str(home / name), False) in binds


def test_every_listed_workspace_mounts_its_capture(host):
    slack, root, workspace, config_path = host
    (slack / "messages" / "quartzmeetings").mkdir()
    c = config(slack, workspace, features={"slack": True},
               slack={"workspaces": [
                   {"workspace": "apexco", "messages_dir": str(slack / "messages" / "apexco")},
                   {"workspace": "quartzmeetings", "messages_dir": str(slack / "messages" / "quartzmeetings")}]})
    c["container"]["mounts"] = [str(slack.parent)]
    instance.write_slack_tokens(c, root)
    binds = instance.mounts(c, config_path, root)
    for name in ("apexco", "quartzmeetings"):
        folder = str(slack / "messages" / name)
        assert (folder, folder, False) in binds
    assert json.loads((root / "slack" / "tokens.json").read_text()) == {}
