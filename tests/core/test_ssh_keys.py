import importlib.util
import json
import os
import signal
import stat
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

import core.ssh_keys as ssh_keys

REPO = Path(__file__).resolve().parent.parent.parent


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_under_test", REPO / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _client(handler, base_url):
    return httpx.Client(base_url=base_url, transport=httpx.MockTransport(handler))


class TestEnsureKey:
    def test_generates_once_and_reuses(self, tmp_path):
        ssh_dir = tmp_path / "ssh"
        first = ssh_keys.ensure_key(ssh_dir, "frshty")
        second = ssh_keys.ensure_key(ssh_dir, "frshty")
        assert first.startswith("ssh-ed25519 ")
        assert first == second
        assert stat.S_IMODE((ssh_dir / "id_ed25519").stat().st_mode) == 0o600
        assert stat.S_IMODE(ssh_dir.stat().st_mode) == 0o700

    def test_half_a_pair_is_refused(self, tmp_path):
        ssh_dir = tmp_path / "ssh"
        ssh_keys.ensure_key(ssh_dir, "frshty")
        (ssh_dir / "id_ed25519.pub").unlink()
        with pytest.raises(ssh_keys.KeyBootstrapError, match="one half"):
            ssh_keys.ensure_key(ssh_dir, "frshty")
        assert (ssh_dir / "id_ed25519").exists()

    def test_ssh_config_routes_aliases_to_the_instance_key(self, tmp_path):
        ssh_keys.write_ssh_config(tmp_path)
        text = (tmp_path / "config").read_text()
        assert "Host github.com github-*" in text
        assert "Host bitbucket.org bitbucket-*" in text
        assert "IdentityFile ~/.ssh/id_ed25519" in text
        assert "/tmp/.ssh-host" not in text


class TestKeyBody:
    def test_comment_is_ignored(self):
        assert ssh_keys.key_body("ssh-ed25519 AAAA frshty-a") == ssh_keys.key_body("ssh-ed25519 AAAA other")


PUB = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKEY frshty-frshty"


class TestGithub:
    def _handler(self, calls, login="tipu", keys=(), post_status=201):
        def handler(request):
            calls.append((request.method, request.url.path, request.content))
            if request.url.path == "/user":
                return httpx.Response(200, json={"login": login})
            if request.method == "GET":
                return httpx.Response(200, json=[{"key": k} for k in keys])
            return httpx.Response(post_status, json={"message": "nope"})
        return handler

    def test_adds_a_missing_key(self):
        calls = []
        client = _client(self._handler(calls), ssh_keys.GITHUB_API)
        assert ssh_keys.register_github("t", "tipu", "frshty-frshty", PUB, client) == "created"
        posts = [c for c in calls if c[0] == "POST"]
        assert len(posts) == 1
        assert json.loads(posts[0][2]) == {"title": "frshty-frshty", "key": PUB}

    def test_existing_key_adds_nothing(self):
        calls = []
        client = _client(self._handler(calls, keys=["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKEY"]),
                         ssh_keys.GITHUB_API)
        assert ssh_keys.register_github("t", "tipu", "frshty-frshty", PUB, client) == "existing"
        assert not [c for c in calls if c[0] == "POST"]

    def test_wrong_account_is_refused(self):
        calls = []
        client = _client(self._handler(calls, login="someone"), ssh_keys.GITHUB_API)
        with pytest.raises(ssh_keys.KeyBootstrapError, match="belongs to 'someone'"):
            ssh_keys.register_github("t", "tipu", "frshty-frshty", PUB, client)
        assert not [c for c in calls if c[0] == "POST"]

    def test_rejected_add_is_fatal(self):
        client = _client(self._handler([], post_status=403), ssh_keys.GITHUB_API)
        with pytest.raises(ssh_keys.KeyBootstrapError, match="HTTP 403"):
            ssh_keys.register_github("t", "tipu", "frshty-frshty", PUB, client)

    def test_missing_key_scope_names_the_fix(self):
        def handler(request):
            if request.url.path == "/user":
                return httpx.Response(200, json={"login": "q"})
            return httpx.Response(404, json={"message": "Not Found"})

        with pytest.raises(ssh_keys.KeyBootstrapError, match="admin:public_key"):
            ssh_keys.register_github("t", "q", "frshty-quill", PUB, _client(handler, ssh_keys.GITHUB_API))

    def test_missing_token_is_fatal(self):
        with pytest.raises(ssh_keys.KeyBootstrapError, match="GH_TOKEN"):
            ssh_keys.register_github("", "tipu", "frshty-frshty", PUB)


class TestBitbucket:
    def test_follows_pages_before_adding(self):
        calls = []

        def handler(request):
            calls.append((request.method, str(request.url)))
            if request.method == "GET" and "page=2" not in str(request.url):
                return httpx.Response(200, json={
                    "values": [{"key": "ssh-ed25519 OTHER"}],
                    "next": f"{ssh_keys.BITBUCKET_API}/users/u1/ssh-keys?page=2"})
            if request.method == "GET":
                return httpx.Response(200, json={"values": [{"key": "ssh-rsa ALSOOTHER"}]})
            return httpx.Response(201, json={})

        client = _client(handler, ssh_keys.BITBUCKET_API)
        assert ssh_keys.register_bitbucket("me", "tok", "u1", "frshty-aimyable", PUB, client) == "created"
        assert [c[0] for c in calls] == ["GET", "GET", "POST"]

    def test_existing_key_on_second_page(self):
        def handler(request):
            if "page=2" in str(request.url):
                return httpx.Response(200, json={"values": [{"key": PUB}]})
            if request.method == "GET":
                return httpx.Response(200, json={
                    "values": [], "next": f"{ssh_keys.BITBUCKET_API}/users/u1/ssh-keys?page=2"})
            raise AssertionError("must not add an existing key")

        client = _client(handler, ssh_keys.BITBUCKET_API)
        assert ssh_keys.register_bitbucket("me", "tok", "u1", "l", PUB, client) == "existing"

    def test_missing_credentials_are_fatal(self):
        with pytest.raises(ssh_keys.KeyBootstrapError, match="user_account_id"):
            ssh_keys.register_bitbucket("me", "", "u1", "l", PUB)


def _git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


class TestVerifyGit:
    def _config(self, root, repos):
        return {"workspace": {"root": root, "repos": repos}}

    def test_checks_every_repo_with_an_origin(self, tmp_path):
        bare = tmp_path / "bare.git"
        _git("init", "--bare", str(bare), cwd=tmp_path)
        for name in ("a", "b"):
            (tmp_path / name).mkdir()
            _git("init", cwd=tmp_path / name)
        _git("remote", "add", "origin", str(bare), cwd=tmp_path / "a")
        assert ssh_keys.verify_git(self._config(tmp_path, ["a", "b"])) == ["a"]

    def test_unreachable_origin_is_fatal(self, tmp_path):
        (tmp_path / "a").mkdir()
        _git("init", cwd=tmp_path / "a")
        _git("remote", "add", "origin", str(tmp_path / "missing.git"), cwd=tmp_path / "a")
        with pytest.raises(ssh_keys.KeyBootstrapError, match="ls-remote"):
            ssh_keys.verify_git(self._config(tmp_path, ["a"]))

    def test_unmounted_workspace_is_fatal(self, tmp_path):
        with pytest.raises(ssh_keys.KeyBootstrapError, match="not a git repository"):
            ssh_keys.verify_git(self._config(tmp_path, ["a"]))

    def test_no_origin_anywhere_is_fatal(self, tmp_path):
        (tmp_path / "a").mkdir()
        _git("init", cwd=tmp_path / "a")
        with pytest.raises(ssh_keys.KeyBootstrapError, match="no configured repository"):
            ssh_keys.verify_git(self._config(tmp_path, ["a"]))


class TestContainerBoot:
    def test_seed_files_become_writable_copies(self, tmp_path):
        boot = _load_script("container_boot")
        seed, home = tmp_path / "seed", tmp_path / "home"
        seed.mkdir()
        home.mkdir()
        (seed / ".claude.json").write_text("{}")
        os.chmod(seed / ".claude.json", 0o444)
        assert boot.copy_seed_files(seed, home) == [".claude.json"]
        (home / ".claude.json").write_text('{"a": 1}')
        assert (seed / ".claude.json").read_text() == "{}"

    def test_missing_seed_dir_copies_nothing(self, tmp_path):
        boot = _load_script("container_boot")
        assert boot.copy_seed_files(tmp_path / "none", tmp_path) == []

    def test_key_failure_exits_nonzero_before_the_app(self, tmp_path, monkeypatch):
        boot = _load_script("container_boot")
        config = tmp_path / "c.toml"
        config.write_text('[job]\nkey = "x"\nplatform = "github"\nport = 1\n'
                          '[workspace]\nroot = "%s"\nrepos = []\n' % tmp_path)
        monkeypatch.setattr(boot.Path, "home", lambda: tmp_path)
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setattr(boot, "supervise", lambda *a: pytest.fail("app must not start"))
        assert boot.main([str(config)]) == 1


class TestSupervise:
    def test_sighup_restarts_the_child_and_its_exit_ends_the_loop(self, tmp_path):
        boot = _load_script("container_boot")
        runs = tmp_path / "runs"
        script = ("import os, pathlib, sys, time\n"
                  f"p = pathlib.Path({str(runs)!r})\n"
                  "n = len(p.read_text()) if p.exists() else 0\n"
                  "p.write_text('x' * (n + 1))\n"
                  "if n == 0:\n"
                  "    os.kill(os.getppid(), 1)\n"
                  "    time.sleep(30)\n"
                  "sys.exit(7)\n")
        saved = {sig: signal.getsignal(sig) for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)}
        try:
            assert boot.supervise([sys.executable, "-c", script]) == 7
        finally:
            for sig, handler in saved.items():
                signal.signal(sig, handler)
        assert runs.read_text() == "xx"


class TestInstanceLauncher:
    def _config(self, tmp_path, extra=""):
        ws = tmp_path / "ws"
        ws.mkdir(exist_ok=True)
        path = tmp_path / "aimyable.toml"
        path.write_text('[job]\nkey = "aimyable"\nplatform = "bitbucket"\nport = 7100\n'
                        f'[workspace]\nroot = "{ws}"\n' + extra)
        return path

    def _launcher(self, tmp_path, monkeypatch):
        mod = _load_script("instance")
        home = tmp_path / "home"
        (home / ".claude").mkdir(parents=True)
        (home / ".ssh").mkdir()
        (home / ".claude.json").write_text("{}")
        monkeypatch.setattr(mod, "HOME", home)
        monkeypatch.setattr(mod, "CONTAINERS_ROOT", tmp_path / "boxes")
        monkeypatch.setattr(mod, "PEERS", tmp_path / "boxes" / "peers.toml")
        return mod, home

    def test_host_ssh_is_never_mounted(self, tmp_path, monkeypatch):
        mod, home = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        args = mod.run_args(mod.load(str(path)), path, check=False)
        volumes = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
        sources = [v.split(":")[0] for v in volumes]
        assert str(home / ".ssh") not in sources
        assert f"{tmp_path / 'boxes' / 'aimyable' / 'ssh'}:{home / '.ssh'}" in volumes
        state = tmp_path / "boxes" / "aimyable" / "state"
        assert f"{state}:{state}" in volumes
        assert f"FRSHTY_ROOT={state}" in args
        assert str(home / ".frshty") not in sources
        assert f"{tmp_path / 'ws'}:{tmp_path / 'ws'}" in volumes
        assert f"{home / '.claude.json'}:/run/frshty/seed/.claude.json:ro" in volumes
        assert stat.S_IMODE((tmp_path / "boxes" / "aimyable" / "env").stat().st_mode) == 0o600

    def test_container_port_overrides_job_port(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path, "[container]\nport = 7132\n")
        args = mod.run_args(mod.load(str(path)), path, check=False)
        assert args[-2:] == ["--port", "7132"]
        assert "FRSHTY_BOARD_INSTANCE=aimyable" in args
        assert "FRSHTY_PEER_SELF=aimyable" in args

    def test_check_runs_the_proof_only(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        args = mod.run_args(mod.load(str(path)), path, check=True)
        assert args[-1] == "--check"
        assert "--rm" in args and "--restart" not in args

    def test_config_and_peers_mount_from_the_stable_root(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        (tmp_path / "boxes").mkdir()
        (tmp_path / "boxes" / "peers.toml").write_text("")
        args = mod.run_args(mod.load(str(path)), path, check=False)
        volumes = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
        stable = tmp_path / "boxes" / "aimyable" / "config" / "aimyable.toml"
        assert f"{stable}:/app/config/aimyable.toml" in volumes
        assert f"{tmp_path / 'boxes' / 'peers.toml'}:/app/config/peers.toml:ro" in volumes
        assert not any(v.startswith(f"{path}:") for v in volumes)
        assert stable.read_text() == path.read_text()
        assert stat.S_IMODE(stable.stat().st_mode) == 0o600
        path.unlink()
        args = mod.run_args(mod.load(str(stable)), stable, check=False)
        assert f"{stable}:/app/config/aimyable.toml" in [args[i + 1] for i, a in enumerate(args) if a == "-v"]

    def test_an_edited_stable_config_is_never_overwritten(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        mod.run_args(mod.load(str(path)), path, check=False)
        stable = tmp_path / "boxes" / "aimyable" / "config" / "aimyable.toml"
        stable.write_text(stable.read_text() + "# edited in the web UI\n")
        with pytest.raises(SystemExit, match="differs"):
            mod.run_args(mod.load(str(path)), path, check=False)
        assert stable.read_text().endswith("# edited in the web UI\n")

    def test_gateway_reads_the_stable_peers_file(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        with pytest.raises(SystemExit, match="does not exist"):
            mod.gateway_args(7130)
        (tmp_path / "boxes").mkdir()
        (tmp_path / "boxes" / "peers.toml").write_text("")
        assert f"{tmp_path / 'boxes' / 'peers.toml'}:/app/config/peers.toml:ro" in mod.gateway_args(7130)

    def test_code_mounts_read_only_with_secrets_hidden(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        code = tmp_path / "checkout"
        code.mkdir()
        (code / ".env").write_text("SECRET=1\n")
        monkeypatch.setattr(mod, "code_dir", lambda: code)
        path = self._config(tmp_path)
        args = mod.run_args(mod.load(str(path)), path, check=False)
        assert f"{code}:/app:ro" in args
        assert "type=tmpfs,destination=/app/config" in args
        assert "/dev/null:/app/.env:ro" in args
        assert f"FRSHTY_CODE_DIR={code}" in args
        assert "frshty.instance=aimyable" in args

    def test_code_dir_is_the_main_checkout_not_a_worktree(self, tmp_path, monkeypatch):
        mod = _load_script("instance")
        repo = tmp_path / "main"
        repo.mkdir()
        _git("init", "-q", cwd=repo)
        _git("-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "x", cwd=repo)
        _git("worktree", "add", "-q", "-b", "w", str(tmp_path / "wt"), cwd=repo)
        monkeypatch.setattr(mod, "REPO", tmp_path / "wt")
        assert mod.code_dir() == repo

    def test_docker_socket_and_devices_pass_through(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        sock = tmp_path / "docker.sock"
        sock.write_text("")
        monkeypatch.setattr(mod, "DOCKER_SOCKET", sock)
        path = self._config(tmp_path, f'[container]\ndevices = ["{sock}"]\n')
        args = mod.run_args(mod.load(str(path)), path, check=False)
        assert f"{sock}:{sock}" in args
        assert args[args.index("--group-add") + 1] == str(sock.stat().st_gid)
        assert args[args.index("--device") + 1] == str(sock)
        (tmp_path / "boxes" / "aimyable" / "config" / "aimyable.toml").unlink()
        path = self._config(tmp_path, '[container]\ndevices = ["/definitely/not/a/device"]\n')
        with pytest.raises(SystemExit, match="device /definitely/not/a/device does not exist"):
            mod.run_args(mod.load(str(path)), path, check=False)

    def test_missing_mount_source_is_refused(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path, '[container]\nmounts = ["/definitely/not/here"]\n')
        with pytest.raises(SystemExit, match="does not exist"):
            mod.run_args(mod.load(str(path)), path, check=False)


class TestBoardInstance:
    def _read(self, env):
        code = ("import core.config, core.correspondence as c, services.work_store as s;"
                "print(core.config.BOARD_INSTANCE_KEY, c.allowed_on_disk.__defaults__[0], s.BOARD_INSTANCE_KEY)")
        out = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True,
                             text=True, env={**os.environ, **env}, check=True)
        return out.stdout.split()

    def test_defaults_to_personal(self):
        env = {k: v for k, v in os.environ.items() if k != "FRSHTY_BOARD_INSTANCE"}
        out = subprocess.run([sys.executable, "-c", "import core.config;print(core.config.BOARD_INSTANCE_KEY)"],
                             cwd=REPO, capture_output=True, text=True, env=env, check=True)
        assert out.stdout.strip() == "personal"

    def test_env_names_the_board_instance(self):
        assert self._read({"FRSHTY_BOARD_INSTANCE": "frshty"}) == ["frshty", "frshty", "frshty"]


class TestScopedPrune:
    """A container must never drop a worktree another filesystem view registered."""

    def _repo(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        _git("init", "-q", cwd=repo)
        _git("-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "x", cwd=repo)
        return repo

    def _registered(self, repo):
        out = subprocess.run(["git", "worktree", "list", "--porcelain"], cwd=repo,
                             capture_output=True, text=True, check=True).stdout
        return [l.split(" ", 1)[1] for l in out.splitlines() if l.startswith("worktree ")]

    def test_drops_only_missing_worktrees_under_an_owned_root(self, tmp_path, monkeypatch):
        import shutil
        import core.git_util as git_util
        repo = self._repo(tmp_path)
        mine, theirs = tmp_path / "mine", tmp_path / "theirs"
        monkeypatch.setenv("FRSHTY_ROOT", str(mine))
        monkeypatch.setattr(git_util, "_owned_roots", set())
        _git("worktree", "add", "-q", "-b", "a", str(mine / "w"), cwd=repo)
        _git("worktree", "add", "-q", "-b", "b", str(theirs / "w"), cwd=repo)
        shutil.rmtree(mine)
        shutil.rmtree(theirs)
        assert git_util.prune_worktrees(repo) == [str(mine / "w")]
        assert str(theirs / "w") in self._registered(repo)
        assert str(mine / "w") not in self._registered(repo)

    def test_an_owned_workspace_root_is_pruned_too(self, tmp_path, monkeypatch):
        import shutil
        import core.git_util as git_util
        repo = self._repo(tmp_path)
        monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))
        monkeypatch.setattr(git_util, "_owned_roots", set())
        git_util.own_worktree_root(tmp_path / "ws")
        _git("worktree", "add", "-q", "-b", "a", str(tmp_path / "ws" / "t"), cwd=repo)
        shutil.rmtree(tmp_path / "ws" / "t")
        assert git_util.prune_worktrees(repo) == [str(tmp_path / "ws" / "t")]

    def test_a_live_or_locked_worktree_is_kept(self, tmp_path, monkeypatch):
        import shutil
        import core.git_util as git_util
        repo = self._repo(tmp_path)
        monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))
        monkeypatch.setattr(git_util, "_owned_roots", set())
        live, locked = tmp_path / "state" / "live", tmp_path / "state" / "locked"
        _git("worktree", "add", "-q", "-b", "a", str(live), cwd=repo)
        _git("worktree", "add", "-q", "--lock", "-b", "b", str(locked), cwd=repo)
        shutil.rmtree(locked)
        assert git_util.prune_worktrees(repo) == []
        assert {str(live), str(locked)} <= set(self._registered(repo))
