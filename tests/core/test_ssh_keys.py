import importlib.util
import json
import os
import signal
import sqlite3
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

    def test_no_configured_repository_verifies_nothing(self, tmp_path):
        assert ssh_keys.verify_git(self._config(tmp_path, [])) == []

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
        assert stat.S_IMODE((home / ".claude.json").stat().st_mode) == 0o600
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
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setattr(boot, "supervise", lambda *a: pytest.fail("app must not start"))
        assert boot.main([str(config)]) == 1

    def test_mounted_repos_owned_by_another_uid_are_trusted(self, tmp_path, monkeypatch):
        boot = _load_script("container_boot")
        home, seed, repo = tmp_path / "home", tmp_path / "seed", tmp_path / "repo"
        home.mkdir()
        seed.mkdir()
        (seed / ".gitconfig").write_text("[safe]\n\tdirectory = /a\n\tdirectory = /b\n")
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        config = tmp_path / "c.toml"
        config.write_text('[job]\nkey = "x"\nplatform = "github"\nport = 1\n'
                          '[workspace]\nroot = "%s"\nrepos = []\n' % tmp_path)
        monkeypatch.setattr(boot, "SEED_DIR", seed)
        monkeypatch.setattr(boot.Path, "home", lambda: home)
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.delenv("GIT_CONFIG_GLOBAL", raising=False)
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setattr(boot, "supervise", lambda *a: pytest.fail("app must not start"))
        boot.main([str(config)])
        env = {**os.environ, "GIT_TEST_ASSUME_DIFFERENT_OWNER": "1"}
        r = subprocess.run(["git", "-C", str(repo), "status"], capture_output=True, text=True, env=env)
        assert r.returncode == 0, r.stderr


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


class TestSuperviseStop:
    def test_sigterm_after_a_pending_sighup_ends_the_container(self, tmp_path):
        boot = _load_script("container_boot")
        runs = tmp_path / "runs"
        script = ("import os, pathlib, time\n"
                  f"p = pathlib.Path({str(runs)!r})\n"
                  "p.write_text((p.read_text() if p.exists() else '') + 'x')\n"
                  "import signal\n"
                  "signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGTERM])\n"
                  "os.kill(os.getppid(), 1)\n"
                  "os.kill(os.getppid(), 15)\n"
                  "time.sleep(0.5)\n"
                  "signal.pthread_sigmask(signal.SIG_UNBLOCK, [signal.SIGTERM])\n"
                  "time.sleep(30)\n")
        saved = {sig: signal.getsignal(sig) for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)}
        try:
            assert boot.supervise([sys.executable, "-c", script]) == -signal.SIGTERM
        finally:
            for sig, handler in saved.items():
                signal.signal(sig, handler)
        assert runs.read_text() == "x"


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
        assert f"GVOICE_PROFILE_DIR={state / 'gvoice'}" in args
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

    def test_tmp_is_a_capped_executable_tmpfs(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        for check in (False, True):
            args = mod.run_args(mod.load(str(path)), path, check=check)
            mounts = [args[i + 1] for i, a in enumerate(args) if a == "--tmpfs"]
            assert mounts == ["/tmp:rw,exec,nosuid,nodev,size=8g,mode=1777"]

    def test_gvoice_login_shares_the_instance_profile(self, tmp_path, monkeypatch):
        mod, home = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        config = mod.load(str(path))
        run = mod.run_args(config, path, check=False)
        xauth = home / ".Xauthority"
        xauth.write_text("")
        sockets = tmp_path / "x11"
        sockets.mkdir()
        monkeypatch.setattr(mod, "X11_SOCKETS", sockets)
        monkeypatch.setenv("DISPLAY", ":0")
        monkeypatch.delenv("XAUTHORITY", raising=False)
        args = mod.gvoice_login_args(config)
        state = tmp_path / "boxes" / "aimyable" / "state"
        assert f"GVOICE_PROFILE_DIR={state / 'gvoice'}" in run
        assert f"GVOICE_PROFILE_DIR={state / 'gvoice'}" in args
        assert "GVOICE_CHROME_CHANNEL=chrome" in args
        volumes = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
        assert f"{state / 'gvoice'}:{state / 'gvoice'}" in volumes
        assert (state / "gvoice").is_dir()
        assert f"{xauth}:/run/frshty/xauthority:ro" in volumes
        assert "DISPLAY=:0" in args
        assert args[args.index("--entrypoint") + 1:][:2] == ["node", mod.IMAGE]
        assert args[args.index("-w") + 1] == mod.GVOICE_DIR
        script = args[-1]
        assert 'ignoreDefaultArgs: ["--enable-automation"]' in script
        assert "--disable-blink-features=AutomationControlled" in script
        assert mod.GVOICE_URL in script

    def test_gvoice_login_signs_in_the_configured_profile(self, tmp_path, monkeypatch):
        mod, home = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        config = mod.load(str(path))
        mod.run_args(config, path, check=False)
        (home / ".Xauthority").write_text("")
        sockets = tmp_path / "x11"
        sockets.mkdir()
        monkeypatch.setattr(mod, "X11_SOCKETS", sockets)
        monkeypatch.setenv("DISPLAY", ":0")
        monkeypatch.delenv("XAUTHORITY", raising=False)
        profile = tmp_path / "boxes" / "aimyable" / "state" / "gvoice-profile"
        config["direct_inbox"] = {"gvoice_profile_dir": str(profile),
                                  "gvoice_chrome_channel": "chrome-beta"}
        args = mod.gvoice_login_args(config)
        volumes = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
        assert f"GVOICE_PROFILE_DIR={profile}" in args
        assert "GVOICE_CHROME_CHANNEL=chrome-beta" in args
        assert f"{profile}:{profile}" in volumes
        config["direct_inbox"] = {"gvoice_profile_dir": str(tmp_path / "elsewhere")}
        with pytest.raises(SystemExit, match="outside"):
            mod.gvoice_login_args(config)

    def test_gvoice_login_needs_a_display(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        path = self._config(tmp_path)
        config = mod.load(str(path))
        mod.run_args(config, path, check=False)
        monkeypatch.delenv("DISPLAY", raising=False)
        with pytest.raises(SystemExit, match="DISPLAY"):
            mod.gvoice_login_args(config)

    def test_reload_never_uses_docker_kill(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        calls = []
        pids = iter(["10", "20"])
        monkeypatch.setattr(mod, "instance_containers", lambda: ["frshty-aimyable"])
        monkeypatch.setattr(mod, "app_pid", lambda name: next(pids))

        def run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(mod.subprocess, "run", run)
        assert mod.reload(timeout=1) == 0
        assert ["docker", "exec", "frshty-aimyable", "kill", "-HUP", "1"] in calls
        assert not any(cmd[:2] == ["docker", "kill"] for cmd in calls)

    def test_macos_puts_the_database_on_a_volume(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        monkeypatch.setattr(mod.platform, "system", lambda: "Darwin")
        path = self._config(tmp_path)
        args = mod.run_args(mod.load(str(path)), path, check=False)
        volumes = [args[i + 1] for i, a in enumerate(args) if a == "-v"]
        assert "frshty-aimyable-db:/var/lib/frshty" in volumes
        assert "FRSHTY_DB=/var/lib/frshty/frshty.db" in args

    def test_linux_keeps_the_database_in_state(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        monkeypatch.setattr(mod.platform, "system", lambda: "Linux")
        path = self._config(tmp_path)
        args = mod.run_args(mod.load(str(path)), path, check=False)
        state = tmp_path / "boxes" / "aimyable" / "state"
        assert f"FRSHTY_DB={state / 'frshty.db'}" in args
        assert not any(a.endswith(":/var/lib/frshty") for a in args)

    def test_check_before_the_move_reads_the_state_database(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        monkeypatch.setattr(mod.platform, "system", lambda: "Darwin")
        state = tmp_path / "boxes" / "aimyable" / "state"
        state.mkdir(parents=True)
        (state / "frshty.db").write_bytes(b"")
        path = self._config(tmp_path)
        args = mod.run_args(mod.load(str(path)), path, check=True)
        assert f"FRSHTY_DB={state / 'frshty.db'}" in args
        assert not any(a.endswith(":/var/lib/frshty") for a in args)

    def _fake_docker(self, mod, monkeypatch, volume_dir, state):
        calls = []
        real_run = subprocess.run

        def run(cmd, **kwargs):
            calls.append(cmd)
            if "-c" in cmd:
                i = cmd.index("-c")
                src = cmd[i + 2].replace("/src", str(state))
                dst = cmd[i + 3].replace(mod.DB_DIR, str(volume_dir))
                return real_run([sys.executable, "-c", cmd[i + 1], src, dst],
                                capture_output=True, text=True)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(mod.subprocess, "run", run)
        return calls

    def test_up_moves_the_state_database_into_the_volume(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        monkeypatch.setattr(mod.platform, "system", lambda: "Darwin")
        root = tmp_path / "boxes" / "aimyable"
        (root / "state").mkdir(parents=True)
        conn = sqlite3.connect(root / "state" / "frshty.db")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t (v TEXT)")
        conn.execute("INSERT INTO t VALUES ('kept')")
        conn.commit()
        volume_dir = tmp_path / "volume"
        volume_dir.mkdir()
        calls = self._fake_docker(mod, monkeypatch, volume_dir, root / "state")
        config = mod.load(str(self._config(tmp_path)))
        try:
            mod.prepare_db_volume(config, root, move=True)
        finally:
            conn.close()
        assert ["docker", "volume", "create", "frshty-aimyable-db"] in calls
        moved = sqlite3.connect(volume_dir / "frshty.db")
        assert moved.execute("SELECT v FROM t").fetchall() == [("kept",)]
        moved.close()
        assert not (root / "state" / "frshty.db").exists()
        assert (root / "state" / "frshty.db.pre-volume").exists()

    def test_up_refuses_when_the_volume_already_holds_a_database(self, tmp_path, monkeypatch):
        mod, _ = self._launcher(tmp_path, monkeypatch)
        monkeypatch.setattr(mod.platform, "system", lambda: "Darwin")
        root = tmp_path / "boxes" / "aimyable"
        (root / "state").mkdir(parents=True)
        sqlite3.connect(root / "state" / "frshty.db").close()
        volume_dir = tmp_path / "volume"
        volume_dir.mkdir()
        (volume_dir / "frshty.db").write_bytes(b"other")
        self._fake_docker(mod, monkeypatch, volume_dir, root / "state")
        config = mod.load(str(self._config(tmp_path)))
        with pytest.raises(SystemExit, match="already exists"):
            mod.prepare_db_volume(config, root, move=True)
        assert (root / "state" / "frshty.db").exists()
        assert (volume_dir / "frshty.db").read_bytes() == b"other"

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
        assert args[args.index("--device") + 2:args.index("--device") + 4] == ["--group-add", str(sock.stat().st_gid)]
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
