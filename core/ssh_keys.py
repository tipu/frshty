"""The SSH key an instance container pushes with.

A container never sees the host's ~/.ssh. On first boot it generates its own
ed25519 key in ~/.ssh, a directory the host keeps per instance, and adds the
public half to the account the instance works as: the GitHub account behind
GH_TOKEN, or the Bitbucket user of the [bitbucket] block. A restart finds the
key on disk and on the account and adds nothing.

Every failure is fatal. An instance that starts without a registered key looks
healthy and then fails every fetch and push, which is worse than not starting.
"""
import subprocess
from pathlib import Path

import httpx

from core.config import get_repos, resolve_env
from core.git_util import run_git_status

KEY_NAME = "id_ed25519"
GITHUB_API = "https://api.github.com"
BITBUCKET_API = "https://api.bitbucket.org/2.0"

SSH_CONFIG = """Include ~/.ssh/hosts.conf

Host github.com github-*
    HostName github.com
    User git
    IdentityFile ~/.ssh/{key}
    IdentitiesOnly yes

Host bitbucket.org bitbucket-*
    HostName bitbucket.org
    User git
    IdentityFile ~/.ssh/{key}
    IdentitiesOnly yes

Host *
    StrictHostKeyChecking accept-new
    UserKnownHostsFile ~/.ssh/known_hosts
"""


class KeyBootstrapError(RuntimeError):
    pass


def key_body(public_line: str) -> str:
    """The algorithm and base64 of a public key, without its comment. The
    platforms store the comment as a title or drop it, so two copies of one
    key compare equal only on these two fields."""
    return " ".join(public_line.split()[:2])


def ensure_key(ssh_dir: Path, instance_key: str) -> str:
    """Generate the instance key when the directory holds none. Returns the
    public key line."""
    private = ssh_dir / KEY_NAME
    public = ssh_dir / f"{KEY_NAME}.pub"
    ssh_dir.mkdir(parents=True, exist_ok=True)
    ssh_dir.chmod(0o700)
    if private.exists() != public.exists():
        raise KeyBootstrapError(
            f"{ssh_dir} holds only one half of {KEY_NAME}; restore the other "
            f"half or delete both to generate a new key")
    if not private.exists():
        r = subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "",
             "-C", f"frshty-{instance_key}", "-f", str(private)],
            capture_output=True, text=True)
        if r.returncode != 0:
            raise KeyBootstrapError(f"ssh-keygen failed: {r.stderr.strip()[:300]}")
    private.chmod(0o600)
    line = public.read_text().strip()
    if len(line.split()) < 2:
        raise KeyBootstrapError(f"{public} is not an SSH public key")
    return line


def write_ssh_config(ssh_dir: Path) -> None:
    path = ssh_dir / "config"
    path.write_text(SSH_CONFIG.format(key=KEY_NAME))
    path.chmod(0o600)


def _check(resp: httpx.Response, what: str) -> None:
    if resp.status_code >= 400:
        raise KeyBootstrapError(
            f"{what} answered HTTP {resp.status_code}: {resp.text[:300]}")


def register_github(token: str, expected_login: str, title: str,
                    public_line: str, client: httpx.Client | None = None) -> str:
    """Add the key to the GitHub account of `token`. Returns "created" or
    "existing". Adding a key needs the admin:public_key scope."""
    if not token:
        raise KeyBootstrapError("GH_TOKEN is not set; the instance cannot add its SSH key to GitHub")
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28"}
    own = client is None
    client = client or httpx.Client(base_url=GITHUB_API, timeout=30)
    try:
        me = client.get("/user", headers=headers)
        _check(me, "GitHub GET /user")
        login = me.json().get("login", "")
        if expected_login and login != expected_login:
            raise KeyBootstrapError(
                f"GH_TOKEN belongs to {login!r}; the config names {expected_login!r}")
        body = key_body(public_line)
        page = 1
        while True:
            listed = client.get("/user/keys", headers=headers,
                                params={"per_page": 100, "page": page})
            if listed.status_code == 404:
                raise KeyBootstrapError(
                    f"GitHub hides /user/keys from {login!r}: the token lacks the "
                    f"admin:public_key scope. Run: gh auth refresh -h github.com "
                    f"-u {login} -s admin:public_key")
            _check(listed, "GitHub GET /user/keys")
            keys = listed.json()
            if any(key_body(k.get("key", "")) == body for k in keys):
                return "existing"
            if len(keys) < 100:
                break
            page += 1
        created = client.post("/user/keys", headers=headers,
                              json={"title": title, "key": public_line})
        _check(created, "GitHub POST /user/keys")
        return "created"
    finally:
        if own:
            client.close()


def register_bitbucket(user: str, token: str, account_id: str, label: str,
                       public_line: str, client: httpx.Client | None = None) -> str:
    """Add the key to the Bitbucket user `account_id`. Returns "created" or
    "existing"."""
    if not (user and token and account_id):
        raise KeyBootstrapError(
            "[bitbucket] needs user, token and user_account_id to add the SSH key")
    own = client is None
    client = client or httpx.Client(base_url=BITBUCKET_API, timeout=30)
    path = f"/users/{account_id}/ssh-keys"
    try:
        body = key_body(public_line)
        url = path
        params = {"pagelen": 100}
        while url:
            listed = client.get(url, auth=(user, token), params=params)
            _check(listed, f"Bitbucket GET {path}")
            data = listed.json()
            if any(key_body(k.get("key", "")) == body for k in data.get("values", [])):
                return "existing"
            url, params = data.get("next", ""), None
        created = client.post(path, auth=(user, token),
                              json={"label": label, "key": public_line})
        _check(created, f"Bitbucket POST {path}")
        return "created"
    finally:
        if own:
            client.close()


def verify_git(config: dict) -> list[str]:
    """Run git ls-remote against the origin of every configured repository.
    Returns the repository names checked."""
    checked = []
    for repo in get_repos(config):
        path = Path(repo["path"])
        if not (path / ".git").exists():
            raise KeyBootstrapError(f"{path} is not a git repository; is the workspace mounted?")
        if run_git_status(path, ["remote", "get-url", "origin"]).returncode != 0:
            continue
        r = run_git_status(path, ["ls-remote", "--heads", "origin"])
        if r.returncode != 0:
            raise KeyBootstrapError(
                f"git ls-remote origin failed in {path}: {r.stderr.strip()[:300]}")
        checked.append(repo["name"])
    if not checked:
        raise KeyBootstrapError("no configured repository has an origin remote to verify")
    return checked


def bootstrap(config: dict, ssh_dir: Path, gh_token: str) -> dict:
    """Make sure this instance can fetch and push over SSH with its own key."""
    key = config["job"]["key"]
    platform = config["job"]["platform"]
    line = ensure_key(ssh_dir, key)
    write_ssh_config(ssh_dir)
    title = f"frshty-{key}"
    if platform == "github":
        github = config.get("github") or {}
        state = register_github(gh_token, github.get("account", ""), title, line)
    elif platform == "bitbucket":
        bb = config.get("bitbucket") or {}
        state = register_bitbucket(resolve_env(config, "bitbucket", "user_env"),
                                   resolve_env(config, "bitbucket", "token_env"),
                                   bb.get("user_account_id", ""), title, line)
    else:
        raise KeyBootstrapError(f"unknown platform {platform!r}")
    fingerprint = subprocess.run(["ssh-keygen", "-l", "-f", str(ssh_dir / f"{KEY_NAME}.pub")],
                                 capture_output=True, text=True).stdout.split()
    return {"instance": key, "platform": platform, "registration": state,
            "fingerprint": fingerprint[1] if len(fingerprint) > 1 else "",
            "repos": verify_git(config)}
