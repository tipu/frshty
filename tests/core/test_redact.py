import pytest

from core.redact import PLACEHOLDER, redact_json, redact_secrets

AWS_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
GH_TOKEN = "ghp_" + "a1B2c3D4e5" * 4
GH_PAT = "github_pat_11ABCDEFG0123456789_abcdefghijklmnop"
PRIVATE_KEY = ("-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA\n"
               "-----END OPENSSH PRIVATE KEY-----")


@pytest.mark.parametrize("text, secret", [
    ("login: mtrainoul password: Hunter2!x", "Hunter2!x"),
    ("password=Hunter2!x", "Hunter2!x"),
    ("Password = 'Hunter 2'", "Hunter 2"),
    ('{"passwd": "Hunter2!x"}', "Hunter2!x"),
    ("pwd: Hunter2!x", "Hunter2!x"),
    ("client secret: s3cr3tvalue", "s3cr3tvalue"),
    ("GITHUB_TOKEN=abc123def456", "abc123def456"),
    ("api key: sk-live-0011", "sk-live-0011"),
    ("API_KEY=sk-live-0011", "sk-live-0011"),
    (f"aws_secret_access_key = {AWS_SECRET}", AWS_SECRET),
    ("id AKIAIOSFODNN7EXAMPLE in the profile", "AKIAIOSFODNN7EXAMPLE"),
    (f"use {GH_TOKEN} for the push", GH_TOKEN),
    (GH_PAT, GH_PAT),
    (f"key:\n{PRIVATE_KEY}\nend", "b3BlbnNzaC1rZXktdjEAAAAA"),
    ("https://host/cb?token=abc123&x=1", "abc123"),
    ('password: "hunter(42)"', "hunter(42)"),
    ("password: ab(c", "ab(c"),
    ("password: hunter(42)", "hunter(42)"),
])
def test_secret_values_are_replaced(text, secret):
    out = redact_secrets(text)
    assert secret not in out
    assert PLACEHOLDER in out


def test_key_and_surrounding_text_stay():
    assert (redact_secrets("login: mtrainoul password: Hunter2!x, then deploy")
            == f"login: mtrainoul password: {PLACEHOLDER}, then deploy")


@pytest.mark.parametrize("text", [
    "482913",
    "the code is 482913",
    "code: 482913",
    "password_field = form.password_field",
    "the token expired, ask for a new one",
    "rotate the API token before Friday",
    "password = getpass()",
    'pwd = os.environ.get("PWD")',
    "secretName: app-config",
    "Which password manager do you use?",
    "the AKIA prefix marks an access key id",
    "",
])
def test_ordinary_text_is_left_alone(text):
    assert redact_secrets(text) == text


def test_redaction_is_idempotent():
    once = redact_secrets("password: Hunter2!x")
    assert redact_secrets(once) == once


def test_redact_json_walks_nested_values():
    value = {"questions": [{"question": "password: Hunter2!x?", "options": [
        {"label": "token=abc123", "n": 3}]}]}
    out = redact_json(value)
    assert "Hunter2!x" not in str(out)
    assert "abc123" not in str(out)
    assert out["questions"][0]["options"][0]["n"] == 3
