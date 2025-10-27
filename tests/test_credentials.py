from __future__ import annotations

import base64
import os

from pathlib import Path
from unittest.mock import patch

import pytest # noqa

from basepak.credentials import Credentials, load_from_dotenv


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()

def _write_env(p: Path, mapping: dict[str, str | None]) -> None:
    p.write_text("".join(f"{k}={v}\n" if v is not None else f"{k}\n" for k, v in mapping.items()),encoding="utf-8")

@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    load_from_dotenv.cache_clear()
    monkeypatch.setattr(Credentials, "_credentials", {}, raising=False)
    monkeypatch.delenv("BASEPAK_DOTENV_PATH", raising=False)
    yield
    load_from_dotenv.cache_clear()

def test_credentials_is_singleton():
    assert Credentials() is Credentials()

def test_set_with_spec(tmp_path: Path):
    env_p = tmp_path / ".env.spec"
    Credentials.set(spec={"USER1": {"USERNAME": "user1", "PASSWORD": "pass1"}}, dotenv_path=env_p)
    c = Credentials.get()
    assert c["USER1"]["USERNAME"] == "user1" and c["USER1"]["PASSWORD"] == "pass1"

def test_set_with_auths(tmp_path: Path):
    env_p = tmp_path / ".env.auths"
    Credentials.set(auths={"USER2": "user2:pass2"}, dotenv_path=env_p)
    c = Credentials.get()
    assert c["USER2"]["USERNAME"] == "user2" and c["USER2"]["PASSWORD"] == "pass2"

def test_set_with_invalid_auths(tmp_path: Path):
    with pytest.raises(Exception):
        Credentials.set(auths={"USER_INVALID": "invalid_format"}, dotenv_path=tmp_path / ".env.bad")

@patch("basepak.credentials.load_from_dotenv")
def test_set_with_dotenv(mock_load_from_dotenv):
    mock_load_from_dotenv.return_value = {"USER3": "user3:pass3"}
    with patch.dict(os.environ, {"BASEPAK_DOTENV_PATH": "/x/.env"}):
        Credentials.set()
    c = Credentials.get()
    assert c["USER3"]["USERNAME"] == "user3" and c["USER3"]["PASSWORD"] == "pass3"

def test_get_with_user_mask():
    Credentials._credentials = {"USER1": {"USERNAME": "user1", "PASSWORD": "pass1"}, "USER2": {"USERNAME": "user2", "PASSWORD": "pass2"}}
    masked = Credentials.get(user_mask="USER1")
    assert masked["USERNAME"] == "user1" and masked["PASSWORD"] == "pass1"

@patch("basepak.execute.Executable")
def test_set_from_k8s(mock_exec):
    mock_exec.return_value.run.return_value.stdout =\
        """{"items":[{"metadata":{"name":"secret.user4"},"data":{"USERNAME":"dXNlcjQ=","PASSWORD":"cGFzczQ="}}]}""" # noqa
    Credentials.set_from_k8s()
    c = Credentials.get()
    assert c["USER4"]["USERNAME"] == "user4" and c["USER4"]["PASSWORD"] == "pass4"

@pytest.mark.parametrize("spec", [
    {"USER_MISSING_USERNAME": {"PASSWORD": "pass"}},
    {"USER_MISSING_PASSWORD": {"USERNAME": "user"}},
])
def test_set_missing_fields(spec):
    with pytest.raises(Exception):
        Credentials.set(spec=spec)

@pytest.mark.parametrize(
    "env_map, decode_values, expected",
    [
        ({"USER_A": _b64("userA:passA"), "USER_B": _b64("userB:passB")}, "base64", {"USER_A": "userA:passA", "USER_B": "userB:passB"}),
        ({"USER_RAW": _b64("user:pass")}, "", {"USER_RAW": _b64("user:pass")}),
        ({"KEY_EMPTY": "", "KEY_NOVAL": None}, "base64", {"KEY_EMPTY": None, "KEY_NOVAL": None}),
        ({"USER_NL": _b64("user:pass\n")}, "base64", {"USER_NL": "user:pass\n"}),
    ],
)
def test_load_from_dotenv_matrix(tmp_path: Path, env_map, decode_values, expected):
    env_p = tmp_path / ".env.matrix"
    _write_env(env_p, env_map)
    assert load_from_dotenv(str(env_p), decode_values=decode_values) == expected

def test_load_from_dotenv_invalid_base64_raises(tmp_path: Path):
    env_p = tmp_path / ".env.invalid"
    _write_env(env_p, {"BAD": "***not-base64***"})
    with pytest.raises(Exception):
        load_from_dotenv(str(env_p), decode_values="base64")

def test_load_from_dotenv_cache_behavior(tmp_path: Path):
    env_p = tmp_path / ".env.cache"
    _write_env(env_p, {"USER_X": _b64("userX:passX")})
    assert load_from_dotenv(str(env_p), "base64")["USER_X"] == "userX:passX"
    _write_env(env_p, {"USER_X": _b64("userX:CHANGED")})
    # same path + cached -> old value
    assert load_from_dotenv(str(env_p), "base64")["USER_X"] == "userX:passX"
    load_from_dotenv.cache_clear()
    assert load_from_dotenv(str(env_p), "base64")["USER_X"] == "userX:CHANGED"

def test_credentials_set_with_real_dotenv_base64(tmp_path: Path):
    env_p = tmp_path / ".env.real"
    _write_env(env_p, {"USER3": _b64("user3:pass3")})
    os.environ["BASEPAK_DOTENV_PATH"] = str(env_p)
    Credentials.set()
    c = Credentials.get()
    assert c["USER3"]["USERNAME"] == "user3" and c["USER3"]["PASSWORD"] == "pass3"
