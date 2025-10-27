import os
import tempfile
from pathlib import Path
from typing import Dict, Optional
from unittest.mock import patch

import pytest

from basepak.credentials import Credentials, load_from_dotenv
import base64


dotenv_path = tempfile.mktemp()

def test_credentials_singleton():
    cred1 = Credentials()
    cred2 = Credentials()
    assert cred1 is cred2

def test_set_with_spec():
    Credentials._credentials = {}
    spec = {
        'USER1': {
            'USERNAME': 'user1',
            'PASSWORD': 'pass1',
        }
    }
    Credentials.set(spec=spec, dotenv_path=dotenv_path)
    creds = Credentials.get()
    assert creds['USER1']['USERNAME'] == 'user1'
    assert creds['USER1']['PASSWORD'] == 'pass1'

def test_set_with_auths():
    Credentials._credentials = {}
    auths = {'USER2': 'user2:pass2'}
    Credentials.set(auths=auths, dotenv_path=dotenv_path)
    creds = Credentials.get()
    assert creds['USER2']['USERNAME'] == 'user2'
    assert creds['USER2']['PASSWORD'] == 'pass2'

def test_set_with_invalid_auths():
    Credentials._credentials = {}
    auths = {'USER_INVALID': 'invalid_format'}
    with pytest.raises(Exception):
        Credentials.set(auths=auths, dotenv_path=dotenv_path)

@patch('basepak.credentials.load_from_dotenv')
def test_set_with_dotenv(mock_load_from_dotenv):
    Credentials._credentials = {}
    mock_load_from_dotenv.return_value = {'USER3': 'user3:pass3'}
    with patch.dict(os.environ, {'BASEPAK_DOTENV_PATH': '/path/to/.env'}):
        Credentials.set()
    creds = Credentials.get()
    assert creds['USER3']['USERNAME'] == 'user3'
    assert creds['USER3']['PASSWORD'] == 'pass3'

def test_get_with_user_mask():
    Credentials._credentials = {
        'USER1': {'USERNAME': 'user1', 'PASSWORD': 'pass1'},
        'USER2': {'USERNAME': 'user2', 'PASSWORD': 'pass2'},
    }
    creds = Credentials.get(user_mask='USER1')
    assert creds['USERNAME'] == 'user1'
    assert creds['PASSWORD'] == 'pass1'

@patch('basepak.execute.Executable')
def test_set_from_k8s(mock_executable):
    mock_run = mock_executable.return_value.run
    mock_run.return_value.stdout = '''
    {
      "items": [
        {
          "metadata": {"name": "secret.user4"},
          "data": {
            "USERNAME": "dXNlcjQ=",
            "PASSWORD": "cGFzczQ="
          }
        }
      ]
    }
    '''
    Credentials._credentials = {}
    Credentials.set_from_k8s()
    creds = Credentials.get()
    assert creds['USER4']['USERNAME'] == 'user4'
    assert creds['USER4']['PASSWORD'] == 'pass4'

def test_set_missing_username():
    Credentials._credentials = {}
    spec = {'USER_MISSING_USERNAME': {'PASSWORD': 'pass'}}
    with pytest.raises(Exception):
        Credentials.set(spec=spec)

def test_set_missing_password_and_auth_key():
    Credentials._credentials = {}
    spec = {'USER_MISSING_PASSWORD': {'USERNAME': 'user'}}
    with pytest.raises(Exception):
        Credentials.set(spec=spec)


def b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


@pytest.fixture
def env_path(tmp_path: Path) -> Path:
    return tmp_path / ".env"


def write_env(envp: Path, mapping: Dict[str, Optional[str]]) -> None:
    """
    Write a .env from a dict:
      - value is None  -> write a bare KEY line (no '=')
      - value is ''    -> write 'KEY='
      - otherwise      -> write 'KEY=value'
    """
    lines = []
    for k, v in mapping.items():
        if v is None:
            lines.append(f"{k}\n")
        else:
            lines.append(f"{k}={v}\n")
    envp.write_text("".join(lines), encoding="utf-8")


@pytest.fixture(autouse=True)
def _clear_loader_cache():
    """Ensure cache doesn't leak between tests."""
    load_from_dotenv.cache_clear()
    yield
    load_from_dotenv.cache_clear()


@pytest.fixture
def reset_credentials():
    """Reset the singleton storage for clean tests."""
    Credentials._credentials = {}
    yield
    Credentials._credentials = {}


# --------------------------- Parametrized cases ---------------------------

@pytest.mark.parametrize(
    "env_map, decode_values, expected",
    [
        pytest.param( # base64 decode happy path
            {"USER_A": b64("userA:passA"), "USER_B": b64("userB:passB")},
            "base64",
            {"USER_A": "userA:passA", "USER_B": "userB:passB"},
            id="base64_ok",
        ),
        pytest.param( # no decoding -> raw base64 returned
            {"USER_RAW": b64("user:pass")},
            "",
            {"USER_RAW": b64("user:pass")},
            id="plain_no_decode",
        ),
        pytest.param( # empty and no-value keys
            {"KEY_EMPTY": "", "KEY_NOVAL": None},
            "base64",
            {"KEY_EMPTY": None, "KEY_NOVAL": None},  # your function maps falsy to None
            id="empty_and_no_value_lines",
        ),
        pytest.param( # trailing newline encoded by macOS `echo | base64`
            {"USER_NL": b64("user:pass\n")},
            "base64",
            {"USER_NL": "user:pass\n"},  # preserved by current implementation
            id="trailing_newline_preserved",
        ),
    ],
)
def test_load_from_dotenv_matrix(env_path: Path, env_map, decode_values, expected):
    write_env(env_path, env_map)
    out = load_from_dotenv(str(env_path), decode_values=decode_values)
    assert out == expected


# --------------------------- Focused edge cases ---------------------------

def test_load_from_dotenv_invalid_base64_raises(env_path: Path):
    write_env(env_path, {"BAD": "***not-base64***"})
    with pytest.raises(Exception):
        load_from_dotenv(str(env_path), decode_values="base64")


def test_load_from_dotenv_cache_behavior(env_path: Path, monkeypatch):
    # initial
    write_env(env_path, {"USER_X": b64("userX:passX")})
    first = load_from_dotenv(str(env_path), decode_values="base64")
    assert first["USER_X"] == "userX:passX"

    # change file; cached result should remain
    write_env(env_path, {"USER_X": b64("userX:CHANGED")})
    cached = load_from_dotenv(str(env_path), decode_values="base64")
    assert cached["USER_X"] == "userX:passX"

    # clear cache -> now we observe the change
    load_from_dotenv.cache_clear()
    refreshed = load_from_dotenv(str(env_path), decode_values="base64")
    assert refreshed["USER_X"] == "userX:CHANGED"


def test_credentials_set_with_real_dotenv_base64(env_path: Path, monkeypatch, reset_credentials):
    # End-to-end (no mocks): .env with base64 values -> Credentials.set() -> dict
    write_env(env_path, {"USER3": b64("user3:pass3")})
    monkeypatch.setenv("BASEPAK_DOTENV_PATH", str(env_path))

    Credentials.set()
    creds = Credentials.get()
    assert creds["USER3"]["USERNAME"] == "user3"
    assert creds["USER3"]["PASSWORD"] == "pass3"
