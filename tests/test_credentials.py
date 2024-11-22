import os
import tempfile
from unittest.mock import patch

import pytest

from basepak.credentials import Credentials

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
