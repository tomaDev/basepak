from __future__ import annotations
import base64
import functools
import json
from typing import Mapping, Optional, Dict, Iterable, Type

import click
from importlib import resources

from dotenv import dotenv_values

from . import log
from .helpers import Executable
from . import __name__ as package_name

@functools.lru_cache()
def load_from_dotenv(dotenv_path: Optional[str] = None) -> Dict[str, str]:
    return dotenv_values(str(dotenv_path or resources.files('bkp').joinpath('.env')), verbose=True)
    # todo: generalize to be used for other modules


class Credentials:
    _instance = None
    _credentials = dict()

    def __new__(cls):  # Singleton
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get(cls, user_mask: Optional[str] = None, default=None) -> Dict[str, str | dict] | None:
        """Get credentials for all users or a specific user"""
        if user_mask:
            return cls._credentials.get(user_mask, default if default else dict()).copy()
        return cls._credentials.copy()

    @classmethod
    def set_from_k8s(cls, user_mask: str = None, namespace: str = 'default-tenant', skip: Iterable = None):
        """Pull secret from k8s, and set credentials for user_mask"""
        logger = log.get_logger()
        kubectl = Executable(
            'kubectl',
            'kubectl get secrets --output json --ignore-not-found --namespace', namespace,
            '--selector created-by=bakpak',
            logger=log.get_logger(name='plain'),
        )
        secrets = kubectl.run(show_cmd_level='warning').stdout
        if not secrets:
            logger.warning(f'No secrets found in namespace {namespace}')
            return
        for item in json.loads(secrets)['items']:
            name = user_mask if user_mask else item['metadata']['name'].split('.')[-1].upper().replace('-', '_')
            if skip and name in skip:
                logger.info(f'Credentials for {name} were passed as flags. Skipping...')
                continue
            creds = {k: base64.b64decode(v).decode(errors='replace') for k, v in item['data'].items()}
            logger.info(f'Loading credentials for {name} from k8s secret: {namespace}/{item["metadata"]["name"]}')
            cls._credentials[name] = creds

    @classmethod
    def set(
            cls,
            spec: Optional[Mapping[str, Mapping[str, str]]] = None,
            auths: Optional[Mapping[str, str]] = None
    ) -> Type[Credentials]:
        """Set credentials for users in spec and args
        @param spec: dict of dicts to get credentials from a file:
            {
                USER_MASK: {
                    'USERNAME': USERNAME,
                    'PASSWORD': PASSWORD,
                    },
                ...,
            }
        @param auths: dict of strings to get credentials from command line flags:
            {
                USER_MASK: USERNAME:PASSWORD,
                ...,
            }
        """
        dotenv_configs = load_from_dotenv()
        cls._credentials.setdefault('RETHINKDB_ADMIN', {
            'USERNAME': 'admin',
            'AUTH_KEY': dotenv_configs.get('RETHINKDB_AUTH_KEY')
        })
        cls._credentials.setdefault('IGUAZIO_ADMINISTRATOR', {
            'USERNAME': dotenv_configs.get('USERNAME'),
            'PASSWORD': dotenv_configs.get('PASSWORD'),
        })
        if not spec:
            spec = dict()
        cls._credentials.update(spec)
        logger = log.get_logger()
        if auths:
            for user_mask, auth_string in auths.items():
                if not auth_string:
                    continue
                auth = tuple(auth_string.split(':'))
                if len(auth) != 2:
                    logger.error(f'Invalid auth: {auth_string}')
                    raise click.Abort('Invalid auth')
                if not cls._credentials.get(user_mask):
                    cls._credentials[user_mask] = dict()
                cls._credentials[user_mask].update({
                    'USERNAME': auth[0],
                    'PASSWORD': auth[1],
                })
        for user_mask, creds in cls._credentials.items():
            if not isinstance(creds, Mapping):
                continue
            mask = cls._credentials[user_mask]
            if not mask.get('USERNAME'):
                logger.error(f'No username for {user_mask}')
                raise click.Abort(user_mask)
            if not any([mask.get('PASSWORD'), mask.get('AUTH_KEY')]):
                logger.error(f'No secret for {user_mask}')
                raise click.Abort(user_mask)
            if mask.get('USERNAME').lower() == 'username':
                logger.error(f'Invalid username for {user_mask}')
                raise click.Abort(user_mask)
        return cls
