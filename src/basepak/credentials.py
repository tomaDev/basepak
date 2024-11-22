from __future__ import annotations

import functools
from collections.abc import Iterable, Mapping
from typing import Dict, Optional, Type


@functools.lru_cache
def load_from_dotenv(dotenv_path: Optional[str] = None) -> Dict[str, str]:
    """Load environment variables from a .env file
    :param dotenv_path: path to .env file
    :return: dict of environment variables"""
    from dotenv import dotenv_values, find_dotenv
    return dotenv_values(str(dotenv_path or find_dotenv()), verbose=True)


class Credentials:
    """Singleton class to store credentials globally

    \b
    Sources supported:
    - Code
    - K8S secrets
    - Dotfile. Path defaults to environment variable BASEPAK_DOTENV_PATH
    """
    _instance = None
    _credentials = dict()

    def __new__(cls):  # Singleton
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get(cls, user_mask: Optional[str] = None, default: Optional[str | dict] = None) -> Dict[str, str | dict] | None:
        """Get credentials for all users or a specific user

        :param user_mask: user mask to get credentials for. If None, return all credentials
        :param default: default value to return if user_mask is not found

        :return: dict of credentials for user_mask or all users
        """
        from copy import deepcopy
        if user_mask:
            return deepcopy(cls._credentials.get(user_mask, default if default else dict()))
        return deepcopy(cls._credentials)

    @classmethod
    def set_from_k8s(cls, user_mask: Optional[str] = None, namespace: Optional[str] = 'default-tenant',
                     selector: Optional[str] = '', skip: Optional[Iterable] = None) -> None:
        """Pull secret from k8s, and set credentials for user_mask

        :param user_mask: user mask to set credentials for
        :param namespace: k8s namespace to pull secrets from
        :param selector: k8s selector to filter secrets
        :param skip: list of user masks to skip"""
        import json

        from . import log
        from .execute import Executable
        logger = log.get_logger()
        kubectl = Executable(
            'kubectl',
            'kubectl get secrets --output json --ignore-not-found --namespace', namespace,
            f'--selector {selector}' if selector else '',
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
            import base64
            creds = {k: base64.b64decode(v).decode(errors='replace') for k, v in item['data'].items()}
            logger.info(f'Loading credentials for {name} from k8s secret: {namespace}/{item["metadata"]["name"]}')
            cls._credentials[name] = creds

    @classmethod
    def set(
            cls,
            spec: Optional[Mapping[str, Mapping[str, str]]] = None,
            auths: Optional[Mapping[str, str]] = None,
            dotenv_path: Optional[str] = None,
    ) -> Type[Credentials]:
        """Set credentials for users in spec and args
        :param spec: dict of dicts to get credentials from a file:
            {
                USER_MASK: {
                    'USERNAME': USERNAME,
                    'PASSWORD': PASSWORD,
                    },
                ...,
            }
        :param auths: dict of strings to get credentials from command line flags:
            {
                USER_MASK: USERNAME:PASSWORD,
                ...,
            }
        :param dotenv_path: path to .env file. Defaults to environment variable BASEPAK_DOTENV_PATH
        :return: Credentials instance
        """
        import os

        import click

        from . import log
        logger = log.get_logger()
        dotenv_path = dotenv_path or os.environ['BASEPAK_DOTENV_PATH']
        if dotenv_configs := load_from_dotenv(dotenv_path):
            logger.debug(f'{dotenv_path=}')
            for user_mask, creds in dotenv_configs.items():
                if not creds:
                    cls._credentials.setdefault(user_mask, None)
                elif ':' not in creds:
                    cls._credentials.setdefault(user_mask, creds)
                else:
                    user, secret = creds.split(':')
                    cls._credentials.setdefault(user_mask, {'USERNAME': user, 'PASSWORD': secret})
        if not spec:
            spec = dict()
        cls._credentials.update(spec)
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

        logger.debug(f'Loaded credentials for masks: {cls._credentials}')
        return cls
