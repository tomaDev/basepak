from __future__ import annotations

import subprocess
import sys
from functools import wraps
from ssl import SSLCertVerificationError

import click
import requests
from igz_mgmt import exceptions as igz_mgmt_exceptions
from tenacity import retry, wait_random_exponential, stop_after_attempt, wait_exponential

from basepak import log


class CustomExecError(Exception):
    def __init__(self, returncode: int, message: str, stderr: str):
        self.returncode = returncode
        self.message = message
        self.stderr = stderr
        super().__init__(self.message)

    def __str__(self):
        return f'CustomExecErr: {self.message}, [{self.returncode}]: {self.stderr}'


class ServiceNotReadyError(Exception):
    def __init__(self, name: str, state: str):
        self.message = f"App service: {name} not ready. Current state: {state}"
        super().__init__(self.message)


class ClusterNotReadyError(Exception):
    def __init__(self, name: str = None, state: str = None, message: str = None):
        self.message = message or f"Cluster: {name} not ready. Current state: {state}"
        super().__init__(self.message)


def retry_strategy_too_many_requests(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        decorated_func = retry(
            reraise=True,
            wait=wait_exponential(multiplier=3, max=600),
        )(func)
        logger = log.get_logger()
        try:
            return decorated_func(*args, **kwargs)
        except KeyboardInterrupt:
            logger.warning('Interrupted by user')
            sys.exit(1)
        except SSLCertVerificationError as e:
            logger.error(f"SSL verification error\n{e}")
            raise e
        except requests.exceptions.ConnectionError as e:
            logger.error(f'Failed to connect\n{e}')
            raise e
        except requests.exceptions.Timeout as e:
            logger.error(f'Connection timed out\n{e}')
            raise e
        except requests.exceptions.HTTPError as e:
            logger.error(e)
            if e.response.status_code in (401, 403, 405):
                sys.exit(1)
            else:
                raise e
    return wrapper


def retry_strategy_default(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        decorated_func = retry(
            reraise=True,
            wait=wait_random_exponential(multiplier=3),
            stop=stop_after_attempt(5),
        )(func)
        logger = log.get_logger()
        try:
            return decorated_func(*args, **kwargs)
        except KeyboardInterrupt:
            logger.warning('Interrupted by user')
            sys.exit(1)
        except SSLCertVerificationError as e:
            logger.error(f"SSL verification error\n{e}")
            raise e
        except requests.exceptions.ConnectionError as e:
            logger.error(f'Failed to connect\n{e}')
            raise e
        except requests.exceptions.Timeout as e:
            logger.error(f'Connection timed out\n{e}')
            raise e
        except requests.exceptions.HTTPError as e:
            logger.error(e)
            if e.response.status_code in (401, 403, 405):
                sys.exit(1)
            else:
                raise click.Abort(e)
        except subprocess.CalledProcessError as e:
            logger.error(e.stderr)
            raise e
        except (FileNotFoundError, FileExistsError, AssertionError, RuntimeError, ValueError, StopIteration,
                ServiceNotReadyError,  ClusterNotReadyError, igz_mgmt_exceptions.AppServiceNotExistsException) as e:
            logger.error(f'{type(e).__name__}: {e}')
            raise e
        except OSError as e:
            logger.error(f'OSError: {e}')
            raise e
        except (KeyError, IndexError, TypeError) as e:
            logger.exception(f'{type(e).__name__}: {e}')
            raise e
        except Exception as e:
            logger.exception(e)
            raise e

    return wrapper
