from __future__ import annotations

import subprocess
import sys
from functools import wraps
from ssl import SSLCertVerificationError
from typing import Optional

import requests
from igz_mgmt import exceptions as igz_mgmt_exceptions
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random_exponential


class CustomExecError(Exception):
    """Custom exception for subprocess errors"""
    def __init__(self, returncode: int, message: str, stderr: str):
        self.returncode = returncode
        self.message = message
        self.stderr = stderr
        super().__init__(self.message)

    def __str__(self):
        return f'CustomExecErr: {self.message}, [{self.returncode}]: {self.stderr}'


class AppServiceNotReadyError(Exception):
    """App service not ready for operation"""
    def __init__(self, name: str, state: str):
        self.message = f"App service: {name} not ready. Current state: {state}"
        super().__init__(self.message)


class ClusterNotReadyError(Exception):
    """Cluster not ready for operation"""
    def __init__(self, name: str = None, state: str = None, message: str = None):
        self.message = message or f"Cluster: {name} not ready. Current state: {state}"
        super().__init__(self.message)


class UnexpectedResponse(requests.exceptions.RequestsWarning):
    """Unexpected response from platform API"""
    def __init__(self, expected: int, received: int, text: Optional[str] = ''):
        self.message = f"Expected status code: {expected}. Received: {received}. {text}"
        super().__init__(self.message)


def retry_strategy_too_many_requests(func):
    """Decorator for retrying requests that return 429 status code"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        decorated_func = retry(
            reraise=True,
            wait=wait_exponential(multiplier=3, max=600),
        )(func)
        from . import log
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
    """Decorator with default retry strategy for exceptions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        decorated_func = retry(
            reraise=True,
            wait=wait_random_exponential(multiplier=3),
            stop=stop_after_attempt(5),
        )(func)
        from . import log
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
                import click
                raise click.Abort(e)
        except subprocess.CalledProcessError as e:
            logger.error(e.stderr)
            raise e
        except (FileNotFoundError, FileExistsError, AssertionError, RuntimeError, ValueError, StopIteration,
                AppServiceNotReadyError, ClusterNotReadyError, igz_mgmt_exceptions.AppServiceNotExistsException) as e:
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
