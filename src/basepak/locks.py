import functools
import os

import click
import fcntl

from . import log
from . import __name__ as package_name


def group_lock(func):
    """Lock decorator for a single node. Collisions are possible in multi-node, since we allow different steps to run
    from different machines in the same environment. Please avoid scheduling the same step on more than one node"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = log.get_logger(name=kwargs.get('logger_name') or 'long', level=kwargs.get('log_level', 'INFO'))
        ctx = next((arg for arg in args if isinstance(arg, click.Context)), None)
        if ctx is None:
            logger.error('ctx argument must be provided')
            raise click.Abort()
        lock_file_dir = os.path.join('/tmp', ctx.obj.get('cli_name') or package_name)
        try:
            os.makedirs(lock_file_dir, exist_ok=True)
        except PermissionError as e:
            logger.error(f'Failed to create lock file directory: {lock_file_dir}')
            raise click.Abort(e)
        lock_file_path = os.path.join(lock_file_dir, f'{ctx.obj.get("click_group_name") or func.__name__}.lock')
        logger.debug(f'Lock file path: {lock_file_path}')
        with open(lock_file_path, 'w') as lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                logger.error(f'{func.__name__} is already running')
                raise click.Abort()
            try:
                result = func(*args, **kwargs)
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                try:  # try/except to handle the critical section
                    os.remove(lock_file_path)
                except FileNotFoundError:
                    pass
        return result

    return wrapper
