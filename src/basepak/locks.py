from typing import Callable

import click


def group_lock(func: Callable) -> Callable:
    """Global Lock decorator for a single node. This lock allows runs from different nodes in the same cluster, so
    collisions are possible in multi-node
    Please avoid scheduling the same step on more than one node!

    Global lock file is at /tmp/{cli_name}/{click_group_name}.lock on the machine where the command is run.

    :param func: function to decorate. click context must be provided to the function as an arg/kwarg
    :return: decorated function
    """
    import fcntl
    import functools
    import os

    from . import log

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ctx = click.get_current_context()

        logger = log.get_logger(name=kwargs.get('logger_name'), level=kwargs.get('log_level'))
        lock_file_dir = os.path.join('/tmp', ctx.obj.get('cli_name') or 'basepak')  # nosec: B108:hardcoded_tmp_directory
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
            except OSError:
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


def clean_locks(ctx: click.Context) -> int:
    """Forcefully remove lock files. Use with caution!
    :param ctx: click context
    :return: 0 if successful, else raise
    """
    import os
    from glob import glob

    lock_file_dir = os.path.join('/tmp', ctx.obj.get('cli_name') or 'basepak')  # nosec: B108:hardcoded_tmp_directory
    paths = glob(os.path.join(lock_file_dir, '*.lock'))
    if not paths:
        click.echo('No lock files found')
        return 0
    success = True
    for path in paths:
        try:
            os.remove(path)
            click.echo(path)
        except Exception as e:  # usually it's a PermissionError
            success = False
            click.echo(f'{path}: {e}', err=True)
    if not success:
        raise click.ClickException('Some lock files could not be removed')
    return 0
