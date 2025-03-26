import logging
from typing import AnyStr


def extractall(path: AnyStr, mode: AnyStr, logger: logging.Logger) -> str:
    """Extract tar file to same dir and returns path to extracted dir
    :param path: path to tar file
    :param mode: execution mode
    :param logger: logger instance
    :return: path to extracted dir
    """
    import os
    path = os.path.realpath(os.path.expanduser(path))

    if os.path.isdir(path):
        return path

    # Check whether we have the extracted source dir already
    assumed_dir = os.path.join(os.path.dirname(path), os.path.basename(path).split('.', maxsplit=1)[0])

    if os.path.isdir(assumed_dir):
        logger.info(f'Using assumed {path} as source')
        return str(assumed_dir)

    import tarfile
    try:
        tarfile.is_tarfile(path)
        if mode == 'dry-run':
            assumed_dir = os.path.dirname(path)
            logger.info(f'Would have extracted {path} to {assumed_dir}\nUsing {assumed_dir} as source mock')
            return assumed_dir
        logger.info(f'Extracting {path}')
        with tarfile.open(path) as tar:
            tar.extractall(path=os.path.dirname(path))  # nosec [B202:tarfile_unsafe_members]
        return str(assumed_dir)
    except FileNotFoundError:
        import click
        raise click.MissingParameter(param_type='source', message=f'FileNotFoundError: {path}')
    except (tarfile.ExtractError, tarfile.ReadError) as e:
        raise e
    except Exception as e:
        import click
        raise click.ClickException(f'Error extracting {path}: {e}')


def validate_dir(path: AnyStr) -> str:
    """Validate path is an existent dir with rw permissions

    :param path: path to validate
    :return: validated path"""
    import os
    path = os.path.realpath(os.path.expanduser(path))
    if not os.path.exists(path):
        raise FileNotFoundError(f'{path} not found')
    if os.path.isfile(path):
        raise FileExistsError(f'{path} is a file')
    if not os.path.isdir(path):  # Not a file, not a dir, not a symlink (due to os.path.realpath above). What is it?
        raise NotADirectoryError(f'{path} is not a directory')
    if not os.access(path, os.R_OK | os.W_OK, follow_symlinks=True):
        raise PermissionError(f'No read+write permissions for {path}')
    return path
