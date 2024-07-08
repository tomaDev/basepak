import logging
from typing import AnyStr


def print_tar_top_level_members(tar_path: AnyStr):
    import tarfile
    with tarfile.open(tar_path) as tar:
        for member in tar.getmembers():
            if member.name.count('/') != 1:
                continue
            if member.size == 0:
                print('dir', member.name.partition('/')[2])
            else:
                from .units import Unit
                print(Unit(f'{member.size} B'), member.name.partition('/')[2])


def extractall(path: AnyStr, mode: str, logger: logging.Logger) -> str:
    """Extracts tar file to same dir and returns path to extracted dir"""
    import click
    import tarfile
    import os
    path = os.path.realpath(path)

    logger.info(f'realpath: {path}')
    if os.path.isdir(path):
        return path

    # Check whether we have the extracted source dir already
    assumed_dir = os.path.join(os.path.dirname(path), os.path.basename(path).split('.', maxsplit=1)[0])

    if os.path.isdir(assumed_dir):
        logger.info(f'Using assumed {path} as source')
        return str(assumed_dir)
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
        raise click.MissingParameter(param_type='source', message=f'FileNotFoundError: {path}')
    except (tarfile.ExtractError, tarfile.ReadError) as e:
        raise e
    except Exception as e:
        raise click.ClickException(f'Error extracting {path}: {e}')


def validate_dir(path: AnyStr) -> str:
    """Validate path is an existent dir with rw permissions"""
    import os
    path = os.path.realpath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f'{path} not found')
    if os.path.isfile(path):
        raise FileExistsError(f'{path} is a file')
    if not os.path.isdir(path):  # Not a file, not a dir, not a symlink (due to os.path.realpath above). What is it?
        raise NotADirectoryError(f'{path} is not a directory')
    if not os.access(path, os.R_OK | os.W_OK, follow_symlinks=True):
        raise PermissionError(f'No read+write permissions for {path}')
    return path
