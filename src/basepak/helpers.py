from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import shlex
import shutil
import subprocess
import tarfile
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Optional, List, Mapping, Dict, Callable

import click
import psutil
import ruyaml as yaml

from . import log
from .classes import DateTimeEncoder
from .units import Unit


class Executable:
    def __init__(self, name: str, *cmd_base: str, logger: logging.Logger = None,
                 run_kwargs: Dict[str, str | int] = None):
        self.name = self._cmd_base = name
        if cmd_base:
            self._cmd_base = ' '.join(cmd_base)
        self._cmd_base += ' '
        self._args = self._cmd_base
        self.logger = logger
        self.run_kwargs = run_kwargs or dict()

    def __repr__(self):
        return self.with_('')

    @staticmethod
    def assert_executable(name: str):
        """Assert executable exists and runnable"""
        if not shutil.which(name):
            raise NameError(f'Command "{name}" not found or has no executable permissions')

    def set_args(self, *args: str):
        self._args = self._cmd_base + ' '.join(args) + ' '

    def with_(self, *args: str, **kwargs: Dict[str, str]) -> str:
        ret = self._args + ' '.join(args)
        kwargs = {**self.run_kwargs, **kwargs}
        return ret + json.dumps(kwargs) if kwargs else ret

    def show(self, *args: str, level: str = 'warning', **kwargs: Dict[str, str]):
        try:
            getattr(self.logger, level.lower())(self.with_(*args, **kwargs))
        except AttributeError:
            raise AttributeError(f'Logger for {self.name} has no method {level}')

    def run(self, *args: str, **kwargs):
        kwargs.setdefault('errors', 'replace')
        kwargs.setdefault('capture_output', True)
        kwargs.setdefault('check', True)
        kwargs.setdefault('shell', True)
        if kwargs.pop('show_cmd', True):
            self.show(*args, level=kwargs.pop('show_cmd_level', 'debug'))
        return subprocess.run(self._args + ' '.join(args), **self.run_kwargs, **kwargs)

    def stream(self, *args: str, **kwargs):
        kwargs.setdefault('logger', self.logger)
        if kwargs.pop('show_cmd', True):
            self.show(*args, level=kwargs.pop('show_cmd_level', 'warning'))
        subprocess_stream(self._args + ' '.join(args), **self.run_kwargs, **kwargs)


def print_as(syntax: str, data: Mapping | str, printer=None, yaml_default_flow_style: bool = True):
    """Print data to console as file

    @param syntax  - supported formatting syntax: yaml, json
    @param data    - data to print
    @param printer - printer function to use, defaults to logger.info
    @param yaml_default_flow_style - yaml style. True - Flow style for brevity. False - Block style for readability
    """
    if not data:
        return
    if syntax == 'yaml':
        if isinstance(data, Mapping):
            data = iter_to_case(data, target_case='camelBackCase')
        stream = io.StringIO()
        yaml_instance = yaml.YAML(typ='safe', pure='True')
        yaml_instance.default_flow_style = yaml_default_flow_style
        yaml_instance.dump(data, stream)
        log_msg = stream.getvalue()
    elif syntax == 'json':
        if isinstance(data, Mapping):
            data = json.dumps(data, cls=DateTimeEncoder)
        log_msg = json.dumps(json.loads(data), sort_keys=True, indent=2, cls=DateTimeEncoder)
    else:
        raise NotImplementedError(f'Printing data as {syntax} is not implemented')
    printer = printer or log.get_logger('plain').info
    printer(log_msg)


def subprocess_stream(
        cmd: str, output_file: Optional[str | Path] = None, error_file: Optional[str | Path] = None, *args, **kwargs
) -> None:
    """Subprocess run with streaming output to logger.

    @param cmd: str              - command to run
    @param output_file: str      - file to write stdout to
    @param error_file: str       - file to write stderr to
    @param args: list[any]       - additional args to pass to subprocess call
    @param kwargs: dict[any,any] - additional kwargs to pass to subprocess call
    """
    logger_name = kwargs.pop('logger_name', None)
    logger = kwargs.pop('logger', None)
    # noinspection PyUnresolvedReferences
    if logger_name and logger and logger_name != logger.name:
        # noinspection PyUnresolvedReferences
        raise ValueError(f'Logger name mismatch: {logger_name} != {logger.name}')
    logger = logger or log.get_logger(name=logger_name)
    stdout = open(output_file, 'w') if output_file else subprocess.PIPE
    stderr = open(error_file, 'w') if error_file else subprocess.PIPE

    stderr_output = []
    with stdout if output_file else contextlib.nullcontext(), stderr if error_file else contextlib.nullcontext():
        out = subprocess.Popen(shlex.split(cmd), stdout=stdout, stderr=stderr, *args, **kwargs)
        if stdout == subprocess.PIPE:
            for line in out.stdout:
                logger.info(line.decode('utf-8', errors='replace').rstrip())
        if stderr == subprocess.PIPE:
            for line in out.stderr:
                decoded_line = line.decode('utf-8', errors='replace').rstrip()
                logger.error(decoded_line)
                stderr_output.append(decoded_line)

    if out.wait() != 0:
        logger.error(f'Command failed: {cmd}')
        raise subprocess.CalledProcessError(out.returncode, cmd, stderr='\n'.join(stderr_output))


def iter_to_case(input_: Iterable, target_case: str = 'UPPER_SNAKE_CASE',
                 skip_prefixes: str | list | None = None) -> Iterable | Mapping:
    """
    Converts all keys in the dictionaries of a list to a specified case

    @param input_: The iterable to be converted
    @param target_case: The target case for the dictionary keys, either "UPPER_SNAKE_CASE" or "camelBackCase"
    @param skip_prefixes: skip converting trees, the keys of which start with any of these prefixes
    @return: The converted iterable
    """
    output_dict = {}
    if isinstance(input_, (str, int, float, bool)):
        return input_
    if isinstance(skip_prefixes, str):
        skip_prefixes = [skip_prefixes]
    if isinstance(input_, Mapping):  # dict, OrderedDict, etc
        for key, value in input_.items():
            if skip_prefixes and any(key.startswith(prefix) for prefix in skip_prefixes):
                output_dict[key] = value
                continue
            if isinstance(value, Iterable) and not isinstance(value, str):
                value = iter_to_case(value, target_case, skip_prefixes)

            if target_case == 'UPPER_SNAKE_CASE':
                new_key = camel_to_upper_snake_case(key)
            elif target_case == 'camelBackCase':
                new_key = snake_to_camel_back_case(key)
            else:
                raise NotImplementedError(f'Requested case {target_case} not implemented yet')

            output_dict[new_key] = value

    elif isinstance(input_, Iterable):  # list, tuple, etc
        return type(input_)(iter_to_case(item, target_case, skip_prefixes) for item in input_)  # type: ignore

    return output_dict


def camel_to_upper_snake_case(value):
    """Convert CamelCase/camelBack to UPPER_SNAKE_CASE"""
    new_key = ''
    for i, letter in enumerate(value):
        if i > 0 and letter.isupper() and not value[i - 1].isupper():
            new_key += '_'
        new_key += letter.upper()
    return new_key


def snake_to_camel_back_case(value):
    """Convert SNAKE_CASE to camelBackCase"""
    new_key = ''
    capitalize_next = False
    for letter in value:
        if letter == '_':
            capitalize_next = True
        elif capitalize_next:
            new_key += letter.upper()
            capitalize_next = False
        else:
            new_key += letter.lower()
    return new_key


def iter_without(iter_: Iterable, without: Iterable) -> list:
    return [item for item in iter_ if item not in without]


def tail(file_path: str | bytes, n: int) -> List[str]:
    """Tails the last n lines of a file"""
    with open(file_path, 'rb') as file:
        file.seek(0, os.SEEK_END)
        buffer = io.BytesIO()
        remaining = n + 1
        while remaining > 0 and file.tell() > 0:
            block_size = min(4096, file.tell())
            file.seek(-block_size, os.SEEK_CUR)
            block = file.read(block_size)
            buffer.write(block)
            file.seek(-block_size, os.SEEK_CUR)
            remaining -= block.count(b'\n')
        buffer.seek(0, os.SEEK_SET)
        return buffer.read().decode(errors='replace').splitlines()


def pattern_in_tail(path: str | bytes, pattern: str, logger: logging.Logger, num_of_lines: int = 100) -> bool:
    """Tails the last n lines of a file and checks if pattern is in any of them
    :param path: path to file
    :param pattern: pattern to look for
    :param logger: logger
    :param num_of_lines: number of lines to tail
    :return: True if pattern is found, raise AssertionError otherwise"""
    logger.info(f'Tailing last {num_of_lines} lines of {path}')
    lines = tail(path, num_of_lines)
    if not any(pattern in line for line in lines):
        for line in lines:
            logger.warning(line)
        raise StopIteration(f'pattern "{pattern}" not found')
    logger.info(f'pattern "{pattern}" found')
    return True


def kubectl_dump(command: str | Executable, output_file: str | Path, mode: str = 'dry-run'):
    """Runs kubectl command and saves output to file

    :param command: kubectl command to run
    :param output_file: file to save output to
    :param mode: 'dry-run' or 'normal'
    """
    command = str(command)
    output_file = str(output_file)
    logger = log.get_logger(name='plain')
    logger.info(f'{command} > {output_file}')
    if mode == 'dry-run':
        return
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    error_file = f'{output_file}.err'
    subprocess_stream(command, output_file=output_file, error_file=error_file)
    if os.path.getsize(error_file) == 0:
        os.remove(error_file)


def confirm_default(banner: Optional[str] = 'Continue?', style_kwargs: Optional[dict] = None,
                    confirm_kwargs: Optional[dict] = None):
    if style_kwargs is None:
        style_kwargs = {'fg': 'yellow'}
    if confirm_kwargs is None:
        confirm_kwargs = {'abort': True}
    click.confirm(click.style(banner, **style_kwargs), **confirm_kwargs)


def print_tar_top_level_members(tar_path):  # todo: make use of this or delete
    with tarfile.open(tar_path) as tar:
        for member in tar.getmembers():
            if member.name.count('/') != 1:
                continue
            if member.size == 0:
                print('dir', member.name.partition('/')[2])
            else:
                print(Unit(f'{member.size} B'), member.name.partition('/')[2])


def extractall(path: str, mode: str, logger: logging.Logger) -> str:
    """Extracts tar file to same dir and returns path to extracted dir"""
    path = os.path.realpath(path)

    logger.info(f'realpath: {path}')
    if os.path.isdir(path):
        return path

    # Check whether we have the extracted source dir already
    assumed_dir = os.path.join(os.path.dirname(path), os.path.basename(path).split('.', maxsplit=1)[0])

    if os.path.isdir(assumed_dir):
        logger.info(f'Using assumed {path} as source')
        return assumed_dir
    try:
        tarfile.is_tarfile(path)
        if mode == 'dry-run':
            assumed_dir = os.path.dirname(path)
            logger.info(f'Would have extracted {path} to {assumed_dir}\nUsing {assumed_dir} as source mock')
            return assumed_dir
        logger.info(f'Extracting {path}')
        with tarfile.open(path) as tar:
            tar.extractall(path=os.path.dirname(path))  # nosec [B202:tarfile_unsafe_members]
        return assumed_dir
    except FileNotFoundError:
        raise click.MissingParameter(param_type='source', message=f'FileNotFoundError: {path}')
    except (tarfile.ExtractError, tarfile.ReadError) as e:
        raise e
    except Exception as e:
        raise click.ClickException(f'Error extracting {path}: {e}')


def validate_dir(path) -> str:
    """Validate path is an existent dir with rw permissions"""
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


def truncate(string: str, max_len: int, hash_len: int = 4, suffix: str = '') -> str:
    if len(string) <= max_len:
        return string
    salt = hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()[:hash_len]
    return string[:max_len - hash_len - len(suffix)] + salt + suffix


def truncate_middle(
        string: str,
        max_len: int = 63,  # k8s job name limit - 63 characters
        hash_len: int = 4,
        delimiter: str = '-'
) -> str:
    if len(string) <= max_len:
        return string
    upto = (max_len + hash_len + len(delimiter)) // 2
    from_ = (max_len - hash_len - len(delimiter)) // 2
    return truncate(string[:upto + 1], upto, hash_len, delimiter) + string[-from_:]


def validate_os_thresholds(thresholds: dict[str, Optional[float]], logger: logging.Logger, mode: str) -> None:
    if not thresholds:
        logger.warning('No thresholds provided - skipping')
        return
    _await_stat(thresholds.get('MEMORY_PERCENT'), stat=_get_virtual_memory, name='memory', logger=logger, mode=mode)
    _await_stat(thresholds.get('CPU_PERCENT'), stat=_get_load_avg, name='load avg', logger=logger, mode=mode)


def _await_stat(threshold: Optional[float] = None, iterations: Optional[int] = 60, stat: Callable = None,
                name: str = None, logger: logging.Logger = None, mode='dry-run') -> None:
    logger = logger or log.get_logger(name='plain')
    if threshold is None:
        logger.warning(f'No {name} threshold provided - skipping')
        return
    if mode != 'normal':
        return
    running_stat = stat()
    logger.debug(f'Initial {name} usage: {running_stat: .2f}%')
    if running_stat < threshold:
        return
    ratio = 3  # aging ratio, to give more weight to the more recent values
    logger.warning(f'Awaiting {name} average usage to undershoot {threshold}%...')
    for i in range(iterations):
        running_stat = (running_stat * (ratio - 1) + stat()) / ratio
        logger.info(f'{i: >2} of {iterations}: {running_stat: .2f}%')
        if running_stat < threshold:
            return
        time.sleep(1)
    raise AssertionError(f'{name} usage threshold: {threshold}%. Current usage: {running_stat: .2f}%')  # noqa w0202


def _get_load_avg() -> float:
    return psutil.getloadavg()[0] / psutil.cpu_count()


def _get_virtual_memory() -> float:
    return psutil.virtual_memory()._asdict()['percent']  # noqa w0212
