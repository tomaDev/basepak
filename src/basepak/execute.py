from __future__ import annotations

import contextlib
import json
import logging
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Dict

from . import log


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
