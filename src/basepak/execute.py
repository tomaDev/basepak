from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Dict, Optional


def subprocess_stream(
        cmd: str, output_file: Optional[str | Path] = None, error_file: Optional[str | Path] = None, *args, **kwargs
) -> None:
    """Subprocess run with streaming output to logger.

    :param cmd:         command to run
    :param output_file: file to write stdout to
    :param error_file:  file to write stderr to
    :param args:        additional args to pass to subprocess call
    :param kwargs:      additional kwargs to pass to subprocess call
    """
    logger_name = kwargs.pop('logger_name', None)
    logger = kwargs.pop('logger', None)
    # noinspection PyUnresolvedReferences
    if logger_name and logger and logger_name != logger.name:
        # noinspection PyUnresolvedReferences
        raise ValueError(f'Logger name mismatch: {logger_name} != {logger.name}')

    from . import log
    logger = logger or log.get_logger(name=logger_name)

    stdout = open(output_file, 'w') if output_file else subprocess.PIPE
    stderr = open(error_file, 'w') if error_file else subprocess.PIPE

    stderr_output = []

    import contextlib
    import shlex
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
    """Executable object to run commands with logging and error handling"""
    def __init__(self, name: str, *cmd_base: str, logger: logging.Logger = None,
                 run_kwargs: Dict[str, str | int] = None):
        self.name = self._cmd_base = name
        if cmd_base:
            self._cmd_base = ' '.join(cmd_base)
        self._cmd_base += ' '
        self._args = self._cmd_base
        if not logger:
            from . import log
            logger = log.get_logger(name='plain')
        self.logger = logger
        self.run_kwargs = run_kwargs or dict()

    def __repr__(self):
        return self.with_('')

    def assert_executable(self, name: Optional[str] = None) -> None:
        """Assert executable exists and runnable
        :param name: name of the executable. If None, uses the first word (args[0]) of the command base
        :raises NameError: if executable not found or not runnable
        :raises ValueError: if name is an empty string
        """
        if name == '':
            raise ValueError('Empty string is not a valid name! Use None to assert args[0] of the command base')
        import shutil
        if not shutil.which(name or self._cmd_base.split()[0]):
            raise NameError(f'Command "{name}" not found or has no executable permissions')

    def set_args(self, *args: str) -> None:
        """Set arguments for the command"""
        self._args = self._cmd_base + ' '.join(args) + ' '

    def with_(self, *args: str, **kwargs: Dict[str, str]) -> str:
        """Return current command with provided args and kwargs appended at the end"""
        import json
        ret = self._args + ' '.join(args)
        kwargs = {**self.run_kwargs, **kwargs}
        return ret + json.dumps(kwargs) if kwargs else ret

    def show(self, *args: str, level: str = 'warning', **kwargs: Dict[str, str]) -> None:
        """Log the command
        :param level: log level"""
        try:
            getattr(self.logger, level.lower())(self.with_(*args, **kwargs))
        except AttributeError:
            raise AttributeError(f'Logger for {self.name} has no method {level}')

    def run(self, *args: str, **kwargs) -> subprocess.CompletedProcess:
        """Run the command
        :return: run result"""
        kwargs.setdefault('errors', 'replace')
        kwargs.setdefault('capture_output', True)
        kwargs.setdefault('check', True)
        kwargs.setdefault('shell', True)
        if kwargs.pop('show_cmd', True):
            self.show(*args, level=kwargs.pop('show_cmd_level', 'debug'))
        if kwargs.pop('mode', '') == 'dry-run':
            return subprocess.CompletedProcess([], 0)
        return subprocess.run(self._args + ' '.join(args), **self.run_kwargs, **kwargs)

    def stream(self, *args: str, **kwargs) -> None:
        """Run the command, streaming output to logger
        """
        kwargs.setdefault('logger', self.logger)
        if kwargs.pop('show_cmd', True):
            self.show(*args, level=kwargs.pop('show_cmd_level', 'warning'))
        if kwargs.pop('mode', '') == 'dry-run':
            return
        subprocess_stream(self._args + ' '.join(args), **self.run_kwargs, **kwargs)
