from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Dict, Optional

import re

def _get_console_from_logger(logger: logging.Logger):
    """Return the Console from a RichHandler if available, else a new Console()."""
    for h in logger.handlers:
        # RichHandler has a .console attribute
        if hasattr(h, 'console'):
            return h.console
    raise RuntimeError('No console available')



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
        return ret + ' ' + json.dumps(kwargs) if kwargs else ret

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


    # todo: integrate into stream command
    def stream_with_progress(
            self, title: str = '',
            cwd: Optional[str | os.PathLike] = None,
            env: Optional[dict] = None,
            show_cmd: bool = True,
            mode: str = 'normal',
            **kwargs,
    ) -> int:
        """Run a command with progress bar and return its exit status

        :param title: title of the progress bar
        :param cwd: working directory, defaults to None
        :param env: environment variables, defaults to None
        :param show_cmd: whether to show the command, defaults to True
        :param mode: execution mode, defaults to 'dry-run'
        :return: exit status code of the command run
        """
        title = title if title else ''
        cwd = cwd if cwd else None

        parsed_kwargs = ' '
        for k, v in kwargs.items():
            parsed_kwargs += f'{k}{v} ' if (k.strip().endswith('=') or v.strip().startswith('=')) else f'{k} {v} '

        cmd = self._args + parsed_kwargs

        if show_cmd:
            self.logger.info(cmd)
        if mode == 'dry-run':
            return 0

        percent_re = re.compile(r'(\d{1,3})%')
        bracketed_progress_re = re.compile(r'^\[.*\]\s+\d{1,3}%\s*$')
        just_percent_re = re.compile(r'^\s*\d{1,3}%\s*$')

        def is_progress_line(s: str) -> Optional[int]:
            m = percent_re.search(s)
            if not m:
                return None
            if bracketed_progress_re.match(s) or just_percent_re.match(s):
                return max(0, min(100, int(m.group(1))))
            return None

        from rich.progress import Progress, TextColumn, BarColumn, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn

        with Progress(
                TextColumn(title),
                SpinnerColumn(),
                BarColumn(),
                TextColumn('{task.percentage:>5.1f}%'),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=_get_console_from_logger(self.logger),
                transient=True,
        ) as progress:
            task_id = progress.add_task('run', total=100)
            proc = subprocess.Popen(
                shell=True,
                args=cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(cwd) if cwd else None,
                env=env,
            )

            for raw_line in proc.stdout:
                line = raw_line.split('\r')[-1].rstrip('\n')
                pct = is_progress_line(line)
                if pct is not None:
                    progress.update(task_id, completed=pct)
                    continue
                if line.strip():
                    self.logger.info(line)

            return proc.wait()
