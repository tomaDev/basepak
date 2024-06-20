########################################################################################################################
# Lifted generic setup. Need to extract
########################################################################################################################
import collections
import os
import subprocess
from pathlib import Path
from typing import Iterator

import click

_BASE_ENV = os.environ


def _mk_env(env):
    if env is None:
        return _BASE_ENV
    return dict(collections.ChainMap(env, _BASE_ENV))


def assert_binary_exists(name):  # todo: find a home for this (Executable?)
    try:
        call(f"command -v {name}", stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        raise click.ClickException(click.style(f"Unable to find {name}, does it exist?", fg="yellow"))


def call(cmd, env=None, **kwargs):
    return subprocess.check_call(cmd, cwd=Path.home(), shell=True, env=_mk_env(env), **kwargs)


########################################################################################################################
# Wrapper
########################################################################################################################

SIGKILL_EXIT_COND = 130


def select(options: Iterator[str], preview=None) -> str:
    assert_binary_exists("fzf")
    command = ["fzf", "--ansi", "--layout=reverse", "--height", "80%", "--border"]
    if preview:
        command.extend(["--preview", preview])
    proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    for option in options:
        try:
            proc.stdin.write(f"{option}\n".encode())
            proc.stdin.flush()
        except BrokenPipeError:
            break
    selection = proc.communicate()[0].strip().decode()
    if proc.returncode == SIGKILL_EXIT_COND:
        raise KeyboardInterrupt()
    elif proc.returncode:
        raise click.ClickException(f"non-zero exit code from fzf ({proc.returncode})")
    return selection
