from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sys
from collections.abc import Mapping, Sequence
from functools import partial
from numbers import Number
from typing import AnyStr, Callable, Optional

import rich
from rich import box, console, table, theme
from rich.logging import RichHandler

LOGGERS: set[str] = set()
LOG_FILE_NAME_DEFAULT = 'basepak.log'
APP_NAME_DEFAULT = 'basepak'
RICH_THEME_KWARGS_DEFAULT = {
    'logging.warning': 'yellow',
    'logging.level.warning': 'bold yellow',
}

def register_exception_hook(logger_name='plain'):
    def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
        logger = get_logger(logger_name)
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.critical('Uncaught Exception:', exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = log_uncaught_exceptions


def is_yes(input_: Optional[str | Number]) -> bool:
    """Check if the input string is a yes
    :param input_: input string
    :return: True if the input string is a yes
    """
    if input_ is None:
        return False
    if isinstance(input_, Number):
        return bool(input_)
    return input_.lower() in ('y', 'yes', 'true', '1')


TERMINAL_SIZE_FALLBACK = (140, 24)  # fallback for running in cron or non-interactive environments
_terminal_size_columns = shutil.get_terminal_size(fallback=TERMINAL_SIZE_FALLBACK).columns

rich.reconfigure(
    width=_terminal_size_columns,
    theme=theme.Theme(RICH_THEME_KWARGS_DEFAULT),
    force_terminal=not is_yes(os.environ.get('NO_COLOR')),
    soft_wrap=True,
)

Table = partial(
    table.Table,
    header_style='bold magenta',
    box=box.MARKDOWN,
    show_edge=False,
)


def redact_str(string: str, mask: Optional[str] = '*', plaintext_suffix_length: Optional[int] = 4) -> str:
    """Redact a string, leaving only the last `plaintext_suffix_length` characters unmasked
    :param string: string to redact
    :param mask: mask character to use. Default is '*'
    :param plaintext_suffix_length: number of characters to leave unmasked at the end of the string. Default is 4
    :return: redacted string
    """
    if len(string) <= plaintext_suffix_length:
        return mask * len(string)
    redacted = (mask if char not in ('-', ' ') else char for char in string[:-plaintext_suffix_length])
    return ''.join(redacted) + string[-plaintext_suffix_length:]


LOG_MASK = '********'
SENSITIVE_PATH_KEYWORDS = ['password', 'secret', 'token', 'key', 'cred', 'auth']

# Build a case-insensitive subpattern that matches a redirect to a path
# whose basename contains any sensitive keyword. Supports quoted paths
# and optional FD redirection like `1>` or `2>>`.
KEYS = '|'.join(map(re.escape, SENSITIVE_PATH_KEYWORDS))
SENSITIVE_REDIRECT = rf"""
    \s*                # optional whitespace
    (?:\d{{1,2}})?     # optional FD number, e.g., 1>, 2>>
    \s*>>?             # '>' or '>>'
    \s*
    (?:                # target path (quoted or unquoted) that includes a keyword
        "(?:[^"]*(?:{KEYS})[^"]*)"      |
        '(?:[^']*(?:{KEYS})[^']*)'      |
        [^\s"'|;&]*(?:{KEYS})[^\s"'|;&]*
    )
"""

# Mask ONLY the payload of echo/printf if followed by a sensitive redirect.
# Group 1: the command and flags + trailing spaces
# Group 2: the payload to mask
ECHO_PRINTF_SENSITIVE_MASK = re.compile(
    rf"""
    (?ix)                                  # ignore case, verbose
    (\b(?:echo|printf)(?:\s+-[neE]+)*\s+)  # 1: echo/printf with optional flags and space
    (.+?)                                  # 2: payload (lazy)
    (?=                                    # lookahead: must be followed by sensitive redirect
        {SENSITIVE_REDIRECT}
        \s*(?:\|\||&&|;|\||$)              # then end or next operator
    )
    """,
    re.IGNORECASE | re.VERBOSE,
    )

# (Keep your existing patterns as-is, then add the one above at the end)
SECRET_KEYWORD_FLAGS = ['password', 'data-access-key', 'control-access-key', 'access-key', 'db-auth-key']
SECRET_KEYWORD_PATTERNS = ['password && echo ', r"""["']PASSWORD["']:\s?["']""", 'PASSWORD=', 'password: ']

EXPRESSIONS_TO_MASK = [
                          re.compile(rf'((?:--)?{keyword}[ =])\S+') for keyword in SECRET_KEYWORD_FLAGS
                      ] + [
                          re.compile(rf'({keyword})\S+') for keyword in SECRET_KEYWORD_PATTERNS
                      ] + [
                          ECHO_PRINTF_SENSITIVE_MASK,
                      ]

class MaskingFilter(logging.Filter):
    """Filter to mask sensitive information in log messages"""
    def filter(self, record: logging.LogRecord):
        # Work on the rendered message, not raw msg+args, so formatters don't resurrect secrets
        message = record.getMessage() if hasattr(record, 'getMessage') else str(record.msg)
        for pattern in EXPRESSIONS_TO_MASK:
            # Replace payload with LOG_MASK while preserving the command (group 1)
            message = pattern.sub(r"\1" + LOG_MASK, message)
        record.msg = message
        record.args = ()  # avoid reformatting with stale args
        return True


class _BaseRichHandler(RichHandler):
    def __init__(self, *args, **kwargs):
        kwargs.update({
            'show_path': False,
            # 'markup': False,  # added for visibility, as this is the default. On k8s events, markup may error out
        })
        super().__init__(*args, **kwargs)
        self.addFilter(MaskingFilter())
        self.propagate = False  # prevent messages from being passed to the root logger


class ShortRichHandler(_BaseRichHandler):
    """Short RichHandler for logging messages

    Example:

    15:30:27 INFO     message
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setFormatter(logging.Formatter(fmt='%(message)s', datefmt='%X', ))


class LongRichHandler(_BaseRichHandler):
    """Long RichHandler for logging messages

    Example:

    2021-10-01 15:30:27.123456 INFO     message
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setFormatter(logging.Formatter(fmt='%(message)s', datefmt='%Y-%m-%d %H:%M:%S.%s', ))


class PlainRichHandler(_BaseRichHandler):
    """Plain RichHandler for logging messages

    Example:

    message
    """
    def emit(self, record):
        log_entry = self.format(record)
        self.console.print(log_entry)


SUPPORTED_LOGGERS = {
    'short': ShortRichHandler,
    'long': LongRichHandler,
    'plain': PlainRichHandler,
}


def name_to_handler(name: str, *args, **kwargs) -> logging.Handler:
    """Retrieve a log stream-handler
    :param name: handler name
    :return: instance of the handler
    :raises ValueError: if the handler name is not supported
    """
    try:
        return SUPPORTED_LOGGERS[name](*args, **kwargs)
    except KeyError:
        raise ValueError(f'Unsupported logger name: {name}. Supported names are: {SUPPORTED_LOGGERS.keys()}')


def get_logger(name: Optional[str] = None, level: Optional[str | int] = None, ) -> logging.Logger:
    """Retrieve or create a globally scoped logger

    :param name: logger name, which dictates its configuration. Default is 'short'
    :param level: instance Log level. If None, will adopt level of first found existing logger. Else, default is INFO
    :return: Configured logger instance
    """
    name = name or 'short'
    logger = logging.getLogger(name)
    level = level or (logging.INFO if not LOGGERS else logging.getLogger(next(iter(LOGGERS))).getEffectiveLevel())

    if name in LOGGERS:
        logger.setLevel(logging.getLevelName(level) if isinstance(level, int) else level.upper())
        return logger
    
    logger.addHandler(name_to_handler(name))
    logger.setLevel(logging.getLevelName(level) if isinstance(level, int) else level.upper())
    LOGGERS.add(name)

    if not is_yes(os.environ.get('BASEPAK_WRITE_LOG_TO_FILE')):
        return logger

    try:
        file_console = console.Console(
            width=_terminal_size_columns,
            force_terminal=not is_yes(os.environ.get('NO_COLOR')),
            soft_wrap=True,
        )
        file_console.file = open(_set_log_path(), 'a', encoding='utf-8', errors='replace')
        file_handler = name_to_handler(name, console=file_console, rich_tracebacks=True)
        file_handler.addFilter(MaskingFilter())
        logger.addHandler(file_handler)
        register_exception_hook()
    except Exception as e:  # noqa too broad - best effort basis
        raise e
    return logger


def _write_table_to_file(table_: rich.table.Table) -> None:
    with open(_set_log_path(), 'a', encoding='utf-8', errors='replace') as f:
        w = console.Console(
            width=_terminal_size_columns,
            force_terminal=not is_yes(os.environ.get('NO_COLOR')),
            soft_wrap=True,
            file=f,
        )
        w.print(table_)

def _set_log_path() -> str:
    log_file = os.environ.setdefault('BASEPAK_LOG_FILE', LOG_FILE_NAME_DEFAULT)
    log_dir = os.environ.setdefault('BASEPAK_LOG_DIR', os.path.expanduser('~'))
    log_path = os.environ.setdefault('BASEPAK_LOG_PATH', os.path.join(log_dir, log_file))

    os.environ['BASEPAK_LOG_FILE'] = os.path.basename(log_path)
    log_dir = os.environ['BASEPAK_LOG_DIR'] = os.path.dirname(log_path)
    if os.environ.get('BASEPAK_WRITE_LOG_TO_FILE'):
        os.makedirs(log_dir, exist_ok=True)
    return log_path

def print_table(table_: rich.table.Table) -> None:
    rich.print(table_)
    if is_yes(os.environ.get('BASEPAK_WRITE_LOG_TO_FILE')):
        _write_table_to_file(table_)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        import datetime
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


def log_as(syntax: str, data: Optional[Mapping | str] = None, printer: Optional[Callable] = None,
           yaml_default_flow_style: Optional[bool] = True) -> None:
    """Print data to console as file

    :param syntax: supported formatting syntax: yaml, json
    :param data: data to print
    :param printer: printer function to use, defaults to logger.info
    :param yaml_default_flow_style: yaml style. True - Flow style for brevity. False - Block style for readability
    """
    if not data:
        return
    if syntax == 'yaml':
        import io

        import ruyaml as yaml

        from .strings import iter_to_case
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
    printer = printer or get_logger('plain').info
    printer(log_msg)

def redact_file(path: AnyStr, keys: Optional[Sequence[str]] = None) -> None:
    """Redact sensitive information in a file by applying one or more regex substitutions in-place.

    :param path: Path to the file to be redacted.
    :param keys: List of sensitive keys to redact. Defaults to the log mask expressions.
    """
    path = str(path)
    patterns = dict()
    for key_ in keys or SECRET_KEYWORD_FLAGS:
        patterns[rf'(?i)({key_}\s*=\s*)(\S+)'] = rf'\1{LOG_MASK}'
        patterns[rf'(?i)({key_}\s+)(\S+)'] = rf'\1{LOG_MASK}'

    with open(path, encoding='utf-8', errors='replace') as f:
        content = f.read()

    import re
    for pattern, replacement in patterns.items():
        content = re.sub(pattern, replacement,
                         content) # noqa

    with open(path, 'w', encoding='utf-8', errors='replace') as f:
        f.write(
            content # noqa
        )
