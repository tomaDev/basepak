from __future__ import annotations

import json
import logging
import os
import re
import shutil
from collections.abc import Mapping
from functools import partial
from numbers import Number
from typing import Callable, Optional, AnyStr, Sequence

import rich
from rich import theme, table, box, console
from rich.logging import RichHandler

LOGGERS: set[str] = set()
LOG_MASK = '********'
SECRET_KEYWORD_FLAGS = ['password', 'data-access-key', 'control-access-key', 'access-key', 'db-auth-key']
SECRET_KEYWORD_PATTERNS = ['password && echo ', "[\"']PASSWORD[\"']:[ ]?[\"']", 'PASSWORD=', 'password: ']
LOG_FILE_NAME_DEFAULT = 'basepak.log'
APP_NAME_DEFAULT = 'basepak'

EXPRESSIONS_TO_MASK = [
    rf'((?:--)?{keyword}[ =])[\S]+' for keyword in SECRET_KEYWORD_FLAGS
] + [
    rf'({keyword})[\S]+' for keyword in SECRET_KEYWORD_PATTERNS
]

RICH_THEME_KWARGS_DEFAULT = {
    'logging.warning': 'yellow',
    'logging.level.warning': 'bold yellow',
}

rich.reconfigure(
    width=shutil.get_terminal_size(fallback=(140, 24)).columns,  # fallback for running in cron
    theme=theme.Theme(RICH_THEME_KWARGS_DEFAULT),
)

Table = partial(table.Table, header_style='bold magenta', box=box.MARKDOWN)


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


class MaskingFilter(logging.Filter):
    """Filter to mask sensitive information in log messages"""
    def filter(self, record: logging.LogRecord):
        if type(record.msg) not in (str, bytes):
            record.msg = str(record.msg)
        for expression in EXPRESSIONS_TO_MASK:
            if re.search(expression, record.msg):
                substitution = r'\1' + LOG_MASK
                record.msg = re.sub(expression, substitution, record.msg)
        return True


class _BaseRichHandler(RichHandler):
    def __init__(self, *args, **kwargs):
        # if is_yes(os.environ.get('NO_COLOR')):
        #     kwargs['console'] = console.Console(no_color=True)
        kwargs.update({
            'show_path': False,
            # 'markup': False,  # added for visibility, as this is the default. On k8s events, markup may error out
        })
        super().__init__(*args, **kwargs)
        self.addFilter(MaskingFilter())
        self.propagate = False  # prevent messages from being passed to the root logger


class _RichRichHandler(_BaseRichHandler):
    def __init__(self, *args, **kwargs):
        kwargs['rich_tracebacks'] = True
        super().__init__(*args, **kwargs)
        self.tracebacks_suppress = [  # solid libs, skip tracebacks
            # installed
            'click', 'requests', 'urllib3', 'paramiko', 'tenacity', 'rich', 'paramiko', 'psutil', 'scp',
            # built-in
            'futures', 'concurrent',
        ]


class ShortRichHandler(_RichRichHandler):
    """Short RichHandler for logging messages

    Example:

    15:30:27 INFO     message
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setFormatter(logging.Formatter(fmt='%(message)s', datefmt='%X', ))


class LongRichHandler(_RichRichHandler):
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


def name_to_handler(name: str, *args, **kwargs) -> logging.StreamHandler:
    """Retrieve a log stream handler
    :param name: name of the handler
    :return: instance of the handler
    :raises ValueError: if the handler name is not supported
    """
    try:
        return SUPPORTED_LOGGERS[name](*args, **kwargs)
    except KeyError:
        raise ValueError(f'Unsupported logger name: {name}. Supported names are: {SUPPORTED_LOGGERS.keys()}')


def get_logger(name: Optional[str] = None, level: Optional[str | int] = None) -> logging.Logger:
    """Retrieve or create a globally scoped logger

    :param name: logger name, which dictates its configuration. Default is 'short'
    :param level: instance Log level. If None, will adopt level of first found existing logger. Else, default is INFO
    :return: Configured logger instance
    """
    name = name or 'short'
    if name in LOGGERS:
        return logging.getLogger(name)
    level = level or (logging.INFO if not LOGGERS else logging.getLogger(next(iter(LOGGERS))).getEffectiveLevel())
    logger = logging.getLogger(name)
    logger.addHandler(name_to_handler(name))
    logger.setLevel(logging.getLevelName(level) if isinstance(level, int) else level.upper())

    LOGGERS.add(name)

    if not is_yes(os.environ.get('BASEPAK_WRITE_LOG_TO_FILE')):
        return logger

    log_file_name = os.environ.get('BASEPAK_LOG_FILE_NAME') or LOG_FILE_NAME_DEFAULT
    try:
        log_path =  os.environ.get('BASEPAK_LOG_PATH', os.path.expanduser('~') + '/' + log_file_name)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        file_console = console.Console(file=open(log_path, 'a', encoding='utf-8', errors='replace'), tab_size=2,
                                       width=shutil.get_terminal_size(fallback=(140, 24)).columns, force_terminal=True,)
        file_handler = name_to_handler(name, console=file_console, rich_tracebacks=True)
        file_handler.addFilter(MaskingFilter())
        logger.addHandler(file_handler)
    except:  # noqa too broad - best effort basis
        pass
    return logger

def _write_table_to_file(table_: rich.table.Table) -> None:
    log_file_name = os.environ.get('BASEPAK_LOG_FILE_NAME') or LOG_FILE_NAME_DEFAULT
    log_path = os.environ.get('BASEPAK_LOG_PATH', os.path.expanduser('~') + '/' + log_file_name)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, 'a', encoding='utf-8', errors='replace') as f:
        w = console.Console(file=f, force_terminal=True)
        w.print(table_)


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

    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    import re
    for pattern, replacement in patterns.items():
        content = re.sub(pattern, replacement, content)

    with open(path, 'w', encoding='utf-8', errors='replace') as f:
        f.write(content)


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
