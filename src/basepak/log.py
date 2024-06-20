from __future__ import annotations

import logging
import re
import shutil
from functools import lru_cache, partial

import rich
from rich.logging import RichHandler

LOGGERS: set[str] = set()
LOG_MASK = '********'
SECRET_KEYWORD_FLAGS = ['password', 'data-access-key', 'control-access-key', 'access-key', 'db-auth-key']
SECRET_KEYWORD_PATTERNS = ['password && echo ', "[\"']PASSWORD[\"']:[ ]?[\"']", 'PASSWORD=', 'password: ']

EXPRESSIONS_TO_MASK = [
    rf'(--{keyword}[ =])[\S]+' for keyword in SECRET_KEYWORD_FLAGS
] + [
    rf'({keyword})[\S]+' for keyword in SECRET_KEYWORD_PATTERNS
]

RICH_THEME_KWARGS_DEFAULT = {
    'logging.warning': 'yellow',
    'logging.level.warning': 'bold yellow',
}

rich.reconfigure(
    width=shutil.get_terminal_size(fallback=(140, 24)).columns,  # fallback for running in cron,
    theme=rich.theme.Theme(RICH_THEME_KWARGS_DEFAULT),  # noqa
)

Table = partial(rich.table.Table, header_style='bold magenta')  # noqa


def redact_str(string: str, mask: str = '*', plaintext_suffix_length: int = 4) -> str:
    if len(string) <= plaintext_suffix_length:
        return mask * len(string)
    redacted = (mask if char not in ('-', ' ') else char for char in string[:-plaintext_suffix_length])
    return ''.join(redacted) + string[-plaintext_suffix_length:]


class MaskingFilter(logging.Filter):
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
            'click', 'requests', 'urllib3', 'paramiko', 'tenacity',
            # built-in
            'futures', 'concurrent',
        ]


class ShortRichHandler(_RichRichHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setFormatter(logging.Formatter(fmt='%(message)s', datefmt='%X', ))


class LongRichHandler(_RichRichHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setFormatter(logging.Formatter(fmt='%(message)s', datefmt='%Y-%m-%d %H:%M:%S.%s', ))


class PlainRichHandler(_BaseRichHandler):
    def emit(self, record):
        log_entry = self.format(record)
        self.console.print(log_entry)


SUPPORTED_LOGGERS = {
    'short': ShortRichHandler,
    'long': LongRichHandler,
    'plain': PlainRichHandler,
}


def name_to_handler(name: str) -> logging.StreamHandler:
    try:
        return SUPPORTED_LOGGERS[name]()
    except KeyError:
        raise ValueError(f'Unsupported logger name: {name}. Supported names are: {SUPPORTED_LOGGERS.keys()}')


@lru_cache()
def get_logger(name: str = None, level: str | int = logging.INFO) -> logging.Logger:
    """Retrieve or create a globally scoped logger based on the given name and level

    @param level: Logging level as a string or int (e.g. 'INFO' or logging.INFO)
    @param name: Name of the logger, which dictates its configuration.
    @return logger: Configured logger instance
    """
    if not name:
        name = 'short'
    logger = logging.getLogger(name)
    if name not in LOGGERS:
        LOGGERS.add(name)
        logger.addHandler(name_to_handler(name))
        logger.setLevel(logging.getLevelName(level) if isinstance(level, int) else level.upper())

    return logger
