import functools
import re
import time
from datetime import datetime, timedelta
from typing import List, Optional

DEFAULT_FORMAT = '%Y-%m-%dt%H-%M-%S'

# list of lists vs dicts to allow key duplication if needed
SUPPORTED_TIME_NOTATION = [
    ['weeks', 'w'],
    ['days', 'd'],
    ['hours', 'h'],
    ['minutes', 'm'],
    ['seconds', 's'],
]


def _make_timedelta_pattern(patterns: List[List[str]]):
    return ''.join(fr'((?P<{keyword}>\d+? *){notation})?' for keyword, notation in patterns)


@functools.lru_cache()
def str_to_timedelta(time_str: str) -> timedelta:
    regex = re.compile(_make_timedelta_pattern(SUPPORTED_TIME_NOTATION))
    parts = regex.match(time_str)
    if not parts:
        raise 'regex parsing error. Was passed match object a string?'
    parts = parts.groupdict()
    time_params = {name: int(param) for name, param in parts.items() if param}
    return timedelta(**time_params)


def str_to_mmin(time_str: str) -> int:
    return int(str_to_timedelta(time_str).total_seconds() // 60)


def str_to_seconds(value: Optional[str] = None) -> int:
    if not value:
        return 0
    return int(str_to_timedelta(value).total_seconds())


def strptime(date_string: str, date_format: str = DEFAULT_FORMAT) -> datetime:
    return datetime.strptime(date_string, date_format)


def create_timestamp(format_: str = DEFAULT_FORMAT) -> str:
    return datetime.strftime(datetime.now(), format_)


def fromtimestamp(float_time: float) -> datetime:
    return datetime.fromtimestamp(float_time)


def sleep(seconds: float):
    if seconds < 0:
        seconds = 0
    time.sleep(seconds)
