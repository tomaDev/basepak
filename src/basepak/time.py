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


def _make_timedelta_pattern(patterns: List[List[str]]) -> str:
    return ''.join(fr'((?P<{keyword}>\d+? *){notation})?' for keyword, notation in patterns)


@functools.lru_cache
def str_to_timedelta(time_str: str) -> timedelta:
    """Convert a string to a timedelta object
    :param time_str: string representation of time
    :return: timedelta object
    :raises ValueError: if parsing time_str fails
    """
    if not isinstance(time_str, str):
        raise TypeError(f'time_str - expected str, got {type(time_str)}')
    time_str = time_str.lower()
    if time_str in ('', '0'):
        return timedelta()
    regex = re.compile(_make_timedelta_pattern(SUPPORTED_TIME_NOTATION))
    parts = regex.match(time_str)
    if not parts:
        raise ValueError(f'Parsing error for {time_str=}')
    parts = parts.groupdict()
    time_params = {name: int(param) for name, param in parts.items() if param}
    if not time_params:
        raise ValueError(f'No valid time units found in {time_str=}')
    return timedelta(**time_params)


def str_to_mmin(time_str: str) -> int:
    """Convert a string to minutes
    :param time_str: string representation of time
    :return: minutes
    """
    return int(str_to_timedelta(time_str).total_seconds() // 60)


def str_to_seconds(value: Optional[str] = None) -> int:
    """Convert a string to seconds
    :param value: string representation of time
    :return: seconds
    """
    if not value:
        return 0
    return int(str_to_timedelta(value).total_seconds())


def strptime(date_string: str, date_format: Optional[str] = DEFAULT_FORMAT) -> datetime:
    """datetime.strptime wrapper with default format
    :param date_string: date string
    :param date_format: date format
    :return: datetime object
    """
    return datetime.strptime(date_string, date_format)


def create_timestamp(format_: Optional[str] = DEFAULT_FORMAT) -> str:
    """Create a timestamp string
    :param format_: format string
    :return: timestamp string in the specified format
    """
    return datetime.strftime(datetime.now(), format_)


def fromtimestamp(float_time: float) -> datetime:
    """datetime.fromtimestamp wrapper
    :param float_time: float representation of time
    :return: datetime object
    """
    return datetime.fromtimestamp(float_time)


def sleep(seconds: float) -> None:
    """time.sleep wrapper that handles negative values
    :param seconds: seconds to sleep
    """
    if seconds < 0:
        seconds = 0
    time.sleep(seconds)
