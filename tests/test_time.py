from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from basepak.time import (
    DEFAULT_FORMAT,
    create_timestamp,
    sleep,
    str_to_mmin,
    str_to_seconds,
    str_to_timedelta,
    strptime,
)


@pytest.mark.parametrize('time_str, expected_timedelta', [
    ('1w', timedelta(weeks=1)),
    ('2days', timedelta(days=2)),
    ('3H', timedelta(hours=3)),
    ('4minute', timedelta(minutes=4)),
    ('5 seconds', timedelta(seconds=5)),
    ('1w2d3h4m5s', timedelta(weeks=1, days=2, hours=3, minutes=4, seconds=5)),
    ('7d5h', timedelta(days=7, hours=5)),
    ('15m30s', timedelta(minutes=15, seconds=30)),
    ('', timedelta()),
    ('0s', timedelta()),
])
def test_str_to_timedelta_valid(time_str, expected_timedelta):
    assert str_to_timedelta(time_str) == expected_timedelta

def test_str_to_timedelta_no_matches():
    with pytest.raises(ValueError):
        str_to_timedelta('no time units')

def test_str_to_timedelta_large_values():
    assert str_to_timedelta('1000d') == timedelta(days=1000)

@pytest.mark.parametrize('time_str, expected_minutes', [
    ('1h', 60),
    ('90m', 90),
    ('2h30m', 150),
    ('45s', 0),  # Less than a minute rounds down
    ('1h15s', 60),  # 1 hour and 15 seconds
    ('1d', 1440),  # 24 * 60
    ('', 0),
])
def test_str_to_mmin(time_str, expected_minutes):
    assert str_to_mmin(time_str) == expected_minutes

@pytest.mark.parametrize('time_str, expected_seconds', [
    ('1h', 3600),
    ('90m', 5400),
    ('2h30m', 9000),
    ('45s', 45),
    ('1h15s', 3615),
    ('1d', 86400),
    ('', 0),
])
def test_str_to_seconds(time_str, expected_seconds):
    assert str_to_seconds(time_str) == expected_seconds

def test_strptime_default_format():
    date_string = '2023-09-15t13-45-30'
    expected_datetime = datetime(2023, 9, 15, 13, 45, 30)
    assert strptime(date_string) == expected_datetime

def test_strptime_custom_format():
    date_string = '15/09/2023 13:45:30'
    date_format = '%d/%m/%Y %H:%M:%S'
    expected_datetime = datetime(2023, 9, 15, 13, 45, 30)
    assert strptime(date_string, date_format) == expected_datetime

def test_strptime_invalid_format():
    with pytest.raises(ValueError):
        strptime('invalid date string')

def test_create_timestamp_default_format():
    timestamp = create_timestamp()
    # Check if the timestamp matches the default format
    datetime.strptime(timestamp, DEFAULT_FORMAT)  # Should not raise an exception

def test_create_timestamp_custom_format():
    format_ = '%Y/%m/%d %H:%M:%S'
    timestamp = create_timestamp(format_)
    # Check if the timestamp matches the custom format
    datetime.strptime(timestamp, format_)  # Should not raise an exception

@pytest.mark.parametrize('zero_time', [0, -5])
def test_sleep_zero(zero_time):
    with patch('time.sleep') as mock_sleep:
        sleep(zero_time)
        mock_sleep.assert_called_once_with(0)
