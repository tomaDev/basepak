import logging

import pytest

from basepak.log import (
    LOGGERS,
    SUPPORTED_LOGGERS,
    MaskingFilter,
    get_logger,
    log_as,
    name_to_handler,
    redact_str,
)

REDACTION_TEST_DATA = [
    ("mysecretpassword", "************word"),
    ("short", "*hort"),
    ("test", "****"),  # strings shorter than plaintext_suffix_length should be fully redacted
    ("1234567890", "******7890"),
    ("--password=my_phrase", "--**************rase"),
    ("password: my_phrase", "********* *****rase"),
    ("No secrets here", "** ******* here"),
]

def test_redact_str():
    for original, expected in REDACTION_TEST_DATA:
        assert redact_str(original) == expected

def test_redact_str_custom_mask():
    original = "mysecretpassword"
    expected = "########password"
    assert redact_str(original, mask="#", plaintext_suffix_length=8) == expected

def test_masking_filter():
    filter_ = MaskingFilter()
    sensitive_message = "password: my_phrase --access-key=abc123 data-access-key=secret123"
    expected_message =  "password: ******** --access-key=******** data-access-key=********"
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg=sensitive_message,
        args=(),
        exc_info=None,
    )
    filter_.filter(record)
    assert record.msg == expected_message

def test_masking_filter_no_mask():
    filter_ = MaskingFilter()
    message = "This is a !safe message."
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg=message,
        args=(),
        exc_info=None,
    )
    filter_.filter(record)
    assert record.msg == message

def test_name_to_handler():
    for name, handler_class in SUPPORTED_LOGGERS.items():
        handler = name_to_handler(name)
        assert isinstance(handler, handler_class)

    with pytest.raises(ValueError) as _:
        name_to_handler("unsupported")

def test_singleton_per_name():
    LOGGERS.discard('short')
    logger = get_logger('short')
    assert logger is get_logger('short')

    LOGGERS.discard('long')
    assert logger is not get_logger('long')

def test_set_logger_level():
    LOGGERS.discard('long')
    logger = get_logger('long', level='DEBUG')
    assert logger.level == logging.DEBUG

def test_date_time_encoder():
    import datetime

    from basepak.log import DateTimeEncoder
    now = datetime.datetime.now()
    import json

    data = {"timestamp": now}
    json_str = json.dumps(data, cls=DateTimeEncoder)
    assert json_str == f'{{"timestamp": "{now.isoformat()}"}}'

def test_log_as_json(caplog):
    logger = get_logger("plain")
    data = {"key": "value", "number": 123}
    with caplog.at_level(logging.INFO):
        log_as("json", data, printer=logger.info)
        assert '"key": "value"' in caplog.text
        assert '"number": 123' in caplog.text

def test_log_as_yaml(caplog):
    logger = get_logger("plain")
    data = {"key": "value", "number": 123}
    with caplog.at_level(logging.INFO):
        log_as("yaml", data, printer=logger.info)
        assert "key: value" in caplog.text
        assert "number: 123" in caplog.text

def test_log_as_invalid_syntax():
    with pytest.raises(NotImplementedError):
        log_as("xml", data={"key": "value"})

def test_log_as_with_string_data(caplog):
    logger = get_logger("plain")
    data = '{"key": "value", "number": 123}'
    with caplog.at_level(logging.INFO):
        log_as("json", data, printer=logger.info)
        assert '"key": "value"' in caplog.text
        assert '"number": 123' in caplog.text

def test_log_as_with_no_data(caplog):
    logger = get_logger("plain")
    with caplog.at_level(logging.INFO):
        log_as("json", data=None, printer=logger.info)
        assert caplog.text == ""

def test_log_as_yaml_flow_style(caplog):
    logger = get_logger("plain")
    data = {"key": "value", "number": 123}
    with caplog.at_level(logging.INFO):
        log_as("yaml", data, printer=logger.info, yaml_default_flow_style=False)
        assert "key: value" in caplog.text
        assert "number: 123" in caplog.text

def test_rich_handlers():
    for name in SUPPORTED_LOGGERS.keys():
        logger = get_logger(name)
        assert logger.handlers
        for handler in logger.handlers:
            assert isinstance(handler, logging.Handler)

def test_plain_rich_handler_emit(capsys):
    logger = get_logger("plain")
    logger.info("Test message")
    captured = capsys.readouterr()
    assert "Test message" in captured.out

def test_table_partial():
    from basepak.log import Table
    table = Table()
    assert table.header_style == "bold magenta"

def test_log_as_with_mapping_data(caplog):
    logger = get_logger("plain")
    data = {"key": "value", "number": 123}
    with caplog.at_level(logging.INFO):
        log_as("json", data=data, printer=logger.info)
        assert '"key": "value"' in caplog.text
        assert '"number": 123' in caplog.text

def test_get_logger_caching():
    logger1 = get_logger("plain")
    logger2 = get_logger("plain")
    assert logger1 is logger2
