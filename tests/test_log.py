import logging
import os
import tempfile

import pytest
from rich.table import Table

from basepak.log import (
    LOGGERS,
    SUPPORTED_LOGGERS,
    MaskingFilter,
    get_logger,
    log_as,
    name_to_handler,
    print_table,
    redact_file,
    redact_str,
)

REDACTION_TEST_DATA = [
    ('mysecretpassword', '************word'),
    ('short', '*hort'),
    ('test', '****'),  # strings shorter than plaintext_suffix_length should be fully redacted
    ('1234567890', '******7890'),
    ('--password=my_phrase', '--**************rase'),
    ('password: my_phrase', '********* *****rase'),
    ('No secrets here', '** ******* here'),
]

def test_redact_str():
    for original, expected in REDACTION_TEST_DATA:
        assert redact_str(original) == expected

def test_redact_str_custom_mask():
    original = 'mysecretpassword'
    expected = '########password'
    assert redact_str(original, mask='#', plaintext_suffix_length=8) == expected

def test_masking_filter():
    filter_ = MaskingFilter()
    sensitive_message = 'password: my_phrase --access-key=abc123 data-access-key=secret123'
    expected_message =  'password: ******** --access-key=******** data-access-key=********'
    record = logging.LogRecord(
        name='test',
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
    message = 'This is a !safe message.'
    record = logging.LogRecord(
        name='test',
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

    data = {'timestamp': now}
    json_str = json.dumps(data, cls=DateTimeEncoder)
    assert json_str == f'{{"timestamp": "{now.isoformat()}"}}'

def test_log_as_json(caplog):
    logger = get_logger('plain')
    data = {'key': 'value', 'number': 123}
    with caplog.at_level(logging.INFO):
        log_as('json', data, printer=logger.info)
        assert '"key": "value"' in caplog.text
        assert '"number": 123' in caplog.text

def test_log_as_yaml(caplog):
    logger = get_logger('plain')
    data = {'key': 'value', 'number': 123}
    with caplog.at_level(logging.INFO):
        log_as('yaml', data, printer=logger.info)
        assert 'key: value' in caplog.text
        assert 'number: 123' in caplog.text

def test_log_as_invalid_syntax():
    with pytest.raises(NotImplementedError):
        log_as('xml', data={'key': 'value'})

def test_log_as_with_string_data(caplog):
    logger = get_logger('plain')
    data = '{"key": "value", "number": 123}'
    with caplog.at_level(logging.INFO):
        log_as('json', data, printer=logger.info)
        assert '"key": "value"' in caplog.text
        assert '"number": 123' in caplog.text

def test_log_as_with_no_data(caplog):
    logger = get_logger('plain')
    with caplog.at_level(logging.INFO):
        log_as('json', data=None, printer=logger.info)
        assert caplog.text == ''

def test_log_as_yaml_flow_style(caplog):
    logger = get_logger('plain')
    data = {'key': 'value', 'number': 123}
    with caplog.at_level(logging.INFO):
        log_as('yaml', data, printer=logger.info, yaml_default_flow_style=False)
        assert 'key: value' in caplog.text
        assert 'number: 123' in caplog.text

def test_rich_handlers():
    for name in SUPPORTED_LOGGERS.keys():
        logger = get_logger(name)
        assert logger.handlers
        for handler in logger.handlers:
            assert isinstance(handler, logging.Handler)

def test_plain_rich_handler_emit(capsys):
    logger = get_logger('plain')
    logger.info('Test message')
    captured = capsys.readouterr()
    assert 'Test message' in captured.out

def test_table_partial():
    from basepak.log import Table
    table = Table()
    assert table.header_style == 'bold magenta'

def test_log_as_with_mapping_data(caplog):
    logger = get_logger('plain')
    data = {'key': 'value', 'number': 123}
    with caplog.at_level(logging.INFO):
        log_as('json', data=data, printer=logger.info)
        assert '"key": "value"' in caplog.text
        assert '"number": 123' in caplog.text

def test_get_logger_is_monad():
    logger1 = get_logger('plain')
    logger2 = get_logger('plain')
    assert logger1 is logger2


@pytest.fixture
def create_tempfile():
    """
    Pytest fixture that yields a path to a writable temporary file.
    We remove the file after the test is done.
    """
    fd, file_path = tempfile.mkstemp(suffix='.txt')
    os.close(fd)  # We only need the path; close the low-level file descriptor.
    yield file_path
    # Cleanup after the test
    if os.path.exists(file_path):
        os.remove(file_path)

def test_redact_file_password(create_tempfile):
    """
    Test that 'password = super_secret' is replaced with 'password = ******'
    using default keys (SECRET_KEYWORD_FLAGS).
    """
    file_path = create_tempfile

    original_content = """some_key = foo
password = super_secret
not_sensitive = remain
password plainvalue
"""
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(original_content)

    # Act: Redact the file
    redact_file(file_path)  # uses default keys => includes "password"

    # Assert
    with open(file_path, encoding='utf-8') as f:
        result = f.read()

    assert "super_secret" not in result
    # Make sure other lines are unchanged
    assert "some_key = foo" in result


def test_redact_file_user_replacement(create_tempfile):
    """Test that '--user root' or 'user = admin' style strings get redacted when 'user' is included in the keys"""
    file_path = create_tempfile

    original_content = """--user root
username = not_the_same_key
user = admin
"""
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(original_content)

    # Custom keys (you could also rely on SECRET_KEYWORD_FLAGS if it has 'user')
    keys = ["user"]
    redact_file(file_path, keys=keys)

    with open(file_path, encoding="utf-8") as f:
        result = f.read()

    assert 'root' not in result
    assert 'admin' not in result
    # Should not have replaced 'username = not_the_same_key'
    assert "username = not_the_same_key" in result


def test_redact_file_custom_key_pattern(create_tempfile):
    """
    Demonstrate that a custom key not in the default flags can be redacted.
    """
    file_path = create_tempfile

    original_content = """my_key = top-secret
password = my_password
"""
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(original_content)

    # We only want to redact 'my_key'
    keys = ["my_key"]
    redact_file(file_path, keys=keys)

    with open(file_path, encoding="utf-8") as f:
        result = f.read()

    # my_key value should be replaced
    assert 'top-secret' not in result
    # password line remains unchanged since we didn't provide 'password' in keys
    assert "password = my_password" in result


def test_redact_file_empty(create_tempfile):
    """
    Test that redacting an empty file does not fail and remains empty.
    """
    file_path = create_tempfile
    # Create an empty file
    open(file_path, "w").close()

    # Try redacting
    redact_file(file_path)

    # Should still be empty
    with open(file_path, encoding="utf-8") as f:
        result = f.read()

    assert result == ""


def clear_existing_loggers():
    """Clear the logging configuration for testing."""
    # Remove handlers from the root logger
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    # Clear all handlers from all existing loggers
    for logger_name in list(logging.root.manager.loggerDict):
        logger = logging.getLogger(logger_name)
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            logger.level = logging.NOTSET

    # Clear our module-level cache
    LOGGERS.clear()


def test_write_log_to_file(tmp_path, monkeypatch):
    # Set up a temporary log file environment
    clear_existing_loggers()
    log_file_name = "test.log"
    log_dir = tmp_path / "logs"
    log_file_path = str(log_dir / log_file_name)

    monkeypatch.setenv("BASEPAK_WRITE_LOG_TO_FILE", "True")
    monkeypatch.setenv("BASEPAK_LOG_FILE", log_file_name)
    monkeypatch.setenv("BASEPAK_LOG_PATH", log_file_path)

    logger = get_logger("short", level='debug')
    logger.info("Test message")

    with open(log_file_path, encoding="utf-8") as f:
        content = f.read()

    assert "Test message" in content

def test_no_write_log_to_file(tmp_path, monkeypatch):
    assert not os.environ.get('BASEPAK_WRITE_LOG_TO_FILE')
    clear_existing_loggers()
    log_file_name = 'test.log'
    log_dir = tmp_path / 'logs'
    log_file_path = str(log_dir / log_file_name)

    assert not os.path.exists(log_file_path)

    monkeypatch.setenv('BASEPAK_LOG_PATH', log_file_path)

    logger = get_logger('short', level='debug')
    logger.info('Test message')

    assert not os.path.exists(log_file_path)


def test_multiple_loggers_write_to_same_file(tmp_path, monkeypatch):
    clear_existing_loggers()
    log_file_name = 'test.log'
    log_dir = tmp_path / 'logs'
    log_file_path = str(log_dir / log_file_name)

    monkeypatch.setenv('BASEPAK_LOG_FILE', log_file_name)
    monkeypatch.setenv('BASEPAK_LOG_PATH', log_file_path)
    monkeypatch.setenv('BASEPAK_WRITE_LOG_TO_FILE', 'True')

    logger1 = get_logger('short', level='debug')
    logger2 = get_logger('long')
    logger3 = get_logger('plain', level='info')
    logger1.info('Logger1 message')
    logger2.info('Logger2 message')
    logger3.info('Logger3 message')

    with open(log_file_path, encoding='utf-8') as f:
        content = f.readlines()
    print(content)

    assert len(content) == 3, content
    assert 'Logger1 message' in content[0]
    assert 'Logger2 message' in content[1]
    assert 'Logger3 message' in content[2]


def test_uncaught_exception_writes_to_file(tmp_path, monkeypatch):
    import subprocess
    import textwrap
    clear_existing_loggers()

    log_file_name = 'test.log'
    log_dir = tmp_path / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file_path = str(log_dir / log_file_name)

    monkeypatch.setenv('BASEPAK_LOG_FILE', log_file_name)
    monkeypatch.setenv('BASEPAK_LOG_PATH', log_file_path)
    monkeypatch.setenv('BASEPAK_WRITE_LOG_TO_FILE', 'True')

    error_msg = 'Test exception'
    error_type = 'ValueError'
    code = textwrap.dedent(f"""
        from basepak.log import get_logger
        logger = get_logger()
        raise {error_type}('{error_msg}')
    """)

    result = subprocess.run(['python3', '-c', code], capture_output=True, env=os.environ)
    assert result.returncode != 0

    assert error_msg in result.stdout.decode()
    assert error_type in result.stdout.decode()
    with open(log_file_path, encoding='utf-8') as f:
        content = f.read()
        assert error_msg in content
        assert error_type in content

@pytest.fixture
def _table():
    table = Table('Task', 'Status', 'Notes', header_style='bold magenta')
    table.add_row('Task1', 'Success', 'Test message')
    table.add_row('Task2', 'Failure', 'Note2')
    return table


def test_write_table_to_file(tmp_path, monkeypatch, _table):
    log_file_name = 'test.log'
    log_dir = tmp_path / 'logs'
    log_file_path = str(log_dir / log_file_name)

    monkeypatch.setenv('BASEPAK_LOG_FILE', log_file_name)
    monkeypatch.setenv('BASEPAK_LOG_PATH', log_file_path)
    monkeypatch.setenv('BASEPAK_WRITE_LOG_TO_FILE', 'True')

    print_table(_table)

    with open(log_file_path, encoding='utf-8') as f:
        content = f.read()

    # Debug output (can be removed)
    print('Logged content:')
    print(content)

    assert 'Task' in content
    assert 'Success' in content
    assert 'Failure' in content

    import re
    ansi_escape = re.compile(r'\x1b\[') # Verify that ANSI escape sequences are present,
    assert ansi_escape.search(content), 'No ANSI escape codes found in the log output!'


def test_no_write_table_to_file(tmp_path, monkeypatch, _table):
    assert not os.environ.get('BASEPAK_WRITE_LOG_TO_FILE')
    log_file_name = 'test.log'
    log_dir = tmp_path / 'logs'
    log_file_path = str(log_dir / log_file_name)

    monkeypatch.setenv('BASEPAK_LOG_FILE', log_file_name)
    monkeypatch.setenv('BASEPAK_LOG_PATH', log_file_path)

    print_table(_table)

    assert not os.path.exists(log_file_path)

def test_write_logger_no_color(tmp_path, monkeypatch):
    clear_existing_loggers()
    log_file_name = 'test.log'
    log_dir = tmp_path / 'logs'
    log_file_path = str(log_dir / log_file_name)

    monkeypatch.setenv('BASEPAK_LOG_FILE', log_file_name)
    monkeypatch.setenv('BASEPAK_LOG_PATH', log_file_path)
    monkeypatch.setenv('BASEPAK_WRITE_LOG_TO_FILE', '1')
    monkeypatch.setenv('NO_COLOR', '1')

    logger = get_logger('short')
    logger.info('Test message')
    with open(log_file_path, encoding='utf-8') as f:
        content = f.read()

    print(content)

    import re
    ansi_escape = re.compile(r'\x1b\[')
    assert 'Test message' in content
    assert not ansi_escape.search(content)

def test_write_table_no_color(tmp_path, monkeypatch, _table):
    clear_existing_loggers()
    log_file_name = 'test.log'
    log_dir = tmp_path / 'logs'
    log_file_path = str(log_dir / log_file_name)

    monkeypatch.setenv('BASEPAK_LOG_FILE', log_file_name)
    monkeypatch.setenv('BASEPAK_LOG_PATH', log_file_path)
    monkeypatch.setenv('BASEPAK_WRITE_LOG_TO_FILE', '1')
    monkeypatch.setenv('NO_COLOR', '1')

    print_table(_table)

    with open(log_file_path, encoding='utf-8') as f:
        content = f.read()

    import re
    ansi_escape = re.compile(r'\x1b\[')
    assert 'Test message' in content
    assert not ansi_escape.search(content)
