import logging
import os
import tempfile
from unittest.mock import MagicMock

import pytest

from basepak.tail import tail, validate_pattern


@pytest.fixture
def temp_file_with_content():
    temp_files = []
    def _create_temp_file(contents: str):
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp:
            tmp.write(contents)
            tmp.flush()
        temp_files.append(tmp.name)
        return tmp.name
    yield _create_temp_file
    for file_path in temp_files:  # Cleanup
        os.remove(file_path)

@pytest.mark.parametrize('contents, n, expected', [
    ('', 10, []),
    ('Line1\nLine2\nLine3\nLine4\nLine5\n', 10, ['Line1', 'Line2', 'Line3', 'Line4', 'Line5']),
    ('Single line without newline', 1, ['Single line without newline']),
])
def test_tail(temp_file_with_content, contents, n, expected):
    file_path = temp_file_with_content(contents)
    result = tail(file_path, n)
    assert result == expected

def test_tail_nonexistent_file():
    with pytest.raises(FileNotFoundError):
        tail('nonexistent_file.txt', 10)

@pytest.mark.parametrize('contents, pattern, expected', [
    ('Line1\nLine2\nLine3\nLine4\nLine5\n', 'Line3', True),
    ('Line1\nLine2\nLine3\nLine4\nLine5\n', 'NoMatch', False),
    ('', 'NoMatch', False),
    ('Single line without newline', 'newline', True),
])
def test_validate_pattern(temp_file_with_content, contents, pattern, expected):
    file_path = temp_file_with_content(contents)
    last_lines = 100
    logger = MagicMock(spec=logging.Logger)
    if expected:
        assert validate_pattern(file_path, pattern, logger)
        logger.info.assert_any_call(f'pattern "{pattern}" found')
    else:
        with pytest.raises(StopIteration):
            validate_pattern(file_path, pattern, logger, num_of_lines=last_lines)
    logger.info.assert_any_call(f'Tailing last {last_lines} lines of {file_path}')

def test_validate_pattern_nonexistent_file():
    logger = MagicMock(spec=logging.Logger)
    with pytest.raises(FileNotFoundError):
        validate_pattern('nonexistent_file.txt', 'pattern', logger)
