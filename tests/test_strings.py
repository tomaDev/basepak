# test_strings.py

import hashlib

import pytest

from basepak.strings import (
    camel_to_upper_snake_case,
    clean_strings,
    iter_to_case,
    snake_to_camel_back_case,
    split_on_first_letter,
    truncate,
    truncate_middle,
)


# Helper function for expected truncate output
def expected_truncate(string, max_len, hash_len=4, suffix=''):
    if len(string) <= max_len:
        return string
    salt = hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()[:hash_len]
    return string[:max_len - hash_len - len(suffix)] + salt + suffix

# Tests for camel_to_upper_snake_case
@pytest.mark.parametrize('input_str, expected_output', [
    ('camelCaseString', 'CAMEL_CASE_STRING'),
    ('CamelCaseString', 'CAMEL_CASE_STRING'),
    ('alreadyUpperSnake', 'ALREADY_UPPER_SNAKE'),
    ('lowercase', 'LOWERCASE'),
    ('MixedSnakeAndCamelCase', 'MIXED_SNAKE_AND_CAMEL_CASE'),
    ('HTTPResponseCode', 'HTTP_RESPONSE_CODE'),
    ('XMLHttpRequest', 'XML_HTTP_REQUEST'),
])
def test_camel_to_upper_snake_case(input_str, expected_output):
    assert camel_to_upper_snake_case(input_str) == expected_output

# Tests for snake_to_camel_back_case
@pytest.mark.parametrize('input_str, expected_output', [
    ('SNAKE_CASE_STRING', 'snakeCaseString'),
    ('upper_snake_case', 'upperSnakeCase'),
    ('MIXED_SNAKE_Case', 'mixedSnakeCase'),
    ('lowercase', 'lowercase'),
    ('HTTP_RESPONSE_CODE', 'httpResponseCode'),
])
def test_snake_to_camel_back_case(input_str, expected_output):
    assert snake_to_camel_back_case(input_str) == expected_output

# Tests for iter_to_case
def test_iter_to_case_upper_snake_case():
    input_data = {
        'camelCaseKey': 'value',
        'nestedDict': {
            'anotherKey': 'anotherValue',
            'listOfDicts': [
                {'listKeyOne': 1},
                {'listKeyTwo': 2}
            ]
        }
    }
    expected_output = {
        'CAMEL_CASE_KEY': 'value',
        'NESTED_DICT': {
            'ANOTHER_KEY': 'anotherValue',
            'LIST_OF_DICTS': [
                {'LIST_KEY_ONE': 1},
                {'LIST_KEY_TWO': 2}
            ]
        }
    }
    assert iter_to_case(input_data, target_case='UPPER_SNAKE_CASE') == expected_output

def test_iter_to_case_camel_back_case():
    input_data = {
        'UPPER_SNAKE_KEY': 'value',
        'NESTED_DICT': {
            'ANOTHER_KEY': 'anotherValue',
            'LIST_OF_DICTS': [
                {'LIST_KEY_ONE': 1},
                {'LIST_KEY_TWO': 2}
            ]
        }
    }
    expected_output = {
        'upperSnakeKey': 'value',
        'nestedDict': {
            'anotherKey': 'anotherValue',
            'listOfDicts': [
                {'listKeyOne': 1},
                {'listKeyTwo': 2}
            ]
        }
    }
    assert iter_to_case(input_data, source_case='UPPER_SNAKE_CASE', target_case='camelBackCase') == expected_output

def test_iter_to_case_skip_prefixes():
    input_data = {
        'skipThisKey': 'value',
        'convertThisKey': 'value',
        'nestedDict': {
            'skipThisNestedKey': 'nestedValue',
            'convertThisNestedKey': 'nestedValue'
        }
    }
    expected_output = {
        'skipThisKey': 'value',
        'CONVERT_THIS_KEY': 'value',
        'NESTED_DICT': {
            'skipThisNestedKey': 'nestedValue',
            'CONVERT_THIS_NESTED_KEY': 'nestedValue'
        }
    }
    assert iter_to_case(input_data, target_case='UPPER_SNAKE_CASE', skip_prefixes='skip') == expected_output

# Tests for truncate
@pytest.mark.parametrize('string, max_len, hash_len, suffix', [
    ('short string', 20, 4, ''),
    ('this is a very long string that needs to be truncated', 20, 4, ''),
    ('exact length string', 19, 4, ''),
    ('needs hashing', 10, 4, ''),
    ('another long string that requires truncation', 15, 4, ''),
    ('string with suffix', 15, 4, '_end'),
])
def test_truncate(string, max_len, hash_len, suffix):
    expected_output = expected_truncate(string, max_len, hash_len, suffix)
    assert truncate(string, max_len, hash_len, suffix) == expected_output

# Tests for truncate_middle
def test_truncate_middle():
    string = 'abcdefghijklmnopqrstuvwxyz' * 4  # length 104
    max_len = 30
    result = truncate_middle(string, max_len)
    assert len(result) <= max_len
    assert result.startswith(string[:10])
    assert result.endswith(string[-10:])
    middle_part = result[14:-8]
    assert '-' in middle_part

# Tests for split_on_first_letter
@pytest.mark.parametrize('input_str, expected_output', [
    ('123abc', ['123', 'abc']),
    ('abc123', ['', 'abc123']),
    ('123456', ['123456', '']),
    ('!@#$', ['!@#$', '']),
    ('abc', ['', 'abc']),
    ('', ['', '']),
])
def test_split_on_first_letter(input_str, expected_output):
    assert split_on_first_letter(input_str) == expected_output

# Tests for clean_strings
@pytest.mark.parametrize('input_list, expected_output', [
    (['   string   ', '  another   string ', ''], ['string', 'another', 'string']),
    (['one two', 'three  four', '  '], ['one', 'two', 'three', 'four']),
    (['  ', '', '\n'], []),
    (['no spaces', 'singleword'], ['no', 'spaces', 'singleword']),
])
def test_clean_strings(input_list, expected_output):
    assert clean_strings(input_list) == expected_output
