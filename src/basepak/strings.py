from __future__ import annotations

import re
from collections.abc import Iterable, Mapping


def iter_to_case(input_: Iterable, target_case: str = 'UPPER_SNAKE_CASE',
                 skip_prefixes: str | list | None = None) -> Iterable | Mapping:
    """Convert all keys in the dictionaries of a list to a specified case.
    Very useful for converting k8s case convention yaml data to python case convention dict and back
    :param input_: The iterable to be converted
    :param target_case: The target case for the dictionary keys, either "UPPER_SNAKE_CASE" or "camelBackCase"
    :param skip_prefixes: skip converting trees, the keys of which start with any of these prefixes
    :return: The converted iterable
    :raises NotImplementedError: if the target case is not implemented
    """
    output_dict = {}
    if isinstance(input_, (str, int, float, bool)):
        return input_
    if isinstance(skip_prefixes, str):
        skip_prefixes = [skip_prefixes]
    if isinstance(input_, Mapping):  # dict, OrderedDict, etc
        for key, value in input_.items():
            if skip_prefixes and any(key.startswith(prefix) for prefix in skip_prefixes):
                output_dict[key] = value
                continue
            if isinstance(value, Iterable) and not isinstance(value, str):
                value = iter_to_case(value, target_case, skip_prefixes)

            if target_case == 'UPPER_SNAKE_CASE':
                new_key = camel_to_upper_snake_case(key)
            elif target_case == 'camelBackCase':
                new_key = snake_to_camel_back_case(key)
            else:
                raise NotImplementedError(f'Requested case {target_case} not implemented yet')

            output_dict[new_key] = value

    elif isinstance(input_, Iterable):  # list, tuple, etc
        return type(input_)(iter_to_case(item, target_case, skip_prefixes) for item in input_)  # type: ignore

    return output_dict


def camel_to_upper_snake_case(value: str) -> str:
    """Convert CamelCase/camelBack to UPPER_SNAKE_CASE
    :param value: string to convert
    :return: converted string
    """
    # Insert underscore between lowercase and uppercase letters
    s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', value)
    # Insert underscore between sequences of uppercase letters and the following lowercase letters
    s2 = re.sub('([A-Z]+)([A-Z][a-z0-9])', r'\1_\2', s1)
    return s2.upper()


def snake_to_camel_back_case(value: str) -> str:
    """Convert SNAKE_CASE to camelBackCase
    :param value: string to convert
    :return: converted string
    """
    new_key = ''
    capitalize_next = False
    for letter in value:
        if letter == '_':
            capitalize_next = True
        elif capitalize_next:
            new_key += letter.upper()
            capitalize_next = False
        else:
            new_key += letter.lower()
    return new_key


def truncate(string: str, max_len: int, hash_len: int = 4, suffix: str = '') -> str:
    """Truncate a string to a maximum length, adding a hash and suffix if necessary.
    Useful for creating unique names for resources with a maximum length
    :param string: string to truncate
    :param max_len: maximum length of the string
    :param hash_len: length of the hash to append
    :param suffix: suffix to append
    :return: truncated string
    """
    if len(string) <= max_len:
        return string
    import hashlib
    salt = hashlib.md5(string.encode(), usedforsecurity=False).hexdigest()[:hash_len]
    return string[:max_len - hash_len - len(suffix)] + salt + suffix


def truncate_middle(
        string: str,
        max_len: int = 63,  # k8s job name limit - 63 characters
        hash_len: int = 4,
        delimiter: str = '-'
) -> str:
    """Truncate a string to a maximum length, adding a hash and delimiter if necessary.
    Useful for creating unique names for resources with a maximum length, when you care more about the start and end
    bits of the name (e.g. k8s resources)
    :param string: string to truncate
    :param max_len: maximum length of the string
    :param hash_len: length of the hash to append
    :param delimiter: delimiter to part the middle of the string
    :return: truncated string
    """
    if len(string) <= max_len:
        return string
    upto = (max_len + hash_len + len(delimiter)) // 2
    from_ = (max_len - hash_len - len(delimiter)) // 2
    return truncate(string[:upto + 1], upto, hash_len, delimiter) + string[-from_:]


def split_on_first_letter(string: str) -> list[str]:
    """Split a string into two strings on the first occurrence of a letter
    :param string: string to split
    :return: list of two strings"""
    index = next((i for i, c in enumerate(string) if c.isalpha()), len(string))
    return [string[:index], string[index:]]


def clean_strings(string_list: list[str]) -> list[str]:
    """Split, strip and prune empties for a list of strings
    :param string_list: list of strings
    :return: cleaned list of strings
    """
    cleaned_list = []
    for string in string_list:
        parts = [part for part in string.split() if part]
        cleaned_list.extend(parts)
    return cleaned_list
