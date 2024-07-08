from __future__ import annotations

from typing import Iterable, Mapping


def iter_to_case(input_: Iterable, target_case: str = 'UPPER_SNAKE_CASE',
                 skip_prefixes: str | list | None = None) -> Iterable | Mapping:
    """
    Converts all keys in the dictionaries of a list to a specified case

    @param input_: The iterable to be converted
    @param target_case: The target case for the dictionary keys, either "UPPER_SNAKE_CASE" or "camelBackCase"
    @param skip_prefixes: skip converting trees, the keys of which start with any of these prefixes
    @return: The converted iterable
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


def camel_to_upper_snake_case(value):
    """Convert CamelCase/camelBack to UPPER_SNAKE_CASE"""
    new_key = ''
    for i, letter in enumerate(value):
        if i > 0 and letter.isupper() and not value[i - 1].isupper():
            new_key += '_'
        new_key += letter.upper()
    return new_key


def snake_to_camel_back_case(value):
    """Convert SNAKE_CASE to camelBackCase"""
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
    if len(string) <= max_len:
        return string
    upto = (max_len + hash_len + len(delimiter)) // 2
    from_ = (max_len - hash_len - len(delimiter)) // 2
    return truncate(string[:upto + 1], upto, hash_len, delimiter) + string[-from_:]
