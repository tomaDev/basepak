from . import batch_job, daemonset, persistent_volume_claim

from collections.abc import Mapping, Sequence
from typing import Any, Hashable

def recursive_has_pair(obj: Any, key: Hashable, value: Any) -> bool:
    """
    Recursively check whether a key:value pair exists in a nested
    structure of dicts/lists/tuples.

    :param obj: The structure to search in.
    :param key: The key to look for.
    :param value: The value to match.
    :return: True if the key:value pair exists anywhere, else False.
    """
    if isinstance(obj, Mapping):
        if key in obj and obj[key] == value:
            return True
        return any(recursive_has_pair(v, key, value) for v in obj.values())

    elif isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
        return any(recursive_has_pair(item, key, value) for item in obj)

    return False
