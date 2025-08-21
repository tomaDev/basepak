import collections
import pytest # noqa

from basepak.templates import recursive_has_pair


@pytest.mark.parametrize(
    "obj,key,value,expected",
    [
        # direct hit at top level
        ({"a": 1, "b": 2}, "a", 1, True),
        ({"a": 1, "b": 2}, "a", 2, False),

        # nested dicts
        ({"a": {"b": {"c": 3}}}, "c", 3, True),
        ({"a": {"b": {"c": 3}}}, "c", 4, False),

        # list of dicts
        ({"items": [{"x": 1}, {"y": 2}, {"z": 3}]}, "y", 2, True),
        ({"items": [{"x": 1}, {"y": 2}, {"z": 3}]}, "q", 9, False),

        # tuple support
        ({"t": ({"k": "v"}, {"m": "n"})}, "k", "v", True),

        # falsy values should be detected
        ({"a": 0}, "a", 0, True),
        ({"a": False}, "a", False, True),
        ({"a": ""}, "a", "", True),
        ({"a": None}, "a", None, True),

        # sequences nested deeper
        ({"a": [{"b": [{"c": 10}]}]}, "c", 10, True),

        # primitives / non-container
        (42, "a", 1, False),
        ("not a mapping", "a", 1, False),
    ],
)
def test_recursive_has_pair_basic(obj, key, value, expected):
    assert recursive_has_pair(obj, key, value) is expected


def test_does_not_descend_into_strings_or_bytes():
    obj = {"a": "target: 42", "b": b"target: 99"}
    # Ensure we are not treating characters/bytes as elements to descend into
    assert recursive_has_pair(obj, "target", 42) is False
    assert recursive_has_pair(obj, "target", 99) is False


def test_mapping_subclass_userdict():
    ud = collections.UserDict({"inner": {"k": "v"}})
    assert recursive_has_pair(ud, "k", "v") is True


def test_multiple_matches_returns_true_on_first_found():
    obj = {"a": {"k": 1}, "b": [{"k": 2}, {"k": 3}]}
    # Function is specified as "exists anywhere" (any-match). True if at least one match.
    assert recursive_has_pair(obj, "k", 2) is True
    assert recursive_has_pair(obj, "k", 999) is False


def test_equality_vs_identity():
    class Box:
        def __init__(self, x): self.x = x
        def __eq__(self, other): return isinstance(other, Box) and self.x == other.x

    obj = {"box": Box(5)}
    assert recursive_has_pair(obj, "box", Box(5)) is True
    assert recursive_has_pair(obj, "box", Box(6)) is False
