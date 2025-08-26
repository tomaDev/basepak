from unittest.mock import MagicMock

import click
import pytest

from basepak.units import Range, Ranges, Unit


def test_unit_initialization():
    unit = Unit("1024 B")
    assert unit.value == 1024
    assert unit.unit == "B"

    unit = Unit("1 KiB")
    assert unit.value == 1
    assert unit.unit == "KiB"

    unit = Unit("1.5 MB")
    assert unit.value == 1.5
    assert unit.unit == "MB"

    with pytest.raises(ValueError):
        Unit("InvalidInput")


def test_unit_conversion():
    unit = Unit("1024 B")
    assert unit.convert_to("KiB") == 1.0
    assert unit.convert_to("KB") == 1.024

    unit = Unit("1 MiB")
    assert unit.convert_to("KiB") == 1024
    assert unit.convert_to("MB") == 1.048576

    with pytest.raises(ValueError):
        unit.convert_to("UnsupportedUnit")


def test_unit_adjustment():
    unit = Unit("1024 B")
    adjusted = unit.adjust_unit()
    assert adjusted.value == 1.0
    assert adjusted.unit == "KiB"

    unit = Unit("1024 KiB")
    adjusted = unit.adjust_unit()
    assert adjusted.value == 1.0
    assert adjusted.unit == "MiB"

    unit = Unit("1000 B")
    adjusted = unit.adjust_unit()
    assert adjusted.value == 1000
    assert adjusted.unit == "B"

    unit = Unit("0 B")
    adjusted = unit.adjust_unit()
    assert adjusted.value == 0.0
    assert adjusted.unit == "B"


def test_unit_arithmetic_operations():
    unit1 = Unit("1 KiB")
    unit2 = Unit("1 KiB")

    result = unit1 + unit2
    assert result.value == 2.0
    assert result.unit == "KiB"

    result = unit1 - Unit("512 B")
    assert result.value == 512
    assert result.unit == "B"

    result = unit1 * 2
    assert result.value == 2.0
    assert result.unit == "KiB"

    result = unit1 * 10 / 2
    assert result.value == 5
    assert result.unit == "KiB"

    with pytest.raises(ZeroDivisionError):
        unit1 / Unit("0 B")


def test_unit_comparison():
    unit1 = Unit("1 KiB")
    unit2 = Unit("1024 B")

    assert unit1 == unit2
    assert unit1 >= unit2
    assert unit1 <= unit2

    assert unit1 > Unit("512 B")
    assert unit1 < Unit("2 KiB")


def test_unit_reduce():
    units = [Unit("1024 B"), Unit("1 KiB"), Unit("512 B")]
    result = Unit.reduce(units)
    assert result.value == 2.5
    assert result.unit == "KiB"

    result = Unit.reduce(units, unit="MB")
    assert result.value == 0.00256
    assert result.unit == "MB"


def test_unit_iterable_to_unit():
    units = ["1024 B", "1 KiB", "512 B"]
    result = Unit.iterable_to_unit(units, unit="KiB")
    assert result.value == 2.5
    assert result.unit == "KiB"


def test_unit_invalid_operations():
    unit = Unit("1 KiB")

    with pytest.raises(ValueError):
        unit + "InvalidInput"

    with pytest.raises(ValueError):
        unit - Unit("UnsupportedUnit")

@pytest.mark.parametrize('spaces', ['', ' ', '  '])
def test_unit_as_unit(spaces):
    unit = Unit(f'1024{spaces}B')
    assert unit.as_unit("KiB") == "1KiB"
    assert unit.as_unit("auto").strip() == "1.00KiB"

@pytest.fixture
def mock_param():
    return MagicMock(spec=click.Parameter)


@pytest.fixture
def mock_ctx():
    return MagicMock(spec=click.Context)


# Tests for Range
def test_range_single_value(mock_param, mock_ctx):
    range_type = Range()
    result = range_type.convert('5', mock_param, mock_ctx)
    assert result == range(5, 6)


@pytest.mark.parametrize('sep', [':', '-'])
def test_range_valid_full(sep, mock_param, mock_ctx):
    range_type = Range()
    result = range_type.convert(f'1{sep}10', mock_param, mock_ctx)
    assert result == range(1, 10)


@pytest.mark.parametrize('sep', [':', '-'])
def test_range_invalid_format(sep, mock_param, mock_ctx):
    range_type = Range()
    with pytest.raises(Exception):
        range_type.convert(f'1{sep}2{sep}3', mock_param, mock_ctx)


@pytest.mark.parametrize('sep', [':', '-'])
def test_ranges_single_range(sep, mock_param, mock_ctx):
    ranges_type = Ranges()
    result = ranges_type.convert(f'1{sep}5', mock_param, mock_ctx)
    assert result == tuple([range(1, 5)])


@pytest.mark.parametrize('sep1', [':', '-'])
@pytest.mark.parametrize('sep2', [':', '-'])
def test_ranges_valid(sep1, sep2, mock_param, mock_ctx):
    ranges_type = Ranges()
    result = ranges_type.convert(f'1{sep1}5,6{sep2}10', mock_param, mock_ctx)
    assert result == tuple([range(1, 5), range(6, 10)])

def test_ranges_range_with_single(mock_param, mock_ctx):
    ranges_type = Ranges()
    result = ranges_type.convert('1-5,6,10', mock_param, mock_ctx)
    assert result == tuple([range(1, 5), range(6,7), range(10,11)])

def test_ranges_is_cachable():
    from functools import lru_cache

    ranges = Ranges().convert('1-5,6,10', None, None)  # Unexpected type

    try:
        hash(ranges)
    except TypeError:
        assert False, "Ranges object is not hashable"
    @lru_cache
    def test(_):
        return 1
    try:
        test(ranges)
    except Exception as e:
        assert False, f"Ranges object may not be cachable. Exception raised: {e}"
