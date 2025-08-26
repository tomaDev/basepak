from __future__ import annotations

import ipaddress
from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Callable, Optional, Union

import click

from . import strings


@total_ordering
@dataclass
class Unit:
    """Small hand-rolled size unit class"""
    # The Pint library is a robust framework for units - at 1.5MiB with a few extra deps, it's not needed for now
    _input_string: str = field(repr=False)
    value: float = field(init=False, compare=True)
    unit: str = field(init=False)

    ZERO_UNIT = '0 B'
    _UNIT_FACTORS_KILO = {
        'B':  1,    # Bytes only, no bits
        'KB': 1000,
        'MB': 1000 ** 2,
        'GB': 1000 ** 3,
        'TB': 1000 ** 4,
        'PB': 1000 ** 5,
    }
    _UNIT_FACTORS_KIBI = {
        'B':   1,    # Bytes only, no bits
        'K':   1024,
        'Ki':  1024,
        'KiB': 1024,
        'M':   1024 ** 2,
        'Mi':  1024 ** 2,
        'MiB': 1024 ** 2,
        'G':   1024 ** 3,
        'Gi':  1024 ** 3,
        'GiB': 1024 ** 3,
        'T':   1024 ** 4,
        'Ti':  1024 ** 4,
        'TiB': 1024 ** 4,
        'P':   1024 ** 5,
        'Pi':  1024 ** 5,
        'PiB': 1024 ** 5,
    }

    UNIT_FACTORS = {**_UNIT_FACTORS_KILO, **_UNIT_FACTORS_KIBI, }

    def __post_init__(self) -> None:
        input_list = strings.split_on_first_letter(self._input_string)
        input_stripped = strings.clean_strings(input_list)
        if len(input_stripped) != 2:
            raise ValueError(f'Constructor format: Number[ ]?Unit\nGot: {self._input_string}')
        self.value, self.unit = input_stripped
        self.value = float(self.value)

    def convert_to(self, unit: str) -> float:
        """Convert the instance value to the given unit
        :param unit: the unit to convert to
        :return: converted value
        """
        if unit not in self.UNIT_FACTORS:
            raise ValueError(f'Unsupported units for conversion: {unit}\n'
                             f'Options: {self.UNIT_FACTORS.keys()}')
        if unit == self.unit:
            return self.value
        value_in_bytes = self.value * self.UNIT_FACTORS[self.unit]
        return value_in_bytes / self.UNIT_FACTORS[unit]

    def adjust_unit(self) -> Unit:
        """Adjust to the most human-readable form. Preferred scale - Kibibytes
        :return: adjusted unit
        """
        if self.value == 0:
            return Unit(self.ZERO_UNIT)
        value_in_bytes = self.value * self.UNIT_FACTORS[self.unit]
        unit_factors = self._UNIT_FACTORS_KIBI if self.unit in self._UNIT_FACTORS_KIBI else self._UNIT_FACTORS_KILO
        for unit, factor in reversed(list(unit_factors.items())):
            if value_in_bytes >= factor:
                return Unit(f'{value_in_bytes / factor}{unit}')

    @staticmethod
    def reduce(args: Iterable[Union[Unit, str, int, float]], unit: Optional[str] = None,
               operation: Optional[Callable] = sum) -> Unit:
        """Reduce an iterable of units to a single unit
        :param args: iterable of units
        :param unit: unit to convert to
        :param operation: operation to perform on each unit
        :return: single Unit instance
        """
        candidates = list()
        for arg in args:
            if isinstance(arg, str):
                arg = Unit(arg)
            if isinstance(arg, Unit):
                arg = arg.convert_to('B')  # converting to bytes and not 'unit' to avoid float truncation
            candidates.append(arg)

        ret = Unit(f'{operation(candidates)}B')
        if unit:
            ret = Unit(f'{ret.convert_to(unit)}{unit}')
        else :
            ret = ret.adjust_unit()
        return ret

    @staticmethod
    def iterable_to_unit(args: Iterable[Union[Unit, str, int, float]], unit: Optional[str] = None,
                         operation: Optional[Callable] = sum) -> Unit:
        return Unit.reduce(args, unit, operation)

    def __repr__(self):
        result = self.adjust_unit()
        return f'{result.value: .2f}{result.unit}'

    def __eq__(self, other: Union[Unit, str]) -> bool:
        if not isinstance(other, Unit):
            other = Unit(other)
        return self.value == other.convert_to(self.unit)

    def __lt__(self, other: Union[Unit, str]) -> bool:
        if not isinstance(other, Unit):
            other = Unit(other)
        return self.value < other.convert_to(self.unit)

    def __add__(self, other: Union[Unit, str, float, int]) -> Unit:
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other}{self.unit}')
        value = self.convert_to('M') + other.convert_to('M')  # setting to M to avoid float silliness
        return Unit(f'{value} M').adjust_unit()

    def __sub__(self, other: Union[Unit, str, float, int]) -> Unit:
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other}{self.unit}')
        value = self.convert_to('M') - other.convert_to('M')  # setting to M to avoid float silliness
        return Unit(f'{value} M').adjust_unit()

    def __mul__(self, other: Union[Unit, str, float, int]) -> Unit:
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other}{self.unit}')
        value = self.value * other.convert_to(self.unit)
        return Unit(f'{value}{self.unit}').adjust_unit()

    def __truediv__(self, other: Union[Unit, str, float, int]) -> Unit:
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other} {self.unit}')
        value = format(self.convert_to('B') / other.convert_to('B'), '.15f')
        return Unit(f'{value}{self.unit}').adjust_unit()

    def as_unit(self, unit: str) -> str:
        """Return the value as the given unit. If unit='auto', return as is
        :param unit: unit to convert to
        :return: value as string
        """
        return str(self) if unit == 'auto' else f'{int(self.convert_to(unit))}{unit}'


class Range(click.ParamType):  # not subclassing 'range', as is marked as '@final'
    name = 'Range'
    start_default = 1

    def convert(self, value: str, param: click.Parameter, ctx: click.Context) -> range:
        if ':' not in value and '-' not in value:
            start = self._parse_to_int(value, param, ctx, self.start_default)
            return range(start, start + 1)
        split_value = value.split(':') if ':' in value else value.split('-')

        if len(split_value) > 2:
            self.fail(f'{value} is not a valid range', param, ctx)

        import sys
        max_size = sys.maxsize
        if not split_value:
            return range(self.start_default, max_size)
        start = self._parse_to_int(split_value[0], param, ctx, self.start_default)
        stop = self._parse_to_int(split_value[1], param, ctx, max_size)
        return range(start, stop)

    def _parse_to_int(self, value: str, param: click.Parameter, ctx: click.Context, default: int) -> int:
        try:
            return int(value) if value else default
        except ValueError:
            self.fail(f'{value} is not a valid integer', param, ctx)


class Ranges(click.ParamType):
    name = 'Ranges'

    def __iter__(self):
        return iter(tuple(self))

    def convert(self, value: str, param: click.Parameter, ctx: click.Context) -> tuple[range, ...]:
        return tuple(Range().convert(r, param, ctx) for r in value.split(','))


class IPAddress(click.ParamType):
    name = 'IP Address'

    def convert(self, value, param, ctx):
        try:
            return ipaddress.ip_address(value)
        except ValueError:
            self.fail(f'{value} is not a valid IP address', param, ctx)
