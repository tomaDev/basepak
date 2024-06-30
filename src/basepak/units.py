from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Iterable, Union, Callable, Optional

import click


@total_ordering
@dataclass
class Unit:  # todo: need to test numfmt --to=iec to replace huge chunks of this class
    """Size unit class. Generalizable to generic units, but in later Python versions there's the Pint library,
    and right now there's no need"""
    _input_string: str = field(repr=False)
    value: float = field(init=False, compare=True)
    unit: str = field(init=False)

    ZERO_UNIT = '0 B'
    UNIT_FACTORS = {
        'B':  1,    # Bytes only, no bits
        'K':   1024,
        'Ki':  1024,
        'KiB': 1024,
        'KB': 1000,
        'M':   1024 ** 2,
        'Mi':  1024 ** 2,
        'MiB': 1024 ** 2,
        'MB': 1000 ** 2,
        'G':   1024 ** 3,
        'Gi':  1024 ** 3,
        'GiB': 1024 ** 3,
        'GB': 1000 ** 3,
        'T':   1024 ** 4,
        'Ti':  1024 ** 4,
        'TiB': 1024 ** 4,
        'TB': 1000 ** 4,
        'P':   1024 ** 5,
        'Pi':  1024 ** 5,
        'PiB': 1024 ** 5,
        'PB': 1000 ** 5,
    }

    def __post_init__(self) -> None:
        input_list = self.split_on_first_letter(self._input_string)
        input_stripped = self.clean_strings(input_list)
        if len(input_stripped) != 2:
            raise ValueError(f'Constructor format: Number[ ]?Unit\nGot: {self._input_string}')
        self.value, self.unit = input_stripped
        self.value = float(self.value)

    def convert_to(self, unit: str) -> float:
        if unit not in self.UNIT_FACTORS:
            raise ValueError(f'Unsupported units for conversion: {unit}\n'
                             f'Options: {self.UNIT_FACTORS.keys()}')
        if unit == self.unit:
            return self.value
        value_in_bytes = self.value * self.UNIT_FACTORS[self.unit]
        return value_in_bytes / self.UNIT_FACTORS[unit]

    def adjust_unit(self) -> 'Unit':
        if self.value == 0:
            return Unit(self.ZERO_UNIT)
        value_in_bytes = self.value * self.UNIT_FACTORS[self.unit]
        for unit, factor in reversed(list(self.UNIT_FACTORS.items())):
            if value_in_bytes >= factor:
                return Unit(f'{value_in_bytes / factor} {unit}')

    @staticmethod
    def iterable_to_unit(args: Iterable[Union['Unit', str, int, float]], unit: Optional[str] = 'B',
                         operation: Optional[Callable] = sum) -> 'Unit':
        candidates = list()
        for arg in args:
            if isinstance(arg, str):
                arg = Unit(arg)
            if isinstance(arg, Unit):
                arg = arg.convert_to('B')  # converting to bytes and not 'unit' to avoid float truncation
            candidates.append(arg)

        aggregated_value = operation(candidates)
        ret = Unit(f'{aggregated_value} {unit}').adjust_unit()
        return ret

    def __repr__(self):
        result = self.adjust_unit()
        return f'{result.value: .2f} {result.unit}'

    def __lt__(self, other: Union['Unit', str]) -> bool:
        if not isinstance(other, Unit):
            other = Unit(other)
        return self.value < other.convert_to(self.unit)

    def __add__(self, other: Union['Unit', str, float, int]) -> 'Unit':
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other} {self.unit}')
        value = self.convert_to('M') + other.convert_to('M')  # setting to M to avoid float silliness
        return Unit(f'{value} M').adjust_unit()

    def __sub__(self, other: Union['Unit', str, float, int]) -> 'Unit':
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other} {self.unit}')
        value = self.convert_to('M') - other.convert_to('M')  # setting to M to avoid float silliness
        return Unit(f'{value} M').adjust_unit()

    def __mul__(self, other: Union['Unit', str, float, int]) -> 'Unit':
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other} {self.unit}')
        value = self.value * other.convert_to(self.unit)
        return Unit(f'{value} {self.unit}').adjust_unit()

    def __truediv__(self, other: Union['Unit', str, float, int]) -> 'Unit':
        if isinstance(other, str):
            other = Unit(other)
        if isinstance(other, (float, int)):
            other = Unit(f'{other} {self.unit}')
        value = format(self.convert_to('B') / other.convert_to('B'), '.15f')
        return Unit(f'{value} {self.unit}').adjust_unit()

    @staticmethod
    def split_on_first_letter(string: str) -> list[str]:
        """Split a string into two strings on the first occurrence of a letter"""
        index = next((i for i, c in enumerate(string) if c.isalpha()), len(string))
        return [string[:index], string[index:]]

    @staticmethod
    def clean_strings(string_list: list[str]) -> list[str]:
        """Split, strip and prune empties for a list of strings"""
        cleaned_list = []
        for string in string_list:
            parts = [part for part in string.split() if part]
            cleaned_list.extend(parts)
        return cleaned_list

    def as_unit(self, unit: str) -> str:
        """Return the value as the given unit. If unit='auto', return as is"""
        return str(self) if unit == 'auto' else f'{int(self.convert_to(unit))}{unit}'


class Range(click.ParamType):
    name = 'Range'
    start_default = 1
    stop_default = 1000

    def convert(self, value: str, param: click.Parameter, ctx: click.Context) -> range:
        if ':' not in value:
            start = self._parse_to_int(value, param, ctx, self.start_default)
            return range(start, start + 1)
        split_value = value.split(':')
        if len(split_value) > 2:
            self.fail(f'{value} is not a valid range', param, ctx)
        if not split_value:
            return range(self.start_default, self.stop_default)
        start = self._parse_to_int(split_value[0], param, ctx, self.start_default)
        stop = self._parse_to_int(split_value[1], param, ctx, self.stop_default)
        return range(start, stop)

    def _parse_to_int(self, value: str, param: click.Parameter, ctx: click.Context, default: int) -> int:
        try:
            return int(value) if value else default
        except ValueError:
            self.fail(f'{value} is not a valid integer', param, ctx)


class Ranges(click.ParamType):
    name = 'Ranges'

    def convert(self, value: str, param: click.Parameter, ctx: click.Context) -> list[range]:
        return [Range().convert(r, param, ctx) for r in value.split(',')]


class IPAddress(click.ParamType):
    name = 'IP Address'

    def convert(self, value, param, ctx):
        try:
            return ipaddress.ip_address(value)
        except ValueError:
            self.fail(f'{value} is not a valid IP address', param, ctx)
