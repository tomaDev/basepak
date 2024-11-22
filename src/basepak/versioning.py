from dataclasses import dataclass, field


@dataclass(order=True)
class Version:  # looseversion lib comes as a dep for rethinkdb driver. Consider using it instead of rolling our own
    """Parse version strings into ints and compare between them"""
    _input_str: str = field(repr=False, compare=False)
    major: int = field(default=0)
    minor: int = field(default=0)
    patch: int = field(default=0)

    def __post_init__(self) -> None:
        """Parse input string into version parts and set as instance attributes"""
        parts = self._input_str.replace('-', '.').split('.')

        # TODO: move this part into IguazioVersion
        for i, part in enumerate(parts.copy()):
            if part.startswith('b') and part[1:].isdigit():  # handle build numbers
                parts[i] = part[1:]
                break
        parts = [int(part) for part in parts if part.isdigit()]  # ignore branches in comparison

        parts_length = len(parts)
        for i, key_ in enumerate(vars(self)):  # here we deal with variable input lengths
            if i == 0:
                continue
            if i - 1 == parts_length:
                break
            setattr(self, key_, int(parts[i - 1]))
        self._frozen = True

    def __repr__(self):
        return f'{self.major}.{self.minor}.{self.patch}'


@dataclass(order=True)
class IguazioVersion(Version):
    """Iguazio version, eg 3.5.3-b395.20230221201131 3.6.0-rocky8.toma.b2291.20240228143900"""
    build: int = field(default=0)
    timestamp: int = field(default=0)

    def __repr__(self) -> str:
        return self._input_str  # repr includes branches, but dataclass comparison does not
