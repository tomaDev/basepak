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

        # todo: move this part into IguazioVersion
        for i, part in enumerate(parts.copy()):
            if part.startswith('b') and part[1:].isdigit():  # handle build numbers
                parts[i] = part[1:]
                break
        parts = [int(part) for part in parts if part.isdigit()]  # ignore branches in comparison
        #

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


def is_semver(version_str: str) -> bool:
    """Check if a version string is a semantic version"""
    if '+' in version_str:
        version_part, build_metadata = version_str.split('+', 1)
    else:
        version_part = version_str
        build_metadata = None

    # Split the pre-release if any
    if '-' in version_part:
        core_version, prerelease = version_part.split('-', 1)
    else:
        core_version = version_part
        prerelease = None

    # Validate core version
    core_version_parts = core_version.split('.')
    if len(core_version_parts) != 3:
        return False

    major, minor, patch = core_version_parts

    # Validate major, minor, patch
    for num in (major, minor, patch):
        if not num.isdigit():
            return False
        if num != '0' and num.startswith('0'):
            return False

    # Validate prerelease
    if prerelease is not None:
        prerelease_identifiers = prerelease.split('.')
        for identifier in prerelease_identifiers:
            if not identifier:
                return False
            if not all(c.isalnum() or c == '-' for c in identifier):
                return False
            if identifier.isdigit():
                if identifier != '0' and identifier.startswith('0'):
                    return False

    # Validate build metadata
    if build_metadata is not None:
        build_identifiers = build_metadata.split('.')
        for identifier in build_identifiers:
            if not identifier:
                return False
            if not all(c.isalnum() or c == '-' for c in identifier):
                return False

    return True
