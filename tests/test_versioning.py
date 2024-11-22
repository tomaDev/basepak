from basepak.versioning import IguazioVersion, Version


# Tests for the Version class
def test_version_init():
    v = Version('1.2.3')
    assert v.major == 1
    assert v.minor == 2
    assert v.patch == 3

def test_version_repr():
    v = Version('1.2.3')
    assert repr(v) == '1.2.3'

def test_version_comparison():
    v1 = Version('1.2.3')
    v2 = Version('1.2.4')
    assert v2 > v1
    assert v1 < v2
    assert v1 != v2
    assert v1 == Version('1.2.3')

def test_version_non_digit_parts():
    v = Version('1.2.3-alpha')
    assert v.major == 1
    assert v.minor == 2
    assert v.patch == 3

def test_version_with_extra_parts():
    v = Version('1.2.3.4.5')
    assert v.major == 1
    assert v.minor == 2
    assert v.patch == 3  # Extra parts are ignored after patch

def test_version_with_invalid_input():
    v = Version('invalid')
    assert v.major == 0
    assert v.minor == 0
    assert v.patch == 0

def test_version_default_values():
    v = Version('')
    assert v.major == 0
    assert v.minor == 0
    assert v.patch == 0

# Tests for the IguazioVersion class
def test_iguazio_version_init():
    v = IguazioVersion('3.5.3-b395.20230221201131')
    assert v.major == 3
    assert v.minor == 5
    assert v.patch == 3
    assert v.build == 395
    assert v.timestamp == 20230221201131

def test_iguazio_version_repr():
    input_str = '3.6.0-rocky8.toma.b2291.20240228143900'
    v = IguazioVersion(input_str)
    assert repr(v) == input_str

def test_iguazio_version_comparison():
    v1 = IguazioVersion('3.5.3-b395.20230221201131')
    v2 = IguazioVersion('3.6.0-b400.20230301120000')
    assert v2 > v1

def test_iguazio_version_with_extra_parts():
    v = IguazioVersion('3.5.3-branch.b500.20230401000000')
    assert v.major == 3
    assert v.minor == 5
    assert v.patch == 3
    assert v.build == 500
    assert v.timestamp == 20230401000000

def test_version_comparison_with_invalid():
    v1 = Version('1.0.0')
    v2 = Version('invalid')
    assert v1 > v2
    assert v2 < v1
    assert v2 == Version('')

def test_iguazio_version_with_missing_parts():
    v = IguazioVersion('3.5')
    assert v.major == 3
    assert v.minor == 5
    assert v.patch == 0
    assert v.build == 0
    assert v.timestamp == 0

def test_iguazio_version_with_no_build():
    v = IguazioVersion('3.5.3')
    assert v.major == 3
    assert v.minor == 5
    assert v.patch == 3
    assert v.build == 0
    assert v.timestamp == 0
