import logging
import subprocess
from unittest.mock import MagicMock, patch

import pytest
import textwrap
import shlex
import sys


from basepak.execute import Executable, subprocess_stream


@pytest.fixture
def mock_logger():
    logger = MagicMock(spec=logging.Logger)
    logger.name = "test_logger"
    return logger

@pytest.fixture
def mock_popen():
    with patch("subprocess.Popen") as mock_popen:
        yield mock_popen

@pytest.fixture
def mock_run():
    with patch("subprocess.run") as mock_run:
        yield mock_run

@pytest.fixture
def mock_get_logger(mock_logger):
    with patch("basepak.log.get_logger", return_value=mock_logger):
        yield

@pytest.mark.parametrize("cmd, stdout_data, stderr_data, return_code", [
    ("echo 'hello'", [b"hello\n"], [], 0),
    ("false", [], [b"error\n"], 1),
])
def test_subprocess_stream(cmd, stdout_data, stderr_data, return_code, mock_logger, mock_popen, mock_get_logger):
    process_mock = MagicMock()
    process_mock.stdout = stdout_data
    process_mock.stderr = stderr_data
    process_mock.wait.return_value = return_code
    process_mock.returncode = return_code
    mock_popen.return_value = process_mock

    if return_code == 0:
        subprocess_stream(cmd, logger=mock_logger)
        for line in stdout_data:
            mock_logger.info.assert_any_call(line.decode('utf-8', errors='replace').rstrip())
        mock_logger.error.assert_not_called()
    else:
        with pytest.raises(subprocess.CalledProcessError):
            subprocess_stream(cmd, logger=mock_logger)
        for line in stderr_data:
            mock_logger.error.assert_any_call(line.decode('utf-8', errors='replace').rstrip())

@pytest.mark.parametrize("cmd_base, args, expected_cmd", [
    ("echo", ["hello"], "echo hello"),
    ("ls", ["-l", "/"], "ls -l /"),
])
def test_executable_with_(cmd_base, args, expected_cmd):
    exe = Executable(cmd_base)
    result_cmd = exe.with_(*args)
    assert result_cmd.strip() == expected_cmd

def test_executable_assert_executable_success():
    exe = Executable('python')
    with patch('shutil.which', return_value='/usr/bin/python'):
        exe.assert_executable()

def test_executable_assert_executable_failure():
    exe = Executable('nonexistent_cmd')
    with patch('shutil.which', return_value=None):
        with pytest.raises(NameError):
            exe.assert_executable()

def test_executable_show(mock_logger):
    exe = Executable('echo', logger=mock_logger)
    exe.show('hello')
    mock_logger.warning.assert_called_once_with('echo hello')

def test_executable_run_success(mock_run, mock_logger):
    mock_run.return_value = subprocess.CompletedProcess(args='echo "hello"', returncode=0, stdout=b'hello\n')
    exe = Executable('echo', logger=mock_logger)
    result = exe.run('"hello"')
    mock_run.assert_called_once()
    assert result.stdout == b'hello\n'

def test_executable_run_failure(mock_run, mock_logger):
    mock_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd='false')
    exe = Executable('false', logger=mock_logger)
    with pytest.raises(subprocess.CalledProcessError):
        exe.run()

def test_executable_stream_success(mock_popen, mock_logger, mock_get_logger):
    process_mock = MagicMock()
    process_mock.stdout = [b"output\n"]
    process_mock.stderr = []
    process_mock.wait.return_value = 0
    mock_popen.return_value = process_mock

    exe = Executable('echo', logger=mock_logger)
    exe.stream("'output'")
    mock_logger.info.assert_any_call('output')

def test_executable_stream_failure(mock_popen, mock_logger, mock_get_logger):
    process_mock = MagicMock()
    process_mock.stdout = []
    process_mock.stderr = [b"error\n"]
    process_mock.wait.return_value = 1
    process_mock.returncode = 1
    mock_popen.return_value = process_mock

    exe = Executable('false', logger=mock_logger)
    with pytest.raises(subprocess.CalledProcessError):
        exe.stream()
    mock_logger.error.assert_any_call('error')

@pytest.mark.parametrize('progress_line', [
    '[#####     ] 25%',
    '[]25%',
    '[#####################]',
])
def test_stream_with_progress_filters_pure_progress_lines(progress_line, capsys):
    code = textwrap.dedent(f"""
        import sys, time
        print("start")
        sys.stdout.write("{progress_line}\\n")
        sys.stdout.flush()
        print("done")
    """)
    cmd = f'{shlex.quote(sys.executable)} -c {shlex.quote(code)}'
    exe = Executable('test', cmd)

    rc = exe.stream_with_progress(show_cmd=False, mode='normal')
    assert rc == 0

    joined = capsys.readouterr().out
    assert 'start' in joined
    assert 'done' in joined
    assert progress_line.strip() not in joined

@pytest.mark.parametrize('mode', [
    'dry-run',
    'normal',
    'unsafe',
])
def test_stream_with_progress_dry_run(capsys, mode):
    cmd = 'ls -l' # expecting to print "total"
    rc = Executable('test', cmd).stream_with_progress(show_cmd=True, mode=mode)
    assert rc == 0

    joined = capsys.readouterr().out
    assert cmd in joined
    if mode != 'dry-run':
        assert 'total' in joined
    if mode == 'dry-run':
        assert 'total' not in joined

def test_stream_with_progress_logs_mixed_percent_lines(capsys):
    mixed = 'Progress 50% complete'

    code = textwrap.dedent(f"""
        import sys
        print("begin")
        print("{mixed}")
        print("end")
    """)

    exe = Executable('test', f'{shlex.quote(sys.executable)} -c {shlex.quote(code)}')
    rc = exe.stream_with_progress(title='test-mixed', show_cmd=False, mode='normal')
    assert rc == 0

    joined = capsys.readouterr().out
    assert 'begin' in joined
    assert 'Progress' in joined
    # todo: 50% shows in output, but gets mangled in test due to color issues. Need to fix
    assert 'complete' in joined
    assert 'end' in joined


def test_stream_with_progress_handles_carriage_returns(capsys):
    code = textwrap.dedent(r"""
        import sys
        sys.stdout.write("[###] 10%\r[#####] 25%\r[########] 50%\n")
        sys.stdout.flush()
        print("after-progress")
    """)
    exe = Executable('test', f'{shlex.quote(sys.executable)} -c {shlex.quote(code)}')
    rc = exe.stream_with_progress(title='test-mixed', show_cmd=False, mode='normal')
    assert rc == 0

    joined = capsys.readouterr().out
    assert 'after-progress' in joined
    assert '10%' not in joined
    assert '25%' not in joined
    assert '50%' not in joined
