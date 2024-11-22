import logging
import subprocess
from unittest.mock import MagicMock, patch

import pytest

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
    exe = Executable("python")
    with patch("shutil.which", return_value="/usr/bin/python"):
        exe.assert_executable()

def test_executable_assert_executable_failure():
    exe = Executable("nonexistent_cmd")
    with patch("shutil.which", return_value=None):
        with pytest.raises(NameError):
            exe.assert_executable()

def test_executable_show(mock_logger):
    exe = Executable("echo", logger=mock_logger)
    exe.show("hello")
    mock_logger.warning.assert_called_once_with("echo hello")

def test_executable_run_success(mock_run, mock_logger):
    mock_run.return_value = subprocess.CompletedProcess(args='echo "hello"', returncode=0, stdout=b'hello\n')
    exe = Executable("echo", logger=mock_logger)
    result = exe.run('"hello"')
    mock_run.assert_called_once()
    assert result.stdout == b'hello\n'

def test_executable_run_failure(mock_run, mock_logger):
    mock_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd='false')
    exe = Executable("false", logger=mock_logger)
    with pytest.raises(subprocess.CalledProcessError):
        exe.run()

def test_executable_stream_success(mock_popen, mock_logger, mock_get_logger):
    process_mock = MagicMock()
    process_mock.stdout = [b"output\n"]
    process_mock.stderr = []
    process_mock.wait.return_value = 0
    mock_popen.return_value = process_mock

    exe = Executable("echo", logger=mock_logger)
    exe.stream("'output'")
    mock_logger.info.assert_any_call("output")

def test_executable_stream_failure(mock_popen, mock_logger, mock_get_logger):
    process_mock = MagicMock()
    process_mock.stdout = []
    process_mock.stderr = [b"error\n"]
    process_mock.wait.return_value = 1
    process_mock.returncode = 1
    mock_popen.return_value = process_mock

    exe = Executable("false", logger=mock_logger)
    with pytest.raises(subprocess.CalledProcessError):
        exe.stream()
    mock_logger.error.assert_any_call("error")
