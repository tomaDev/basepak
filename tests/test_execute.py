import subprocess

import logging
import pytest  # noqa
import textwrap
import shlex
import sys

from basepak.execute import Executable, subprocess_stream


@pytest.mark.parametrize("cmd, stdout_data, stderr_data, return_code", [
    ("echo 'hello'", ["hello"], [], 0),
    ("false", [], ["Command failed: false"], 1),
])
def test_subprocess_stream(cmd, stdout_data, stderr_data, return_code, capsys):
    if return_code == 0:
        subprocess_stream(cmd, logger_name='plain')
        out = capsys.readouterr().out.splitlines()
        for line in stdout_data:
            assert line in out
    else:
        with pytest.raises(subprocess.CalledProcessError):
            subprocess_stream(cmd, logger_name='plain')
        out = capsys.readouterr().out.splitlines()
        for line in stderr_data:
            assert line in out

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
    exe.assert_executable()

def test_executable_assert_executable_failure():
    exe = Executable('nonexistent_cmd_123')
    with pytest.raises(NameError):
        exe.assert_executable()

def test_executable_show(capsys):
    exe = Executable('echo')
    exe.show('hello')
    assert 'echo hello' in capsys.readouterr().out

def test_executable_run_success():
    exe = Executable('echo')
    result = exe.run('"hello"')
    assert result.stdout == 'hello\n'

def test_executable_run_failure():
    exe = Executable('false')
    with pytest.raises(subprocess.CalledProcessError):
        exe.run()

def test_executable_stream_success(capsys):
    exe = Executable('echo')
    exe.stream("output")
    assert 'output' in capsys.readouterr().out.splitlines()

def test_executable_stream_failure(capsys):
    exe = Executable('false')
    with pytest.raises(subprocess.CalledProcessError):
        exe.stream()
    assert 'Command failed: false' in capsys.readouterr().out

@pytest.mark.parametrize(
    "cmd, expected_lines",
    [
        ("sh -c 'echo hello'", ["hello"]),
        ("sh -c 'printf \"line1\\nline2\\n\"'", ["line1", "line2"]),
    ],
)
def test_subprocess_stream_success_stdout_logged(cmd, expected_lines, caplog):
    """On success, stdout should be streamed as INFO logs, no exception."""
    subprocess_stream(cmd)

    messages = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.INFO]
    for line in expected_lines:
        assert line in messages


def test_subprocess_stream_failure_raises_and_captures_stderr(caplog):
    """On non-zero exit, raises CalledProcessError and stderr is captured in .stderr."""
    cmd = "sh -c 'echo oops >&2; exit 1'"

    with caplog.at_level(logging.ERROR):
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            subprocess_stream(cmd)

        err = exc_info.value
        assert err.returncode == 1
        assert err.stderr == "oops"

    error_messages = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
    assert "oops" in error_messages
    assert any("Command failed" in m for m in error_messages)


def test_subprocess_stream_output_file_only(tmp_path, caplog):
    """When output_file is set, stdout goes to the file; stderr (if any) is logged."""
    out_file = tmp_path / "stdout.txt"
    cmd = "sh -c 'echo out; echo err >&2'"

    with caplog.at_level(logging.INFO):
        subprocess_stream(cmd, output_file=out_file)

    assert out_file.read_text().splitlines() == ["out"]

    error_messages = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
    assert "err" in error_messages

    info_messages = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.INFO]
    assert "out" not in info_messages


def test_subprocess_stream_error_file_only(tmp_path, caplog):
    """When error_file is set, stderr goes to the file; stdout is logged."""
    err_file = tmp_path / "stderr.txt"
    cmd = "sh -c 'echo out; echo err >&2'"

    with caplog.at_level(logging.INFO):
        subprocess_stream(cmd, error_file=err_file)

    assert err_file.read_text().splitlines() == ["err"]

    info_messages = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.INFO]
    assert "out" in info_messages

    error_messages = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
    assert "err" not in error_messages


def test_subprocess_stream_both_files_no_logging(tmp_path, caplog):
    """When both output_file and error_file are set, nothing is logged; everything goes to files."""
    out_file = tmp_path / "stdout.txt"
    err_file = tmp_path / "stderr.txt"
    cmd = "sh -c 'echo out; echo err >&2'"

    with caplog.at_level(logging.INFO):
        subprocess_stream(cmd, output_file=out_file, error_file=err_file)

    assert out_file.read_text().splitlines() == ["out"]
    assert err_file.read_text().splitlines() == ["err"]

    messages = [rec.getMessage() for rec in caplog.records]
    assert messages == []


def test_subprocess_stream_rejects_stdout_stderr_kwargs():
    """Passing stdout/stderr via kwargs should raise ValueError."""
    with pytest.raises(ValueError, match="manages stdout/stderr"):
        subprocess_stream("sh -c 'echo hi'", stdout=subprocess.PIPE)


def test_subprocess_stream_logger_name_mismatch():
    """Providing a logger and a different logger_name should raise ValueError."""
    with pytest.raises(ValueError, match="Logger name mismatch"):
        subprocess_stream("sh -c 'echo hi'", logger=logging.getLogger(name='some-name'), logger_name='other-name')


def test_subprocess_stream_stderr_tail_truncation():
    """If stderr is very long, only the last 1000 lines should be kept in CalledProcessError.stderr."""
    total_lines = 1100
    cmd = "sh -c 'for i in $(seq 1 " + str(total_lines) + "); do echo line$i >&2; done; exit 1'"

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        subprocess_stream(cmd)

    err = exc_info.value
    lines = err.stderr.splitlines()
    assert len(lines) == 1000
    assert lines[0] == "line101"
    assert lines[-1] == "line1100"


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
