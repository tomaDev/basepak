from unittest.mock import patch

import pytest

from basepak.complete import generate_script


@pytest.fixture
def mock_proc_name():
    with patch('basepak.complete.proc_name_best_effort', return_value='mycli'):
        yield

@pytest.fixture
def mock_parent_proc_name():
    with patch('basepak.complete.proc_parent_name_best_effort', return_value='bash'):
        yield

@pytest.fixture
def temp_files(tmp_path):
    profile_path = tmp_path / '.bashrc'
    script_path = tmp_path / '.mycli_completion.sh'
    profile_path.write_text('# Dummy profile')
    return profile_path, script_path

def test_generate_script_no_path(mock_proc_name, mock_parent_proc_name, capsys):
    result = generate_script(profile=None, path=None, shell='auto', force=False)
    assert result == 0
    captured = capsys.readouterr()
    assert len(captured.out) > 1

def test_generate_script_bash(temp_files, mock_proc_name, mock_parent_proc_name):
    profile_path, script_path = temp_files
    result = generate_script(profile=profile_path, path=script_path, shell='bash', force=False)
    assert result == 0
    assert script_path.exists()
    assert len(script_path.read_text()) > 1

def test_generate_script_overwrite(temp_files, mock_proc_name, mock_parent_proc_name, monkeypatch):
    profile_path, script_path = temp_files
    script_path.write_text('# Existing content')

    monkeypatch.setattr('click.confirm', lambda x, abort: True) # Simulate user confirmation
    result = generate_script(profile=profile_path, path=script_path, shell='bash', force=False)
    assert result == 0
    assert len(script_path.read_text()) > 1

def test_generate_script_no_overwrite(temp_files, mock_proc_name, mock_parent_proc_name, monkeypatch):
    profile_path, script_path = temp_files
    script_path.write_text('# Existing content')

    monkeypatch.setattr('click.confirm', lambda x, abort: False) # Simulate user aborting overwrite
    assert script_path.read_text() == '# Existing content' # Ensure the script content is unchanged

def test_generate_script_force_overwrite(temp_files, mock_proc_name, mock_parent_proc_name):
    profile_path, script_path = temp_files
    script_path.write_text('# Existing content')
    result = generate_script(profile=profile_path, path=script_path, shell='bash', force=True)
    assert result == 0
    assert len(script_path.read_text()) > 1

def test_generate_script_zsh(temp_files, mock_proc_name):
    profile_path, script_path = temp_files

    with patch('basepak.complete.proc_parent_name_best_effort', return_value='zsh'):
        with patch('basepak.complete.Executable') as mock_executable:
            mock_executable.return_value.run.return_value.stdout = 'ZSH COMPLETION SCRIPT'
            result = generate_script(profile=profile_path, path=script_path, shell='auto', force=False)
            assert result == 0
            assert mock_executable.return_value.run.return_value.stdout in script_path.read_text()

def test_generate_script_source_exists(temp_files, mock_proc_name, mock_parent_proc_name):
    profile_path, script_path = temp_files
    profile_path.write_text(f'# Existing profile\nsource {script_path}\n')
    result = generate_script(profile=profile_path, path=script_path, shell='bash', force=False)
    assert result == 0
    profile_content = profile_path.read_text()
    assert profile_content.count(f'source {script_path}') == 1

def test_generate_script_invalid_shell(temp_files, mock_proc_name):
    profile_path, script_path = temp_files
    with patch('basepak.complete.proc_parent_name_best_effort', return_value='unknown_shell'):
        result = generate_script(profile=profile_path, path=script_path, shell='auto', force=False)
        assert result == 0
        assert len(script_path.read_text()) > 1
