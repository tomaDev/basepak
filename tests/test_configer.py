import os
import shutil
from pathlib import Path

from ruyaml import YAML

from basepak.configer import generate

yaml = YAML(typ='safe', pure=True)

def test_generate_basic():
    config = {'key': 'value', 'number': 42}
    output_file = generate(config)
    assert os.path.exists(output_file)
    with open(output_file) as f:
        data = yaml.load(f)
    assert data == config
    os.remove(output_file)

def test_generate_with_filename():
    config = {'key': 'value'}
    filename = 'test_config'
    output_file = generate(config, filename=filename)
    expected_file = f'{filename}.yaml'
    assert output_file == expected_file
    assert os.path.exists(output_file)
    with open(output_file) as f:
        data = yaml.load(f)
    assert data == config
    os.remove(output_file)

def test_generate_with_destination_folder(tmp_path):
    config = {'key': 'value'}
    destination_folder = tmp_path / 'config_dir'
    output_file = generate(config, destination_folder=destination_folder)
    expected_file = destination_folder / Path(output_file).name
    assert Path(output_file) == expected_file
    assert expected_file.exists()
    with open(expected_file) as f:
        data = yaml.load(f)
    assert data == config

def test_generate_with_all_parameters(tmp_path):
    config = {'key': 'value'}
    destination_folder = tmp_path / 'config_dir'
    filename = 'custom_config'
    output_file = generate(config, destination_folder=destination_folder, filename=filename)
    expected_file = destination_folder / f'{filename}.yaml'
    assert Path(output_file) == expected_file
    assert expected_file.exists()
    with open(expected_file) as f:
        data = yaml.load(f)
    assert data == config

def test_generate_many_times_to_same_path(tmp_path):
    config = {'key': 'value'}
    filename = 'test_config'
    destination_folder = tmp_path / 'config_dir'
    paths = []
    paths_len = 10
    for _ in range(paths_len):
        paths.append(generate(config, destination_folder=destination_folder, filename=filename))
        assert os.path.exists(paths[-1])
    assert len(set(paths)) == paths_len, 'All paths should be unique'
    shutil.rmtree(destination_folder)
