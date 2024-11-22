from pathlib import Path
from basepak.configer import generate
from ruyaml import YAML
import os

yaml = YAML(typ='safe', pure=True)

def test_generate_basic(tmp_path):
    config = {'key': 'value', 'number': 42}
    output_file = generate(config)
    assert os.path.exists(output_file)
    with open(output_file, 'r') as f:
        data = yaml.load(f)
    assert data == config
    os.remove(output_file)

def test_generate_with_filename(tmp_path):
    config = {'key': 'value'}
    filename = 'test_config'
    output_file = generate(config, filename=filename)
    expected_file = f'{filename}.yaml'
    assert output_file == expected_file
    assert os.path.exists(output_file)
    with open(output_file, 'r') as f:
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
    with open(expected_file, 'r') as f:
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
    with open(expected_file, 'r') as f:
        data = yaml.load(f)
    assert data == config
