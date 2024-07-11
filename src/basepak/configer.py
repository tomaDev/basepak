from __future__ import annotations

import inspect
import os
from typing import Optional
from pathlib import Path
import ruyaml as yaml


def generate(config: dict, destination_folder: Optional[str | Path] = None, filename: Optional[str] = None) -> None:
    """Generate a yaml file from a python dictionary. Adapted from:
    https://anthonyhawkins.medium.com/is-python-the-perfect-json-yaml-templating-engine-c5c1b32418f6

    :param config: The python dictionary to generate the yaml file from
    :param destination_folder: The folder to write the yaml file to
    :param filename: The name of the yaml file to write to
    """
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    slash = '\\' if os.name == 'nt' else '/'
    filename = filename or module.__file__.rsplit(slash, maxsplit=1)[1].rsplit('.', maxsplit=1)[0].replace('_', '-')

    yaml.SafeDumper.org_represent_str = yaml.SafeDumper.represent_str

    def multi_str(dumper, data):
        if '\n' in data:
            return dumper.represent_scalar(
                'tag:yaml.org,2002:str', data, style='|')
        return dumper.org_represent_str(data)
    yaml.add_representer(str, multi_str, Dumper=yaml.SafeDumper)

    write_to = f'{filename}.yaml'
    if destination_folder:
        os.makedirs(destination_folder, exist_ok=True)
        write_to = f'{destination_folder}{slash}' + write_to

    yaml.SafeDumper.ignore_aliases = lambda *args: True
    yaml.YAML(typ='safe', pure=True).dump(config, Path(write_to))
