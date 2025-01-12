from __future__ import annotations

from pathlib import Path
from typing import Optional


def generate(config: dict, destination_folder: Optional[str | Path] = None, filename: Optional[str] = None) -> str:
    """Generate a yaml file from a python dictionary. Adapted from:
    https://anthonyhawkins.medium.com/is-python-the-perfect-json-yaml-templating-engine-c5c1b32418f6

    :param config: The python dictionary to generate the yaml file from
    :param destination_folder: The folder to write the yaml file to
    :param filename: The name of the yaml file to write to
    :return: The path to the generated template file
    """
    import inspect
    import os

    import ruyaml as yaml
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

    if destination_folder:
        os.makedirs(destination_folder, exist_ok=True)
        filename = f'{destination_folder}{slash}' + filename

    suf = '.yaml'
    if Path(filename + suf).exists():
        basename = os.path.basename(filename)
        i = 1
        while f'{basename}-{i}{suf}' in {f for f in os.listdir(Path(filename).parent) if f.startswith(basename)}:
            i += 1
        suf = f'-{i}{suf}'
        # if i > 1000:
        #     from basepak import log
        #     logger = log.get_logger('plain')
        #     logger.warning(f'{i} files in {filename}*\n'
        #                    f'That is too many files!\nPlease consider cleaning up')

    filename += suf

    yaml.SafeDumper.ignore_aliases = lambda *args: True
    yaml.YAML(typ='safe', pure=True).dump(config, Path(filename))
    return filename
