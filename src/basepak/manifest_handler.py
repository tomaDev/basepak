from __future__ import annotations

from pathlib import Path
from typing import AnyStr, Optional


def validate_spec_keys_are_dicts(manifest: dict[str, any]) -> None:
    import click
    for k, v in manifest.items():
        if v is None or not k.endswith('Spec'):
            continue
        if not isinstance(v, dict):
            raise click.BadParameter(f'Expected type: dict. Got: {type(v).__name__}', param_hint=k)
        validate_spec_keys_are_dicts(v)


def load_and_validate(yaml_path: AnyStr | Path) -> dict:
    """Load and validate yaml file
    :param yaml_path: path to yaml file
    :return: content
    :raises click.BadParameter: if file is empty or not a dict
    :raises click.FileError: if file is not found or not accessible
    """
    import click
    import ruyaml as yaml

    from . import log
    yaml_path = Path(yaml_path).resolve()
    try:
        manifest = yaml.YAML(typ='safe', pure=True).load(yaml_path)
        if manifest is None or not manifest or not isinstance(manifest, dict):
            raise click.BadParameter(f'Parsed as an empty file: {yaml_path}')
        log.get_logger().debug(f'Validated yaml structure for {yaml_path}')
    except yaml.YAMLError as e:
        raise click.BadParameter(f'YAMLError for {yaml_path}:\n{e}')
    except OSError as e:
        raise click.FileError(e.filename, hint=f'[{e.errno}] {e.strerror}')
    return manifest


def get_hosts_info(igz_version: Optional[str] = None) -> dict[str, dict[str, str]]:
    """Get hosts info from kompton inventory
    :param igz_version: iguazio version
    :return: hosts info
    :raises click.BadParameter: if hosts file content structure is unexpected
    """
    import click

    from . import consts
    igz_version_path = Path('/home/iguazio/igz/version.txt')
    path = consts.KOMPTON_INVENTORY_HOSTS_PATH_PATTERN.format(igz_version=igz_version or igz_version_path.read_text())
    hosts_file = load_and_validate(path)
    try:
        return hosts_file['all']['children']['nodes']['hosts']
    except KeyError as e:
        raise click.BadParameter(f'No hosts found in {path}', param_hint=f'[KeyError: {e}]')
