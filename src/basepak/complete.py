import functools
import os
from typing import AnyStr, Optional

import click

from . import __name__ as package_name
from . import log
from .execute import Executable

# Click's native shell_completion.py in v8.1 uses `complete -o nosort`, which is supported by bash 4.4+
# Iguazio CentOS7 uses bash 4.2.46, so the Click native completion generation script fails
# Until we drop support for CentOS7, make it with Click in the lab and use an option supported by 4.2
# Generation steps:
# 1. Install bash version >= 4.4
#   wget http://ftp.gnu.org/gnu/bash/bash-5.1.tar.gz
#   tar xzf bash-5.1.tar.gz
#   cd bash-5.1
#   ./configure --prefix=$HOME/local/bash-5.1  LDFLAGS="-static"
#   make && make install
# 2. expose its path, so click could use it
#   export PATH=$HOME/local/bash-5.1/bin:$PATH
# 3. Generate the script (BKP just as an example)
#   _BKP_COMPLETE=bash_source bkp
# 4. Copy the output to the bash_completion file
# 5. Replace \n with \\n
# 6. Replace { and } with {{ and }}
# 7. Replace package_name with {0} for lower and {1} for upper
# 8. Replace the `complete -o nosort` with some other option
# 9. Re 5-8: DO NOT TRUST AI tools. They lie. Validate manually
COMPLETE_SCRIPT_BASH = """
_{0}_completion() {{
    local IFS=$'\\n'
    local response

    response=$(env COMP_WORDS="${{COMP_WORDS[*]}}" COMP_CWORD=$COMP_CWORD _{1}_COMPLETE=bash_complete $1)

    for completion in $response; do
        IFS=',' read type value <<< "$completion"

        if [[ $type == 'dir' ]]; then
            COMPREPLY=()
            compopt -o dirnames
        elif [[ $type == 'file' ]]; then
            COMPREPLY=()
            compopt -o default
        elif [[ $type == 'plain' ]]; then
            COMPREPLY+=($value)
        fi
    done

    return 0
}}

_{0}_completion_setup() {{
    complete -o default -F _{0}_completion {0}
}}

_{0}_completion_setup;
"""


def generate_script(
        profile: Optional[click.Path], path: Optional[click.Path] = None, shell: Optional[str] = 'auto',
        force: Optional[bool] = False, display_name: Optional[str] = None, cli: Optional[str] = None
) -> int:
    """Generate a shell completion script and optionally add a source command to your profile file
    :param profile: Path to the profile file to add the source command to
    :param path: Path to the completion script file
    :param shell: Shell to generate the completion script for. 'auto' to auto-detect
    :param force: Overwrite the completion script file if it exists
    :param display_name: Display name of the package. Defaults to the process name
    :param cli: Command line interface name. Defaults to the process name
    :return: 0 on success, 1 on failure
    """
    display_name = display_name or proc_name_best_effort(default=package_name)
    cli = cli or proc_name_best_effort(default=package_name)
    if shell == 'auto':
        shell = proc_parent_name_best_effort(default='bash')
    complete_script = COMPLETE_SCRIPT_BASH.format(cli, cli.upper())
    profile_filename_default = '.bashrc'
    complete_filename_default = f'.{cli}_completion.sh'
    if shell.endswith('zsh'):
        get_zsh_complete = Executable('zsh', f'_{cli.upper()}_COMPLETE=zsh_source {cli}')
        complete_script = get_zsh_complete.run().stdout
        profile_filename_default = '.zshrc'
        complete_filename_default = f'.{cli}_completion.zsh'
    if path is None:
        logger_plain = log.get_logger(name='plain')
        logger_plain.info(complete_script)
        return 0
    script_path = get_full_path(path, complete_filename_default)

    logger = log.get_logger()
    logger.info(f'Writing completion script to {script_path}')
    if not force and os.path.exists(script_path):
        click.confirm(f'{script_path} exists, overwrite?', abort=True)
    with open(script_path, mode='w') as f:
        f.write(complete_script)
    profile_path = get_full_path(profile, profile_filename_default)

    source_cmd = f'source {script_path}'
    with open(profile_path) as f:
        if source_cmd in [line.strip() for line in f.readlines()]:
            logger.info(f'Source command exists in {profile_path}\nSkipping')
            return 0
    logger.info(f'Adding source command to {profile_path}')
    from datetime import datetime
    with open(profile_path, mode='a') as f:
        f.write(f'\n# Added by {display_name} on {datetime.now()}\n{source_cmd}\n')
    logger.info('Done')
    return 0


def get_full_path(base_path, default_file) -> AnyStr:
    """Get the full path of a file, resolving ~ and ensuring it's a file
    :param base_path: Path to resolve
    :param default_file: Default file name to use if base_path is a directory
    :return: Full path to the file
    """
    if base_path is None or os.path.isdir(base_path):
        base_path = os.path.join(base_path or os.path.expanduser('~'), default_file)
    return os.path.realpath(base_path)


@functools.lru_cache
def proc_name_best_effort(default: str = '') -> str:
    """Get the name of the current process
    :param default: Default name to return if the name is not found
    :return: Process name"""
    import psutil
    try:
        return psutil.Process(os.getpid()).name() or default
    except Exception:  # noqa best effort
        return default


def proc_parent_name_best_effort(default: str = '') -> str:
    """Get the name of the parent of current process
    :param default: Default name to return if the name is not found
    :return: Parent process name"""
    import psutil
    try:
        return psutil.Process(os.getppid()).name() or default
    except Exception:  # noqa best effort
        return default
