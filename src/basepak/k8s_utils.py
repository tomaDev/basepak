"""Functions for creating and managing k8s resources"""
from __future__ import annotations

import functools
import json
import logging
import os
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Dict, Optional, Set, Mapping, Union

from . import consts, log, time
from .execute import Executable, subprocess_stream
from .versioning import Version

PathLike = Union[Path, str]


def md5sum(path: PathLike, chunk_size: int = 8192) -> str:
    """Compute the MD5 checksum of a file, returning a 32‑character hex string.
    """
    import hashlib

    hasher = hashlib.md5()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


DATE_FORMAT_DEFAULT = '%Y-%m-%dT%H:%M:%SZ'
EVENTS_WINDOW_DEFAULT = '1 hour'
RESOURCE_NOT_FOUND = 'Error from server (NotFound)'


def kubectl_dump(command: PathLike | Executable, output_file: PathLike, mode: str = 'dry-run') -> None:
    """Runs kubectl command and saves output to file

    :param command: kubectl command to run
    :param output_file: file to save output to
    :param mode: execution mode. 'dry-run' only shows command, any other mode executes
    """
    command = str(command)
    output_file = str(output_file)
    logger = log.get_logger(name='plain')
    logger.info(f'{command} > {output_file}')
    if mode == 'dry-run':
        return
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    error_file = f'{output_file}.err'
    subprocess_stream(command, output_file=output_file, error_file=error_file)

    logger.warning(Path(error_file).read_text())

    if os.path.getsize(error_file) == 0:
        os.remove(error_file)


def _parse_remote_path(_str: PathLike) -> tuple[str, str]:
    """Parse remote path into host and path parts"""
    remote_path = str(_str)
    if not remote_path:
        raise ValueError('Empty string received')
    cnt = remote_path.count(':')
    if cnt == 0:
        return remote_path, ''
    if cnt > 1:
        raise ValueError(f'Invalid remote path: {remote_path}')
    remote_part, path_part = remote_path.split(':')
    path_split = path_part.split()
    if len(path_split) == 0:
        return remote_part, ''
    if len(path_split) == 1:
        return remote_part, path_part
    return remote_part + ' ' + ' '.join(path_split[1:]), path_split[0]


def kubectl_cp(
        src: PathLike,
        dest: PathLike,
        err_file: Optional[PathLike] = None,
        mode: Optional[str] = 'dry-run',
        show_cmd=True,
        logger: Optional[logging.Logger] = None,
) -> None:
    """Alternative to `kubectl cp`, due to BKP-30. Main departures from `kubectl cp`:

    1. Portability: `kubectl cp` uses `tar` under the hood for any src/dest. For single file transfers, we stream
       directly to the target, thus dropping the need for `tar`. For dir transfers we use the `tar` on source host to
       stream it to the target as a file. We then extract it with the `tar` on the target host. This is done to reduce
       `tar` version compatibility issues across hosts. The tradeoffs are that it takes x2 longer and requires x2 the
       space on the target host.
    2. Functionality: `kubectl cp` does not support remote-to-remote transfers. We do, by downloading to local host first.
       Local host must have space to store x2 the content size

    :param src: source path. Can be local or remote
    :param dest: target path. Can be local or remote
    :param err_file: optional error file to write the error stream
    :param mode: execution mode. 'dry-run' only shows command, any other mode executes
    :param show_cmd: if True, logs the commands that are executed
    :param logger: logger object
    """

    up_src = dl_src = str(src)
    up_dest = dl_dest = str(dest)


    logger = logger or log.get_logger(name='plain')

    is_download = ':' in str(src)
    is_upload = ':' in str(dest)
    if not (is_download or is_upload):
        import inspect
        banner = ('Char ":" not found in either source or dest, suggesting they are both local paths. '
                  f'For local file transfer, please avoid using {inspect.currentframe().f_code.co_name}')
        logger.warning(banner)
        raise ValueError(banner) # implementing this is trivial, but encourages bad boundaries, so we error out instead

    if is_download and is_upload:
        import tempfile
        dl_dest = up_src = tempfile.mktemp()
        logger.warning('Both source and target are remote. Downloading to local host first, and uploading from there')

    if is_download:
        _download(dl_src, dl_dest, str(err_file or f'{dl_dest}.err'), mode=mode, show_=show_cmd, logger=logger)
    if is_upload:
        err_file = str(err_file or f'{up_src}.err')
        _upload(up_src, up_dest, str(err_file or f'{up_src}.err'), mode=mode, show_=show_cmd, logger=logger)


def _download(src: str, dest: str, err_file: str, mode: str, show_: bool, logger: logging.Logger,
              flags: Mapping[str, str] = None) -> None:
    """Download a file or directory from remote using kubectl exec.

    For a file, this is equivalent to:
      kubectl exec host -- cat {source_path} > {target_path}

    For a directory (when is_dir=True), this is similar* to:
      kubectl exec host -- tar cf - {source_path} | tar xf - -C {target_path}

    (*) The actual command is more complex to allow some safety checks and error handling

    :param flags: extra flags to pass
    :param src: remote path to download from. Consists of host:source_path
    :param dest: local file path (if a file) or directory (if a directory download)
    :param mode: 'dry-run' only shows command; any other mode executes
    :param err_file: file to write error output
    :param show_: if True, logs the command that is executed
    """
    remote, s_path = _parse_remote_path(src)

    kubectl = Executable('kubectl', 'kubectl exec', remote, '--')
    source_path_size = kubectl.run('du -sh', s_path, check=False)
    if source_path_size.returncode:
        logger.error(source_path_size.stderr)
        raise RuntimeError(source_path_size.stderr)


    is_dir = kubectl.run('test -d', s_path, check=False).returncode == 0
    is_file = kubectl.run('test -f', s_path, check=False).returncode == 0

    if not is_dir or is_file:
        logger.warning(f'Source path: {s_path}')
        s_type = 'not a dir, file or pipe' if kubectl.run('test -p', s_path, check=False).returncode else 'a pipe'
        logger.warning(f'Source path is {s_type}. Treating as file')

    from .units import Unit
    import psutil

    needed_space =  Unit(source_path_size.stdout.split()[0]) * (2 if is_dir else 1)
    available_disk = Unit(f'{psutil.disk_usage(os.path.dirname(dest)).free} B')

    if needed_space > available_disk:
        banner = f'Insufficient space on local host\n{needed_space=} > {available_disk=}'
        logger.error(banner)
        raise RuntimeError(banner)

    flags = flags or {}
    kubectl.set_args((flags.get('read_file', 'cat {path}')).format(path=s_path))
    if is_dir:
        kubectl.set_args(flags.get('read_dir', 'tar cf - {path}').format(path=s_path))
    if show_:
        logger.info(f'{kubectl} ' + f'| tar xf - -C {dest}' if is_dir else f'> {dest}')

    if mode == 'dry-run':
        return

    output_file = dest + '.tar' if is_dir else dest
    subprocess_stream(str(kubectl), output_file=output_file, error_file=err_file)

    if os.path.exists(err_file):
        logger.error(Path(err_file).read_text())
        if os.path.getsize(err_file) == 0:
            os.remove(err_file)

    remote_checksum = kubectl.run('| md5sum', show_cmd=False).stdout.split()[0]
    local_checksum = md5sum(output_file)

    logger.info(f' Local md5sum: {local_checksum}')
    logger.info(f'Remote md5sum: {remote_checksum}')

    if local_checksum != remote_checksum:
        banner = f' {local_checksum=}\n{remote_checksum=}\nChecksum mismatch!'
        logger.error(banner)
        raise RuntimeError(banner)

    if is_dir:
        import shutil
        shutil.unpack_archive(output_file, dest)
        os.remove(output_file)


def _upload(src: str, dest: str, err_file: str, mode: str, show_: bool, logger: logging.Logger) -> None:
    """Upload to remote using kubectl exec

    End result is the equivalent of:

    `kubectl exec -i remote -- sh -c 'cat > source_path' < source_path`

    Example with values:

    `kubectl exec -i --namespace tests --container c1  deployments/tests -- sh -c 'cat > /tmp/x.log' < /user/some.log`

    :param show_: log the upload command that is executed
    :param src: file to upload (its contents are piped into the command)
    :param dest: remote path to download from. Consists of host:target_path
    :param mode: execution mode. 'dry-run' only shows command, any other mode executes
    :param err_file: error file to write the error stream
    """
    remote, t_path = _parse_remote_path(dest)

    source_exists = os.path.exists(src)
    if not source_exists and mode != 'dry-run':
        logger.error(f'FileNotFound: {src}')
        raise FileNotFoundError(f'{src} does not exist')

    if source_exists and os.path.getsize(src) == 0:
        logger.warning(f'{src} is empty! Skipping upload')
        return

    if source_exists and os.path.isdir(src):
        return _upload_dir(src, t_path, err_file, remote, mode, show_, logger)

    command = f"kubectl exec -i {remote} -- sh -c 'cat > {t_path}'"
    if show_:
        logger.info(command + f' < {src}')

    if mode == 'dry-run':
        return

    with Path(src).open('rb') as f_in:
        subprocess_stream(command, stdin=f_in, error_file=err_file)

    logger.warning(Path(err_file).read_text())
    if os.path.getsize(err_file) == 0:
        os.remove(err_file)

    local_checksum = md5sum(src)
    remote_checksum = Executable('exec_', 'kubectl exec', remote, '-- md5sum').run(t_path).stdout.split()[0]
    if local_checksum != remote_checksum:
        banner = f' {local_checksum=}\n{remote_checksum=}\nChecksum mismatch!'
        logger.error(banner)
        raise RuntimeError(banner)


def _upload_dir(src: str, dest: str, err_file: str, remote: str, mode: str, show_: bool, logger: logging.Logger) -> None:
    exec_ = Executable('exec_', 'kubectl exec', remote, '--')
    target_dir = os.path.dirname(dest)
    ls_ = exec_.run('ls', target_dir).stdout.split()

    upload_path = dest + next(f'-{i}.tar' for i in range(1, 999) if f'{os.path.basename(dest)}-{i}.tar' not in ls_)
    command = f"kubectl exec -i {remote} -- sh -c 'cat > {upload_path}'"
    local_tar_cmd = f'tar cf - -C {os.path.dirname(src)} {os.path.basename(src)}'

    if show_:
        logger.info(f'{local_tar_cmd} | {command}')

    if mode == 'dry-run':
        return

    archiver = Executable('archiver', local_tar_cmd)
    resp = archiver.run('|', command, check=False)
    logger.info(resp.stdout)
    logger.info(resp.stderr)
    if resp.returncode:
        raise RuntimeError(resp.stderr)

    remote_checksum = exec_.run('md5sum', upload_path).stdout.split()[0]
    local_checksum = archiver.run(' | md5sum').stdout.split()[0]

    exec_.stream('tar xf', upload_path, '-C', target_dir, error_file=err_file)
    logger.info(Path(err_file).read_text())
    os.remove(err_file)
    if local_checksum != remote_checksum:
        banner = f' {local_checksum=}\n{remote_checksum=}\nChecksum mismatch!'
        logger.error(banner)
        raise RuntimeError(banner)



def print_namespace_events(namespace: str) -> None:
    """Print k8s events for a namespace, sorted by creation time, supports kubectl v1.21 and later

    :param namespace: k8s namespace"""
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace)
    cmd = 'events'
    if get_kubectl_version() < Version('1.23'):
        cmd = 'get events --sort-by=.metadata.creationTimestamp'
    elif get_kubectl_version() < Version('1.26'):
        cmd = 'alpha events'
    try:
        kubectl.stream(cmd)
    except:  # noqa called on a best effort basis
        pass


@functools.lru_cache
def get_kubectl_version() -> Version:
    """Get kubectl version

    :return: kubectl version as a Version object"""
    kubectl = Executable('kubectl')
    result = kubectl.run('version --client --output json')
    kubectl_version = json.loads(result.stdout)['clientVersion']['gitVersion'][1:]  # strip 'v' prefix
    return Version(kubectl_version)


def get_k8s_service_port(service_name: str, port_name: str, namespace: Optional[str] = 'default-tenant') -> str:
    """Get port of a service by name

    :param service_name: k8s service name
    :param port_name: port name
    :param namespace: k8s namespace
    :return: port number as a string
    """
    jsonpath = '{.spec.ports[?(@.name=="' + port_name + '")].port}'
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace, 'get service', service_name)
    return kubectl.run(f"--output jsonpath='{jsonpath}'").stdout


def get_intersect_app_nodes(node_names: Iterable[str], logger: logging.Logger) -> Set[str]:
    """Get ready nodes in App cluster, intersect with node_names if specified
    :param node_names: list of node names to intersect with ready nodes
    :param logger: logger object
    :return: set of ready nodes
    """
    kubectl = Executable('kubectl', 'kubectl get nodes', logger=logger)
    nodes = [node.split() for node in kubectl.run(f"--output jsonpath='{consts.JSONPATH_READY}'").stdout.splitlines()]
    ready_nodes = set(node[0] for node in nodes if node[1] == 'True')
    if not ready_nodes:
        logger.warning('No ready nodes found in App cluster')
        logger.warning(kubectl.run(f"--output jsonpath='{consts.JSONPATH_CONDITIONS}'").stdout)
        raise RuntimeError('No ready nodes found in App cluster')
    logger.debug(f'Ready nodes: {sorted(ready_nodes)}')
    if not node_names or all(not node for node in node_names):  # if node_names is empty or all nodes are empty
        logger.warning('No node names specified\nUsing all ready nodes')
        return ready_nodes

    node_names_override = set(node_names)
    intersect_nodes = ready_nodes.intersection(node_names_override)
    if node_names_override != intersect_nodes:
        logger.warning(f'Not all specified app nodes are ready\n'
                       f'Available: {sorted(ready_nodes)}\n'
                       f'Specified: {sorted(node_names_override)}\n'
                       f'Intersect: {sorted(intersect_nodes)}')
    if not intersect_nodes:
        raise NameError(f'No matching nodes are ready\nGenerally Available nodes: {sorted(ready_nodes)}\n')
    return intersect_nodes

def get_data_from_configmap(name: str, key: Optional[str] = None, namespace: Optional[str] = 'default-tenant') -> str:
    """Get data from a k8s configmap by name and key

    :param name: k8s configmap name
    :param key: key to get data for. Defaults to getting all data
    :param namespace: k8s namespace. Defaults to default-tenant
    :return: data as a string
    """
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace)
    cmd = 'get configmap ' + name + ' --output jsonpath={{.data' + f'.{key}' if key else '' + '}'
    return kubectl.run(cmd).stdout

def get_data_from_secret(name: str, key: Optional[str] = None, namespace: Optional[str] = 'default-tenant') -> str:
    """Get data from a k8s secret by name and key

    :param name: k8s secret name
    :param key: key to get data for. Defaults to getting all data
    :param namespace: k8s namespace. Defaults to default-tenant
    :return: data as a string
    """
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace)
    cmd = 'get secret ' + name + ' --output jsonpath={.data' + f'.{key}' if key else '' + '}'
    return kubectl.run(cmd).stdout

def get_namespace_from_file(file: str | Path, logger: logging.Logger, mode: str) -> str:
    """Get namespace from file, create if not present in k8s
    :param file: file to get namespace from
    :param logger: logger object
    :param mode: execution mode
    :return: namespace
    """
    file_path = Path(file)
    namespace_from_file = file_path.stem.split('_')[-1]
    if os.path.getsize(str(file)) == 0:
        logger.warning(f'File {file} is empty')
        return namespace_from_file
    namespace = ''
    try:
        with file_path.open('r') as f:
            content = json.load(f)
            try:
                namespace = content['items'][0]['metadata']['namespace']
            except KeyError:  # single item
                namespace = content['metadata']['namespace']
            except IndexError:  # empty list
                return namespace_from_file
    except json.decoder.JSONDecodeError as e:
        logger.warning(f'JSONDecodeError: {e}. This may happen if the file is not json')
    namespace = namespace or namespace_from_file
    if namespace == file_path.stem:
        logger.warning(f'Inferred namespace equals filename ({namespace}), which is suspect')
        if mode == 'normal':
            from . import confirm
            confirm.default('Namespace will be created if not present in k8s. Continue?')
    return namespace


def ensure_namespace(mode: str, logger: logging.Logger, *, namespace: Optional[str] = None,
                     file: Optional[PathLike] = None) -> str:
    """Ensure namespace exists in k8s, create if not present
    :param mode: execution mode
    :param logger: logger object
    :param namespace: namespace string
    :param file: file to get namespace from. If specified, namespace param is ignored
    :return: namespace
    """
    kubectl = Executable('kubectl')
    if file:
        namespace = get_namespace_from_file(file, logger, mode)
    namespace_exists = kubectl.run('get namespace', namespace, check=False)
    if namespace_exists.returncode == 0:  # success
        return namespace
    if namespace_exists.stderr.startswith(RESOURCE_NOT_FOUND):
        logger.warning(f'{namespace=} not found, creating...')
        kubectl.stream('create namespace', namespace, ' --dry-run=client' if mode == 'dry-run' else '')
        return namespace
    if not namespace_exists.stderr.startswith('Error from server (Forbidden)'):
        raise RuntimeError(namespace_exists.stderr)
    elif namespace == 'default-tenant':  # 'get ns' may fail due to permissions, but creating a job is still ok
        return namespace
    raise PermissionError(namespace_exists.stderr)


def ensure_pvc(spec: dict, logger: logging.Logger) -> None:
    """Ensure PVC exists in k8s, create if not present
    :param spec: dict with PVC parameters
    :param logger: logger object
    """
    ensure_namespace(spec['MODE'], logger, namespace=spec['NAMESPACE'])
    if spec['MODE'] == 'dry-run':
        return
    kubectl = Executable('kubectl', 'kubectl --namespace', spec['NAMESPACE'])

    if kubectl.run('get persistentvolumeclaim --ignore-not-found', spec['PERSISTENT_VOLUME_CLAIM_NAME']).stdout:
        logger.debug(f'persistentvolumeclaim {spec["PERSISTENT_VOLUME_CLAIM_NAME"]} exists\nSkipping creation..')
    else:
        logger.info('Creating persistent volume claim')
        from .templates import persistent_volume_claim
        _, path = persistent_volume_claim.generate_template(spec)
        kubectl.stream('create --filename', path)

    pvc_desired_states = [x.lower() for x in spec.get('PERSISTENT_VOLUME_CLAIM_DESIRED_STATES') or ['Bound']]
    pvc_name = spec['PERSISTENT_VOLUME_CLAIM_NAME']

    pvc_phase_jsonpath = 'jsonpath="{.status.phase}"'
    # Running "get" and then "wait" for backwards compatability.
    # kubectl in k8s 1.21 errors out on --for=jsonpath="{.status.phase}"=Bound for pvc
    # So we run "get" first, to allow run for existing bound pvc
    # On k8s 1.21 it will error out otherwise, on later versions "wait" works
    pvc_status: str = kubectl.run('get persistentvolumeclaim --output', pvc_phase_jsonpath, pvc_name).stdout
    if pvc_status.lower() not in pvc_desired_states:
        desired_states = ' '.join(pvc_desired_states)
        logger.warning(f'{pvc_status=} {desired_states=}\nAwaiting state change')

        kubectl.set_args('wait persistentvolumeclaim', pvc_name, '--timeout=15s')
        for phase in pvc_desired_states + pvc_desired_states:  # checking every phase twice to avoid change drift
            out = kubectl.run(f'--for={pvc_phase_jsonpath}={phase}', show_cmd_level='warning')
            if not out.returncode:
                logger.info(f'persistentvolumeclaim {pvc_name} is in desired state: {phase}')
                return
        kubectl.set_args('')
        pvc_status = kubectl.run('get persistentvolumeclaim --output', pvc_phase_jsonpath, pvc_name).stdout
        error_msg = f'{pvc_name=} {pvc_status=} Desired states: {" ".join(pvc_desired_states)}'
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def ensure_daemonset(spec: dict, logger: logging.Logger) -> None:
    """Ensure DaemonSet exists in k8s, create if not present
    :param spec: dict with DaemonSet parameters
    :param logger: logger object
    """
    namespace = spec['NAMESPACE']
    ensure_namespace(spec['MODE'], logger, namespace=namespace)
    ds = spec['DAEMONSET_NAME']
    kubectl = Executable('kubectl')
    get_status = Executable('get_status', 'kubectl get daemonset', ds, '--output jsonpath={.status}',
                            ' --namespace', namespace)
    resp = get_status.run(check=False)
    if resp.returncode:
        if resp.stderr.startswith(RESOURCE_NOT_FOUND):
            logger.info(f'{ds=} not found in {namespace=}. Creating...')
            if spec['MODE'] != 'dry-run':
                filename = 'journal-monitor'
                from .templates import daemonset
                _, path = daemonset.generate_template(spec, dump_folder=spec['CACHE_FOLDER'], filename=filename)
                kubectl.run('create --filename', path)
                resp = get_status.run()
        # TODO: add support for other errors
        else:
            raise RuntimeError(resp.stderr)
    if spec['MODE'] == 'dry-run':
        return
    ds_status = json.loads(resp.stdout)
    retries = 3
    interval = 10
    while ds_status['desiredNumberScheduled'] != ds_status['numberReady'] and retries:
        log.log_as('json', ds_status)
        logger.info('Waiting for desiredNumberScheduled == numberReady')
        retries -= 1
        time.sleep(interval)
        ds_status = json.loads(get_status.run().stdout)
    if not retries:
        raise RuntimeError(f'{ds=} not ready after {retries=} with {interval=} seconds')


def create_oneliner_job(
        spec: dict, command: str | Executable, container_name: str, await_completion: Optional[bool] = False,
        mode: Optional[str] = 'normal', redact: Optional[Sequence[str]] = None, completion_tail: Optional[int] = None
) -> str:
    """Create a k8s job that runs a single command

    \b
    Intended job flow
    1. Ensure namespace
    2. Ensure PVC (including creating PV if needed)
    3. Create job
    4. Redact saved job manifest yaml
    5. Await job completion (can be separated out to Task validate phase)

    :param spec: dict with job parameters
    :param command: command to run in the job
    :param container_name: container name
    :param await_completion: wait for job completion
    :param mode: execution mode
    :param redact: list of strings to redact from the job manifest
    :param completion_tail: num of lines to print from job logs on completion. Defaults to k8s default
    :return: job name
    """
    from .templates import batch_job
    from . import strings

    logger = log.get_logger(name=spec.get('LOGGER_NAME'), level=spec.get('LOG_LEVEL') or 'INFO')
    ensure_pvc(spec, logger)
    spec = spec.copy()
    trunc = strings.truncate_middle
    spec.update({
        'JOB_NAME': trunc(spec.get('JOB_NAME') or spec['INSTANCE_NAME'] + f'-{container_name}'),
        'MODE': mode,
        'CONTAINER_NAME': container_name,
        'COMMAND': ['sh', '-c', f'{command}'],
    })
    if mode == 'dry-run':
        return spec['JOB_NAME']

    kubectl = Executable('kubectl')
    get_jobs_resp = kubectl.run('get jobs --ignore-not-found --output name --namespace', spec['NAMESPACE'])
    job_names = {job.split('/')[-1] for job in get_jobs_resp.stdout.splitlines()}
    if job_names and spec['JOB_NAME'] in job_names:
        suf = next(i for i in range(1, 1000) if trunc(spec['JOB_NAME'] + f'-{i}') not in job_names)
        spec['JOB_NAME'] = trunc(spec['JOB_NAME'] + f'-{suf}')

    manifests_folder = spec.setdefault('GENERATED_MANIFESTS_FOLDER', spec['CACHE_FOLDER'])
    spec['JOB_NAME'], path = batch_job.generate_template(spec, manifests_folder, filename=container_name)
    kubectl.stream('create --filename', path)
    log.redact_file(path, redact)

    if await_completion:
        await_k8s_job_completion(spec, completion_tail)
    return spec['JOB_NAME']


# todo: this function is buggy. It was created because in k8s <=1.21 kubectl errors on many --for conditions.
#   Now we've dropped support for k8s <v1.24, so this function should be refactored to use kubectl wait
def await_k8s_job_completion(spec: dict, tail: Optional[int] = None) -> bool:
    """Wait for k8s job to complete
    :param spec: dict with job parameters
    :param tail: num of lines to print from job logs on completion. Defaults to k8s default
    """
    namespace = spec.get('NAMESPACE')
    if not namespace:
        raise ValueError('namespace not specified')
    name = spec.get('JOB_NAME')
    if not name:
        raise ValueError('job name not specified')
    logger = log.get_logger(name=spec.get('LOGGER_NAME'))
    logger_plain = log.get_logger('plain')
    job_status_cmd = 'get job --output jsonpath={.status} ' + name
    output_wide_on_debug = '--output wide' if spec.get('LOG_LEVEL') == 'DEBUG' else ''
    get_pods_cmd = f'get pods --selector=job-name={name} {output_wide_on_debug}'

    kubectl = Executable('kubectl', f'kubectl --namespace {namespace}')
    kubectl_run = functools.partial(kubectl.run, show_cmd=False, check=False)
    job_timeout = spec['JOB_TIMEOUT']
    logger.info(f'Waiting for {name} to complete, {job_timeout=}')
    if spec['MODE'] == 'dry-run':
        return True
    response = kubectl_run(job_status_cmd)
    retry_total = retry_count = consts.RETRIES_DEFAULT
    while response.returncode and response.stderr.startswith(RESOURCE_NOT_FOUND) and retry_count:
        backoff = 2 ** (retry_total - retry_count)
        time.sleep(backoff)  # lazy man's exponential backoff
        retry_count -= 1
        logger_plain.warning(f'{response.stderr}\nWaiting {backoff}s')
        response = kubectl_run(job_status_cmd)

    wait_interval = spec.get('WAIT_INTERVAL') or consts.WAIT_INTERVAL

    kubectl.stream(get_pods_cmd)
    retry_count = retry_total
    status = json.loads(kubectl.run(job_status_cmd).stdout)
    while status.get('active') and retry_count:  # main wait loop
        retry_not_ready_count = retry_total
        time.sleep(wait_interval)
        while status.get('active') and status.get('ready') == 0 and retry_not_ready_count:
            # job active, no pods ready (retry after failure, large image pull etc.)
            backoff = 2 ** (retry_total - retry_not_ready_count)
            time.sleep(backoff)  # lazy man's exponential backoff
            retry_not_ready_count -= 1
            status = json.loads(kubectl.run(job_status_cmd).stdout)
            kubectl.stream(get_pods_cmd, '--no-headers', show_cmd=False)
        # if 'gibby' in name:  # remove when gibby errors on 'Task failed and retry limit has been reached' and not hang
        #     retry_count = _check_gibby_logs_for_container_hang(name, namespace, kubectl, retry_count, logger_plain)
        status = json.loads(kubectl_run(job_status_cmd).stdout)

        from datetime import datetime
        now = datetime.now()
        if now.minute < 1 and now.second < wait_interval % 60 + 1:  # hourly liveness
            kubectl.stream(get_pods_cmd, '--no-headers', show_cmd=False)

    kubectl.stream(get_pods_cmd, '--no-headers', show_cmd=False)
    if response.returncode:
        print_namespace_events(namespace)
        raise RuntimeError(response.stderr)

    terminal_status = json.loads(kubectl.run(job_status_cmd).stdout)
    kubectl.stream(f'logs --ignore-errors --selector=job-name={name} --since={int(wait_interval)*2}s',
                   '' if not tail else f' --tail={tail}', show_cmd=False)
    if terminal_status.get('succeeded'):
        return True

    if status != terminal_status:
        logger.error('Running status does not match terminal status:')
        log.log_as('json', status, printer=logger_plain.warning)
    logger.info('Terminal status:')
    log.log_as('json', terminal_status, printer=logger_plain.warning)
    if terminal_status.get('failed'):
        raise RuntimeError(f'{name=}, {terminal_status=}')
    raise RuntimeError(f'{name=}, unexpected status - {status}')


def _check_gibby_logs_for_container_hang(job_name: str, namespace: str, kubectl: Executable, retries: int,
                                         logger: logging.Logger) -> int:
    """Check logs for container hang and delete all running pods if detected
    :param job_name: job name
    :param namespace: k8s namespace
    :param kubectl: kubectl Executable object
    :param retries: retry count
    :param logger: logger object
    :return: updated retry count
    """
    # kubectl logs --pod-running-timeout=5m should wait for at least one pod running, but it doesn't for some reason
    # added retry as a workaround

    import subprocess
    logs_cmd = f'logs --selector=job-name={job_name} --since=1m'
    try:
        logs = kubectl.run(logs_cmd, show_cmd=False)
    except subprocess.CalledProcessError:
        logger.warning(f'Failed to fetch logs for {job_name}\nRetrying...')
        try:
            logs = kubectl.run(logs_cmd)
        except subprocess.CalledProcessError:
            logger.warning(f'Failed to fetch logs for {job_name}\nFetching namespace events')
            print_namespace_events(namespace)
            logger.warning('Retrying one last time before erroring out')
            logs = kubectl.run(logs_cmd)

    task_failed_msg = 'Task failed and retry limit has been reached'
    if logs.returncode:
        logger.warning(f'Failed to fetch logs for {job_name}')
        raise RuntimeError(logs.stderr)
    logs = logs.stdout.splitlines()
    first_error_line = next((i for i, line in enumerate(logs) if task_failed_msg in line), None)
    if first_error_line:
        retries -= 1
        logger.warning(f'Stuckage detected!\n"{task_failed_msg}" found in logs')
        if not retries:
            logs = logs[first_error_line:]
            for line in logs:
                logger.warning(line)
            raise RuntimeError(f'"{task_failed_msg}" found in logs of {job_name}: {logs}')
        logger.warning(f'Optimistically deleting all running pods\nRetries remaining: {retries}')
        kubectl.stream(f'delete pod --selector=job-name={job_name} --output name --field-selector=status.phase=Running')
    return retries


def get_pod_name_and_job_image(selector: str, container: str, namespace: str, logger: logging.Logger,
                               retries: Optional[int] = consts.RETRIES_DEFAULT) -> Dict[str, str]:
    """Get pod name and job image from k8s
    :param selector:  selector
    :param container: container name
    :param namespace: k8s namespace
    :param logger: logger object
    :param retries: number of retries
    :return:  {
        'POD_NAME': pod_name,
        'JOB_IMAGE': job_image,
    }
    """
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace)
    kubectl.set_args('--selector', selector, 'get pods --output json')
    kubectl.show()
    pod_manifest = _get_running_pod_manifest(kubectl, tries=retries, retries=retries, logger=logger)
    pod_name = pod_manifest['metadata']['name']
    db_pod_containers = pod_manifest['spec']['containers']
    job_image = next(x['image'] for x in db_pod_containers if x['name'] == container)
    logger.info(f'Pod: {pod_name}\nContainer image: {job_image}')
    return {
        'POD_NAME': pod_name,
        'JOB_IMAGE': job_image,
    }


def _get_running_pod_manifest(kubectl: Executable, tries: int, retries: int, msg: Optional[str] = '',
                              logger: Optional[logging.Logger] = None) -> dict:
    """Get running pod manifest from k8s
    :param kubectl: kubectl Executable object
    :param tries: number of tries param
    :param retries: dynamic retry counter
    :param msg: error message to raise if failed
    :param logger: logger object
    :return: pod manifest
    """
    if retries < 0:
        raise RuntimeError(msg)
    if tries < retries:
        raise Exception(f'{tries=}<{retries=}! This should never happen!')
    time.sleep(2 ** (tries - retries) - 1)  # lazy man's exponential backoff
    retries -= 1
    result = kubectl.run()
    result_stdout = result.stdout.strip()
    try:
        items = json.loads(result_stdout).get('items')
    except json.decoder.JSONDecodeError:
        return _get_running_pod_manifest(kubectl, tries, retries, 'Failed to decode json', logger)
    except TypeError as e:  # "'NoneType' object is not subscriptable" error means parsed json is None
        return _get_running_pod_manifest(kubectl, tries, retries, str(e), logger)
    logger_plain = log.get_logger('plain')
    if not items:
        logger_plain.debug(result.stdout)
        logger.debug(f'{retries=}')
        return _get_running_pod_manifest(kubectl, tries, retries, 'No pods found', logger)
    pod_manifest = next((x for x in items if x['status']['phase'] == 'Running'), None)
    if not pod_manifest:
        log.log_as('json', result_stdout, printer=logger_plain.debug)
        logger.info(f'{retries=}')
        return _get_running_pod_manifest(kubectl, tries, retries, 'No running pods found', logger)
    return pod_manifest


def is_remote_sharing_disk_with_host(  # todo: create test
        spec: dict,
        local_path: str,
        remote_path: Optional[str] = None,
) -> bool:
    """Check if pod and host share the same disk
    :param spec:        task spec
    :param local_path:  local path
    :param remote_path: remote path to be checked from the pod mount. Defaults to local_path
    :return: True if newly created file on the host path is found on the remote path
    """
    if is_path_local(local_path):
        return False
    marker_name = f'.{spec.get("NAME") or "default"}-check-is-remote-sharing-disk-with-host-{os.urandom(4).hex()}'
    marker = Path(local_path, marker_name)
    marker.touch()
    # -1 for single column, -A for all files except ./..
    ls_job_name = create_oneliner_job(spec, f'ls -1A {remote_path or local_path}', 'ls', await_completion=True)
    kubectl = Executable('kubectl', 'kubectl logs --namespace', spec['NAMESPACE'])
    resp = kubectl.run(f'--selector=job-name={ls_job_name}', check=False)
    marker.unlink()
    return marker_name in resp.stdout.splitlines()


def is_path_local(path: str | Path) -> bool:
    """Check if path is on a local or remote disk.
    :param path: path
    :return:     True if path exists and resolves to a recognizable partition on a local disk. False otherwise
    """
    path = Path(path).resolve()
    if not path.exists():
        return False
    path = str(path)

    df = Executable('df')
    df_options = (
        '--local',  # `df --local`  supported on most Linux distros, including Rocky
        '-l',       # `df -l`       supported on MacOS
    )
    if df_option := next((x for x in df_options if df.run(x, check=False).returncode == 0), None):
        return df.run(df_option, path, check=False).returncode == 0

    return is_path_local_best_effort(path) # fallback to best effort without `df`


def is_path_local_best_effort(path: str | Path) -> bool:
    """Check if path is on a local or remote disk (without using `df`).
    :param path: path
    :return:     True if path exists and resolves to a recognizable partition on a local disk. False otherwise
    """
    path = Path(path).resolve()
    path = str(path)

    import psutil

    partitions = psutil.disk_partitions(all=True)

    # Sort by length in descending order. This ensures that the longest matching mount-point is used
    partitions.sort(key=lambda p: len(p.mountpoint), reverse=True)

    matched_partition = None
    for part in partitions:
        mnt = part.mountpoint
        if not mnt.endswith(os.sep):  # Ensure mount-point has a trailing slash for consistent matching
            mnt += os.sep

        if path.startswith(mnt):
            matched_partition = part
            break
    if matched_partition is None:
        raise False

    remote_fs_types = {'nfs', 'cifs', 'smb', 'ssh', 'fuse', 'afp', 'coda', 'gfs', 'lustre', 'gluster', 'ceph', 'dav'}
    fs_type = matched_partition.fstype.lower()
    if any([fs_type.startswith(x) for x in remote_fs_types]):
        return False

    # On some systems, remote mounts may appear as something like //server/share for cifs.
    # Check if device looks like a network path.
    device = matched_partition.device.lower()
    if device.startswith('//') or device.startswith('\\\\'):
        return False

    return True
