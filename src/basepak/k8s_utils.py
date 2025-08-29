"""Functions for creating and managing k8s resources"""
from __future__ import annotations

import functools
import json
import logging
import os
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Dict, Optional, Set, Union

from . import consts, log, time
from .execute import Executable, subprocess_stream
from .versioning import Version

PathLike = Union[Path, str]


def md5sum(path: PathLike, chunk_size: int = 8192) -> str:
    """Compute the MD5 checksum of a file, returning a 32‑character hex string.
    """
    import hashlib

    hasher = hashlib.md5(usedforsecurity=False)
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


DATE_FORMAT_DEFAULT = '%Y-%m-%dT%H:%M:%SZ'
EVENTS_WINDOW_DEFAULT = '1 hour'
RESOURCE_NOT_FOUND = 'Error from server (NotFound)'
BACKOFF_LIMIT_EXCEEDED_ERROR = 'BackoffLimitExceeded'
WAIT_TIMEOUT_ERROR = 'timed out waiting for the condition'


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

    if err_text := Path(error_file).read_text().strip():
        logger.warning(err_text)
    else:
        os.remove(error_file)


def _parse_remote_path(_str: PathLike) -> tuple[str, str]:
    """Parse remote path into host and path parts

    :returns: host, path"""
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
        mode: Optional[str] = 'dry-run',
        show_cmd=True,
        logger: Optional[logging.Logger] = None,
        retries: Optional[int] = 3,
) -> None:
    """wrapper on top of `kubectl cp` with more error handling and remote-to-remote transfer functionality. Host must
    have enough disk to store x2 the content size, as we use local fs to fully store contents before uploading to remote

    :param src: source path. Can be local or remote
    :param dest: target path. Can be local or remote
    :param mode: execution mode. 'dry-run' only shows command, any other mode executes
    :param show_cmd: if True, logs the commands that are executed
    :param logger: logger object
    :param retries: retry attempts on failure
    """

    up_src = dl_src = str(src)
    up_dest = dl_dest = str(dest)

    logger = logger or log.get_logger(name='plain')

    is_download = ':' in str(src)
    is_upload = ':' in str(dest)
    if not (is_download or is_upload):
        import inspect
        banner = ('Char ":" not found in source/dest, suggesting they are both local paths. '
                  f'For local file transfer, please avoid using {inspect.currentframe().f_code.co_name}')
        logger.warning(banner)
        raise ValueError(banner) # implementing this encourages bad boundaries, so we error out instead

    temp_file = None
    try:
        if is_download and is_upload:
            import tempfile
            temp_file = dl_dest = up_src = tempfile.NamedTemporaryFile(delete=False).name
            logger.warning('Both source and target are remote. Downloading to local host first, and then uploading')
        if is_download:
            _dl(dl_src, dl_dest, mode=mode, show_=show_cmd, logger=logger, retries=retries)
        if is_upload:
            _up(up_src, up_dest, mode=mode, show_=show_cmd, logger=logger, retries=retries)
    finally:
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)

def _dl(src: str, dest: str, mode: str, show_: bool, logger: logging.Logger, retries: int) -> None:
    remote, s_path = _parse_remote_path(src)

    kubectl = Executable('kubectl')

    # Option `--pod-running-timeout` in kubectl run/exec doesn't help - exec errors out with:
    #  error: Internal error occurred: unable to upgrade connection: container not found ("container-name")
    # Apparently pod gets Running status without waiting for a running container. So must wait explicitly
    # Unfortunately kubectl wait command doesn't accept the container option, adding another wrinkle to the opt parse
    waitable = remote
    options = ['--container', '-c']
    if container_option := next((x for x in remote.split() if x in options), None):
        i = remote.index(container_option)
        waitable = remote[:i] + remote[i:].split(maxsplit=2)[-1]
    elif container_option := next((x for x in waitable.split() if any((y for y in options if x.startswith(y)))), None):
        i = remote.index(container_option)
        waitable = remote[:i] + remote[i:].split(maxsplit=1)[-1]
    kubectl.stream('wait --for=condition=ready pod --timeout=120s', waitable)

    resp = kubectl.run('exec', remote, '-- du -sh', s_path, check=False)
    if resp.returncode:
        logger.error(resp.stderr)
        raise RuntimeError(resp.stderr)

    import shutil

    from .units import Unit

    dest_dir = os.path.dirname(dest)
    needed_disk =  Unit(resp.stdout.split()[0])
    available_disk = Unit(f'{shutil.disk_usage(dest_dir).free} B')

    if needed_disk > available_disk:
        msg = f'Insufficient space on local host\n{needed_disk=} > {available_disk=}'
        logger.error(msg)
        raise OSError(msg)

    kubectl.stream(f'cp --{retries=}', src, dest, mode=mode, show_cmd=show_)
    return


def _up(src: str, dest: str, mode: str, show_: bool, logger: logging.Logger, retries: int) -> None:
    source_exists = os.path.exists(src)
    if not source_exists and mode != 'dry-run':
        logger.error(f'FileNotFound: {src}')
        raise FileNotFoundError(f'{src} does not exist')

    Executable('kubectl').stream(f'cp --{retries=}', src, dest, mode=mode, show_cmd=show_)
    return


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
    """:return: kubectl version as a Version object"""
    result = Executable('kubectl').run('version --client --output json')
    return Version(json.loads(result.stdout)['clientVersion']['gitVersion'][1:])  # strip 'v' prefix


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


def _get_namespace_from_file(file: str | Path, logger: logging.Logger, mode: str) -> str:
    """Get namespace from file, create if not present in k8s
    :param file: file to get namespace from
    :param logger: logger object
    :param mode: execution mode
    :return: namespace
    """
    file_path = Path(file)
    namespace_from_file = file_path.stem.split('_')[-1]
    file_content = file_path.read_text().strip()
    if not file_content:
        logger.warning(f'File {file} is empty! Using value from filename..')
        return namespace_from_file
    try:
        content_j = json.loads(file_content)
    except json.decoder.JSONDecodeError as e:
        logger.warning(f'JSONDecodeError: {e}. This may happen if the file is not json. Using value from filename..')
        namespace = namespace_from_file
        if namespace == file_path.stem:
            logger.warning(f'Inferred namespace equals filename ({namespace}), which is suspect')
            if mode == 'normal':
                from . import confirm
                confirm.default('Namespace will be created if not present in k8s. Continue?')
        return namespace
    try:
        return content_j['items'][0]['metadata']['namespace']
    except KeyError:  # single item
        try:
            return content_j['metadata']['namespace']
        except KeyError:  # empty item
            return namespace_from_file
    except IndexError:  # empty list of k8s items
        return namespace_from_file
    except TypeError: # empty json list (TypeError: list indices must be integers or slices, not str)
        return namespace_from_file


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
        namespace = _get_namespace_from_file(file, logger, mode)
    namespace_exists = kubectl.run('get namespace', namespace, '-ojsonpath={.status.phase}', check=False)
    if namespace_exists.returncode == 0:  # success
        status = namespace_exists.stdout.strip()
        if status not in  ['Active', 'Terminating']:
            logger.warning(f'{namespace=}, {status=}, expected either "Active", "Terminating" or missing!')
            raise RuntimeError(f'{namespace=}, {status=}, expected either "Active", "Terminating" or missing!')
        if status == 'Terminating':
            logger.warning(f'{namespace=}, {status=}. Awaiting termination and recreating it')
            kubectl.stream('wait namespace', namespace, '--for=delete --timeout=600s')
            return ensure_namespace(mode, logger, namespace=namespace, file=file)
        return namespace
    if namespace_exists.stderr.startswith(RESOURCE_NOT_FOUND):
        logger.warning(f'{namespace=} not found, creating...')
        resp = kubectl.run('create namespace', namespace, ' --dry-run=client' if mode == 'dry-run' else '', check=False)
        if 'AlreadyExists' in resp.stderr:
            return ensure_namespace(mode, logger, namespace=namespace, file=file)
        elif resp.returncode:
            logger.warning(resp.stdout)
            raise RuntimeError(f'{namespace=} failed to create!\n{resp.stderr}')
        return namespace
    if not namespace_exists.stderr.startswith('Error from server (Forbidden)'):
        raise RuntimeError(namespace_exists.stderr)
    elif namespace == 'default-tenant':  # 'get ns' may fail due to permissions, but creating a job is still ok
        return namespace
    raise PermissionError(namespace_exists.stderr)


def ensure_pvc(
        spec: dict,
        logger: logging.Logger,
        _retries: int = 1  # noqa internal undocumented parameter for retrying PVC creation
) -> None:
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
        ensure_namespace(spec['MODE'], logger, namespace=spec['NAMESPACE'])
        import subprocess
        try:
            kubectl.stream('create --filename', path)
        except subprocess.CalledProcessError as e:
            if _retries and 'terminated' in e.stderr:
                log.get_logger(name='plain').error(e.stderr)
                ensure_pvc(spec, logger, 0)

    pvc_desired_states = [x for x in spec.get('PERSISTENT_VOLUME_CLAIM_DESIRED_STATES') or ['Bound']]
    pvc_name = spec['PERSISTENT_VOLUME_CLAIM_NAME']

    pvc_phase_jsonpath = 'jsonpath="{.status.phase}"'
    # Running "get" and then "wait" for backwards compatability.
    # kubectl in k8s 1.21 errors out on --for=jsonpath="{.status.phase}"=Bound for pvc
    # So we run "get" first, to allow run for existing bound pvc
    # On k8s 1.21 it will error out otherwise, on later versions "wait" works
    pvc_status: str = kubectl.run('get persistentvolumeclaim --output', pvc_phase_jsonpath, pvc_name).stdout
    if pvc_status in pvc_desired_states:
        return
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
    4. Redact saved job manifest YAML
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

    from .templates import batch_job, recursive_has_pair

    spec['JOB_NAME'], path = batch_job.generate_template(spec, manifests_folder, filename=container_name)
    if wait_offset := spec.get('WAIT_BEFORE_IMAGE_PULL_POLICY_ALWAYS', 0.1):
        if recursive_has_pair(spec, 'IMAGE_PULL_POLICY', 'Always'):
            from random import random
            sleep = wait_offset + random() * 10  # nosec CWE-330
            logger.info(f'pullImagePolicy=Always detected!\n{sleep=:.2f}s to avoid thundering herd DDoS')
            time.sleep(sleep)
    kubectl.stream('create --filename', path)
    log.redact_file(path, redact)

    if await_completion:
        await_k8s_job_completion(spec, completion_tail)
    return spec['JOB_NAME']


def await_k8s_job_completion(spec: dict, tail: Optional[int] = None) -> bool:
    """Wait for k8s job to complete
    :param spec: dict with job parameters. Must contain 'NAMESPACE', 'JOB_NAME', 'MODE' keys.
    :param tail: num of lines to print from job logs on completion. Defaults to k8s default
    """
    namespace = spec.get('NAMESPACE')
    if not namespace:
        raise ValueError('namespace not specified')
    name = spec.get('JOB_NAME')
    if not name:
        raise ValueError('job name not specified')
    mode = spec.get('MODE')
    if not mode:
        raise ValueError('mode not specified')
    job_timeout = spec.get('JOB_TIMEOUT') or '1h'

    logger = log.get_logger(name=spec.get('LOGGER_NAME'))
    logger_plain = log.get_logger('plain')
    job_status_cmd = 'get job --output jsonpath={.status} ' + name

    kubectl = Executable('kubectl', f'kubectl --namespace {namespace}')
    kubectl_run = functools.partial(kubectl.run, show_cmd=False, check=False)
    logger.info(f'Waiting for {name} to complete, {job_timeout=}')

    if mode == 'dry-run':
        return True

    response = kubectl_run(job_status_cmd)
    retry_total = retry_count = consts.RETRIES_DEFAULT
    while response.returncode and response.stderr.startswith(RESOURCE_NOT_FOUND) and retry_count:
        backoff = 2 ** (retry_total - retry_count)
        time.sleep(backoff)  # lazy man's exponential backoff
        retry_count -= 1
        logger_plain.warning(f'{response.stderr}\nWaiting {backoff}s')
        response = kubectl_run(job_status_cmd)

    get_pods_cmd = f'get pods --ignore-not-found --selector=job-name={name}'
    kubectl.stream( # todo: at this stage, imageID could still be blank. Move this downstream to improve chances.
        get_pods_cmd,
        r"""--output jsonpath='{range .items[*].status.containerStatuses[*]}{.image} -> {.imageID}{"\n"}{end}'""",
        show_cmd=False,
    )

    get_pods_cmd += ' --output wide' if spec.get('LOG_LEVEL') == 'DEBUG' else ''
    kubectl.stream(get_pods_cmd)

    import itertools
    from datetime import datetime

    # until kubectl wait gets support for multiple conditions (https://github.com/kubernetes/kubernetes/issues/95759)
    # we cycle between them as round-robin
    conditions = itertools.cycle(['condition=complete', 'condition=failed'])
    wait_interval = spec.get('WAIT_INTERVAL') or consts.WAIT_INTERVAL
    wait_job_cmd = f'wait job {name} --timeout={int(wait_interval)//2}s --for '

    response = kubectl_run(wait_job_cmd, next(conditions))
    resp = kubectl.run(get_pods_cmd).stdout.strip().splitlines()
    if len(resp) > 1:
        logger_plain.info('\n'.join(resp[1:]))
    while response.returncode:
        now = datetime.now()
        if now.minute < 1 and now.second < wait_interval % 60 + 1:  # hourly liveness
            resp = kubectl.run(get_pods_cmd).stdout.strip().splitlines()
            if len(resp) > 1:
                logger_plain.info('\n'.join(resp[1:]))
        if response.stderr.startswith(RESOURCE_NOT_FOUND):
            msg = f'{response.stderr}! Was the job deleted?'
            logger.warning(f'{msg}\nEvents:')
            print_namespace_events(namespace)
            raise RuntimeError(msg)
        if WAIT_TIMEOUT_ERROR not in response.stderr:
            logger_plain.warning(response.stderr)
            raise RuntimeError(response.stderr)

        response = kubectl_run(wait_job_cmd, next(conditions))

    resp = kubectl.run(get_pods_cmd).stdout.strip().splitlines()
    if len(resp) > 1:
        logger_plain.info('\n'.join(resp[1:]))

    kubectl.stream(f'logs --ignore-errors --selector=job-name={name} --since={int(wait_interval)*2}s',
                   f'--tail={tail}' if tail else '', show_cmd=False)
    try:
        terminal_status = json.loads(kubectl_run(job_status_cmd).stdout)
    except json.JSONDecodeError as e:
        msg = f'Failed to fetch job status'
        logger.warning(msg)
        raise RuntimeError(msg) from e

    if terminal_status.get('succeeded'):
        return True
    logger.warning('Terminal status:')
    log.log_as('json', terminal_status, printer=logger_plain.warning)
    if not terminal_status.get('failed'):
        logger.warning('Terminal status is unexpectedly neither succeeded nor failed')
    raise RuntimeError(f'{name=}, {terminal_status=}')


# deprecated
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
    # kubectl logs --pod-running-timeout=5m should wait for at least one pod running, but it doesn't for some reason.
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


def get_pod_name_and_job_image(
        selector: str, container: str, namespace: str, logger: logging.Logger,
        retries: Optional[int] = consts.RETRIES_DEFAULT, image_override: Optional[str] = None,
) -> Dict[str, str]:
    """Get pod name and job image from k8s
    :param selector:  selector
    :param container: container name
    :param namespace: k8s namespace
    :param logger: logger object
    :param retries: number of retries
    :param image_override:
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
    logger.info(f'Pod: {pod_name}\nJob image: {job_image}')
    if image_override:
        if image_override == job_image:
            logger.warning('image override provided, but it is identical to current job image. Skipping override...')
        else:
            logger.warning(f'{image_override=}\nOverriding...')
            job_image = image_override
    return {
        'POD_NAME': pod_name,
        'JOB_IMAGE': job_image,
    }


def _get_running_pod_manifest(kubectl: Executable, tries: int, retries: int, msg: Optional[str] = '',
                              logger: Optional[logging.Logger] = None) -> dict:
    """Get running pod manifest from k8s
    :param kubectl: kubectl Executable object
    :param tries: number of tries
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
    except TypeError as e:  # "'NoneType' object is not subscriptable" error means parsed JSON is None
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


def is_remote_sharing_disk_with_host(  # TODO: create test
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
    # -1 for single column, -A for all files except . / ..
    ls_job_name = create_oneliner_job(spec, f'ls -1A {remote_path or local_path}', 'ls', await_completion=True)
    kubectl = Executable('kubectl', 'kubectl logs --tail=-1 --namespace', spec['NAMESPACE'])
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


def fetch_from_image(namespace: str, image: str, source, target: str, mode: str) -> None:
    logger = log.get_logger()
    ensure_namespace(mode, logger, namespace=namespace)
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace)

    from random import getrandbits
    pod_name = Path(target).name.split('.', maxsplit=1)[0] + '-temp-' + str(getrandbits(16)) # nosec: B311

    kubectl.stream('delete pod --ignore-not-found --wait', pod_name, mode=mode)
    kubectl.stream('run --image-pull-policy=Always --image', image, pod_name, '--command -- sleep 3600', mode=mode)
    if mode == 'dry-run':
        return
    kubectl.stream('get pods', pod_name, '--output',
                   r"""jsonpath='{range .status.containerStatuses[*]}{.image} -> {.imageID}{"\n"}{end}'""")
    kubectl_cp(f'--{namespace=} {pod_name}:{source}', target, mode=mode)
    kubectl.stream('delete pod --ignore-not-found --wait=false', pod_name, mode=mode)


def prep_binary(mode: str, spec: dict, name: str, refresh_rate_default) -> str:
    """Locate a usable binary, fetch from image if missing. Order of precedence:

    1. name in manifest as {name.upper()}_PATH or {name}Path
    2. name in PATH
    3. name in temp_dir
    4. name in provided k8s image

    :param mode: execution mode
    :param spec: spec dict
    :param name: name of the binary
    :param refresh_rate_default: the rate at which binary will be refreshed from k8s image pull
    :raises: ValueError if path not found
    :raises: PermissionError if path not executable
    """
    import shutil
    path = spec.get(f'{name}_PATH'.upper()) or spec.get(f'{name}Path') or shutil.which(name)

    if not path:
        temp_dir = spec['CACHE_FOLDER']
        image = spec['JOB_IMAGE']
        namespace = spec['NAMESPACE']
        path_on_image = spec['PATH_ON_IMAGE']
        image_pull_policy = spec.get('IMAGE_PULL_POLICY')

        logger = log.get_logger()
        logger.warning(f'{name} path not set in manifest or found in PATH. Checking in temp dir')
        path = os.path.join(temp_dir, name)

        set_image_pull_policy_default(spec, refresh_rate_default)
        if image_pull_policy == 'Always':
            logger.warning(f'Force updating from k8s image')
            swap_path = path + '.swap'
            fetch_from_image(namespace, image, path_on_image, swap_path, mode)
            if mode == 'dry-run':
                return path
            shutil.move(swap_path, path)
        if not os.path.exists(path):
            logger.warning(f'{name} path not found in temp dir, fetching from k8s image')
            fetch_from_image(namespace, image, path_on_image, path, mode)
            if mode == 'dry-run':
                return path
    if not path:
        raise ValueError(f'{name} executable not found')
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    if not os.access(path, os.X_OK):
        import subprocess
        subprocess.run(f'chmod +x {path}', shell=True)
    if not os.access(path, os.X_OK):
        raise PermissionError(f'{name} path is not executable: {path}')
    return path


# Edge case, which looks like a bug:
#  1. node1 runs with "Always" and updates version. Next run:
#  2. node2 runs with "IfNotPresent" and doesn't update
#  This results in a later run will surprisingly use an older version.
#  Assuming the older version is still good, the run will be successful.
#  It's not ideal but implementing a solution here, we can always ensure version is latest by manually setting "Always"
def set_image_pull_policy_default(spec: dict, refresh_rate_default: float):
    from random import random
    from basepak.templates import recursive_has_pair

    if random() * 99.99 > spec.get('REFRESH_RATE', refresh_rate_default):  # nosec: B311
        return
    spec.setdefault('IMAGE_PULL_POLICY', 'Always')

    wait_offset = spec.get('WAIT_BEFORE_IMAGE_PULL_POLICY_ALWAYS', 0.1)
    if not (wait_offset and recursive_has_pair(spec, 'IMAGE_PULL_POLICY', 'Always')):
        return
    sleep = wait_offset + random() * 10  # nosec CWE-330
    logger = log.get_logger()
    logger.info(f'pullImagePolicy=Always detected!\n{sleep=:.2f}s to avoid thundering herd DDoS')
    if spec.get('MODE', '') == 'dry-run':
        logger.info('Dry run. Skipping wait..')
        return

    time.sleep(sleep)


def get_size_on_remote(spec, path) -> str:
    """Calculate the size of a file/dir on a remote fs

    :param spec: spec dict
    :param path: the remote path to measure
    :return: du -sh size result or empty string
    """
    if spec['DISK_TOTALS'] not in ['yes', 'remote']:
        return ''
    logger = log.get_logger('plain')
    kubectl = Executable('kubectl', f'kubectl --namespace', spec['NAMESPACE'], logger=logger)
    name = create_oneliner_job(spec, command='du -sh {}'.format(path), container_name='du', await_completion=True)

    resp = kubectl.run(f'logs --ignore-errors --selector=job-{name=}', check=False)
    logger.error(resp.stderr)
    logger.info(resp.stdout)
    if size := next((x.strip() for x in resp.stdout.strip().splitlines() if x), ''):
        return size.split()[0]
    return ''
