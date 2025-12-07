"""Functions for creating and managing k8s resources"""
from __future__ import annotations

import copy
import functools
import json
import logging
import os
import platform
import re
import subprocess
from collections import namedtuple
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Dict, Optional, Set, Union, List, Iterable
from os import PathLike
from . import consts, log, time
from .execute import Executable, subprocess_stream
from .versioning import Version


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


def kubectl_dump(command: PathLike | Executable | str, output_file: PathLike, mode: str = 'dry-run') -> None:
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


def _parse_remote_path(_str: PathLike | str) -> tuple[str, str]:
    """Parse remote path into host and path parts

    :returns: host, path"""
    remote_path = str(_str)
    if not remote_path:
        raise ValueError('Empty string received')
    if ':' not in remote_path:
        return remote_path, ''
    remote_part, path_part = remote_path.split(':', maxsplit=1)
    path_split = path_part.split()
    if len(path_split) == 0:
        return remote_part, ''
    if len(path_split) == 1:
        return remote_part, path_part
    return ' '.join([remote_part, *path_split[1:]]), path_split[0]


def kubectl_cp(
        src: PathLike | str,
        dest: PathLike | str,
        mode: Optional[str] = 'dry-run',
        show_cmd: bool = True,
        logger: Optional[logging.Logger] = None,
        retries: Optional[int] = 3,
) -> int | list[tuple[int, int]]:
    """wrapper on top of `kubectl cp` with more error handling and remote-to-remote transfer functionality

    :param src: source path. Can be local or remote
    :param dest: target path. Can be local or remote
    :param mode: execution mode. 'dry-run' only shows command, any other mode executes
    :param show_cmd: if True, logs the commands that are executed
    :param logger: logger object
    :param retries: retry attempts on failure
    :return: 0 on success, error exit code on failure
    :except ValueError: if src and dest are local paths
    """

    src_str = str(src)
    dest_str = str(dest)

    logger = logger or log.get_logger(name='plain')

    is_remote_src = ':' in src_str
    is_remote_dest = ':' in dest_str

    if not (is_remote_src or is_remote_dest):
        import inspect
        banner = (
            'Char ":" not found in source/dest, suggesting they are both local paths. '
            f'For local file transfer, please avoid using {inspect.currentframe().f_code.co_name}'
        )
        logger.warning(banner)
        raise ValueError(banner)  # implementing this encourages bad boundaries, so we error out instead

    if is_remote_src and is_remote_dest:
        return _remote_to_remote(
            src_str,
            dest_str,
            mode=mode or 'normal',
            show_=show_cmd,
            logger=logger,
            retries=retries or 1,
        )

    if is_remote_src:
        return _dl(src_str, dest_str, mode=mode or 'normal', show_=show_cmd, logger=logger, retries=retries or 1)

    if is_remote_dest:
        return _up(src_str, dest_str, mode=mode or 'normal', show_=show_cmd, logger=logger, retries=retries or 1)

    raise RuntimeError('Unexpected kubectl_cp code path')


def _dl(src: str, dest: str, mode: str, show_: bool, logger: logging.Logger, retries: int) -> int:
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
    logger.debug(resp.stdout)
    logger.debug(resp.stderr)

    if resp.returncode:
        raise RuntimeError(resp.stderr)


    dest_dir = os.path.dirname(dest)
    logger.debug(f'{dest_dir=}')

    import shutil
    from .units import Unit

    try:
        available_disk = Unit(f'{shutil.disk_usage(dest_dir).free} B')
    except Exception as e:
        logger.error(e)
        raise e

    try:
        needed_disk = Unit(resp.stdout.split()[0])
    except Exception as e:
        logger.error(e)
        raise e

    try:
        if needed_disk > available_disk:
            raise OSError(f'Insufficient space on local host\n{needed_disk=} > {available_disk=}')
    except Exception as e:
        logger.error(e)
        raise e

    kubectl.stream(f'cp --{retries=}', src, dest, mode=mode, show_cmd=show_)
    return 0


def _up(src: str, dest: str, mode: str, show_: bool, logger: logging.Logger, retries: int) -> int:
    source_exists = os.path.exists(src)
    if not source_exists and mode != 'dry-run':
        logger.error(f'FileNotFound: {src}')
        raise FileNotFoundError(f'{src} does not exist')

    Executable('kubectl').stream(f'cp --{retries=}', src, dest, mode=mode, show_cmd=show_)
    return 0

def _remote_to_remote(
        src: str,
        dest: str,
        mode: str,
        show_: bool,
        logger: logging.Logger,
        retries: int,
) -> int | list(tuple[int, int]):
    """Copy between two remote pods in arbitrary namespaces without touching local disk
    """

    src_remote, src_path = _parse_remote_path(src)
    dest_remote, dest_path = _parse_remote_path(dest)

    if not src_path:
        logger.error(f'Invalid remote src path: {src!r}')
        return 1

    src_parent, src_name = os.path.split(src_path)
    if not src_parent:
        src_parent = '/'

    if dest_path.endswith('/'):
        dest_parent = dest_path.rstrip('/') or '/'
    else:
        dest_parent = os.path.dirname(dest_path) or '/'

    if mode == 'dry-run':
        if show_:
            logger.info(f'DRY-RUN remote->remote: {src_remote}:{src_path} -> {dest_remote}:{dest_path}')
            logger.info(
                'DRY-RUN would run:\n'
                f'  kubectl exec {src_remote} -- tar cf - -C {src_parent} {src_name} | '
                f'kubectl exec -i {dest_remote} -- tar xf - -C {dest_parent}',
            )
        return

    attempt = 0
    max_attempts = max(1, retries)

    exit_codes = list()

    while True:
        attempt += 1
        if show_:
            logger.info(f'Attempt {attempt}/{max_attempts}: {src_remote}:{src_path} -> {dest_remote}:{dest_path}')

        src_proc = dest_proc = None
        try:
            src_proc = subprocess.Popen(
                ['kubectl', 'exec', *src_remote.split(), '--', 'tar', 'cf', '-', '-C', src_parent, src_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
            )
            dest_proc = subprocess.Popen(
                ['kubectl', 'exec', '-i', *dest_remote.split(), '--', 'tar', 'xf', '-', '-C', dest_parent],
                stdin=src_proc.stdout,
                stderr=subprocess.PIPE,
                text=False,
            )

            if src_proc.stdout is not None:  # Allow src_proc to receive SIGPIPE if dest exits early
                src_proc.stdout.close()

            dest_rc = dest_proc.wait()
            src_rc = src_proc.wait()

            if src_rc == 0 and dest_rc == 0:
                return 0
            exit_codes.append(tuple(src_rc, dest_rc))

            stderr_src = src_proc.stderr.read().decode('utf-8', 'replace') if src_proc.stderr else ''
            stderr_dest = dest_proc.stderr.read().decode('utf-8', 'replace') if dest_proc.stderr else ''

            logger.error(f'remote->remote copy failed ({src_rc=}, {dest_rc=})\n{stderr_src=}\n{stderr_dest=}')

        finally:  # prevent leaking procs
            for p in (src_proc, dest_proc):
                if p and p.poll() is None:
                    p.kill()

        if attempt >= max_attempts:
            logger.error(f'remote->remote copy failed after {max_attempts} attempts: {src} -> {dest}')
            return exit_codes


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
    except Exception as e:  # noqa called on a best effort basis
        log.get_logger(name='plain').debug(e)


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
    :raises ValueError: if missing parameters from spec
    :raises RuntimeError: if k8s job failed to complete
    :return: True if k8s job succeeded
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
            msg = f'{response.stderr}\nWas the job deleted?'
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


# todo: add tests
def scale_resources_to_zero(
        resources: Mapping, prefix: str, namespace: Optional[str] = 'default-tenant', mode: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
) -> None:
    """Scale to zero given resources

    :param resources:
    :param prefix: string prefix for spec lookups
    :param namespace: namespace of the resources
    :param mode: execution mode
    :param logger: logger object
    """
    logger_plain = log.get_logger('plain')
    kubectl = Executable('kubectl', 'kubectl -n', namespace, logger=logger_plain)
    labels_to_await = []
    mode = mode or 'dry-run'
    logger = logger or log.get_logger()
    if not resources.get(prefix + '_SERVICE_REPLICAS_SCALE_TO_ZERO', True):
        logger.warning('service replicas scale to zero is disabled')
        return
    if mode == 'normal':
        from basepak import confirm
        confirm.default()

    dry_run_option = '--dry-run=server' if mode == 'dry-run' else ''
    if labels := resources.get('LABEL_SELECTOR_DEPLOYMENTS'):
        if isinstance(labels, str):
            labels = [labels]
        for selector in labels:
            if not kubectl.run(f'get deployment --{selector=}', check=False).stdout.strip():
                logger.warning(f'No deployment found in --{namespace=} --{selector=}')
                logger.warning(f'Skipping scale...')
                continue
            labels_to_await.append(selector)
            kubectl.stream(f'scale deployment --replicas=0 --{selector=}', dry_run_option)

    if labels := resources.get('LABEL_SELECTOR_STATEFULSETS'):
        if isinstance(labels, str):
            labels = [labels]
        for selector in labels:
            if not kubectl.run(f'get statefulset --{selector=}', check=False).stdout.strip():
                logger.warning(f'No statefulset found in --{namespace=} --{selector=}')
                logger.warning(f'Skipping scale...')
                continue
            labels_to_await.append(selector)
            kubectl.stream(f'scale statefulset --replicas=0 --{selector=}', dry_run_option)

    logger.info('Waiting for pods to terminate')
    if mode == 'dry-run':
        return
    from time import monotonic
    start_time = monotonic()
    for selector in labels_to_await:
        get_pods_cmd = f'get pods --{selector=}'
        pods = kubectl.run(get_pods_cmd).stdout.strip()
        while pods:
            logger_plain.info(pods)
            if start_time - monotonic() > 3600:
                raise TimeoutError(f'Termination timeout for pods at --{namespace=} --{selector=} ')
            time.sleep(10)
            pod_lines = kubectl.run(get_pods_cmd).stdout.strip().splitlines()
            if len(pod_lines) <= 1:
                break
            pods = '\n'.join(pod_lines[1:])


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

Partition = namedtuple("Partition", "device mountpoint fstype opts")


_OCT_ESC_RE = re.compile(r'\\([0-7]{3})')

def _unescape_proc_mounts(s: str) -> str:
    # /proc/mounts escapes space, tab, newline, and backslash as octal.
    return _OCT_ESC_RE.sub(lambda m: chr(int(m.group(1), 8)), s)

def _linux_partitions_all() -> List[Partition]:
    path = "/proc/self/mounts" if os.path.exists("/proc/self/mounts") else "/proc/mounts"
    parts: List[Partition] = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            cols = line.rstrip("\n").split()
            if len(cols) < 4:
                continue
            dev = _unescape_proc_mounts(cols[0])
            mnt = _unescape_proc_mounts(cols[1])
            fstype = cols[2]
            opts = cols[3]
            parts.append(Partition(dev, mnt, fstype, opts))
    return parts

def _macos_partitions_all() -> List[Partition]:
    # Example line:
    # /dev/disk3s1 on / (apfs, sealed, local, read-only, journaled)
    # map -hosts on /net (autofs, nosuid, automounted, nobrowse)
    out = subprocess.check_output(["mount"], text=True, encoding="utf-8", errors="replace")
    parts: List[Partition] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^(.*?) on (.*?) \((.*?)\)$", line)
        if not m:
            continue
        dev, mnt, opts_str = m.groups()
        fstype = opts_str.split(",")[0].strip() if opts_str else ""
        # normalize options formatting similar to /proc/mounts (comma-separated)
        opts_norm = opts_str.replace(", ", ",") if opts_str else ""
        parts.append(Partition(dev, mnt, fstype, opts_norm))
    return parts

def disk_partitions_all() -> List[Partition]:
    sys = platform.system()
    if sys == "Linux":
        return _linux_partitions_all()
    if sys == "Darwin":
        return _macos_partitions_all()
    raise NotImplementedError("Only Linux and macOS are supported")

_NON_LOCAL_FSTYPES = {
    # Linux/Unix network & cluster FS
    "nfs", "nfs4", "cifs", "smbfs", "9p",
    "glusterfs", "ceph", "cephfs", "lustre", "ocfs2", "gfs2", "afs",
    # FUSE remotes
    "sshfs", "fuse.sshfs", "fuse.s3fs", "gcsfuse", "fuse.rclone",
    "fuse.davfs", "davfs", "fuse.gvfsd-fuse", "fuse.cgofuse",
    # Autofs maps (often remote)
    "autofs",
}

def _is_non_local_fstype(fs: str) -> bool:
    fs_l = (fs or "").lower()
    # any unknown FUSE is suspiciously remote
    return fs_l in _NON_LOCAL_FSTYPES or fs_l.startswith("fuse.")

def _looks_like_network_device(dev: str) -> bool:
    """
    Heuristics that flag network devices:
      - //server/share (CIFS/SMB)
      - server:/export (NFS)
      - scheme://host/path
    """
    if not dev:
        return False
    d = dev.lower()
    if d.startswith("//"):
        return True
    if "://" in d:
        return True
    if ":" in dev and not dev.startswith("/"):
        return True
    return False

def is_path_local_best_effort(
        path: str,
        partitions: Optional[Iterable[Partition]] = None,
) -> bool:
    """
    Best-effort: True if path resides on a local filesystem, False if on network/cluster FS.
    - Picks the deepest matching mountpoint.
    - Classifies using fstype & device heuristics.
    """
    # normalize the path (resolve symlinks so we match the actual mount)
    path = os.path.realpath(path)
    parts = list(partitions) if partitions is not None else disk_partitions_all()
    if not parts:
        return True  # if we can't tell, assume local

    best: Optional[Partition] = None
    best_len = -1

    for p in parts:
        mnt = os.path.normpath(p.mountpoint or "")
        if not mnt:
            continue
        # boundary-aware prefix match
        if path == mnt or path.startswith(mnt.rstrip(os.sep) + os.sep):
            if len(mnt) > best_len:
                best = p
                best_len = len(mnt)

    if best is None:
        return True  # no matching mount -> assume local

    if _is_non_local_fstype(best.fstype):
        return False
    if _looks_like_network_device(best.device):
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

        logger = log.get_logger()
        logger.warning(f'{name} path not set in manifest or found in PATH. Checking in temp dir')
        path = os.path.join(temp_dir, name)

        set_image_pull_policy_default(spec, refresh_rate_default)
        if spec.get('IMAGE_PULL_POLICY') == 'Always':
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
    spec = copy.deepcopy(spec)
    spec.pop('JOB_NAME', None)
    spec.pop('JOB_IMAGE', None)
    spec.pop('ARGS', None)
    spec.pop('ENV_VARS', None)
    if spec['DISK_TOTALS'] not in ['yes', 'remote']:
        return ''
    logger = log.get_logger('plain')
    kubectl = Executable('kubectl', f'kubectl --namespace', spec['NAMESPACE'], logger=logger)
    name = create_oneliner_job(spec, command='du -sh {}'.format(path), container_name='du', await_completion=True)

    resp = kubectl.run(f'logs --ignore-errors --selector=job-{name=}', check=False)
    logger.error(resp.stderr)
    logger.info(resp.stdout)
    if size := next((x.strip() for x in resp.stdout.strip().splitlines() if x), ''):
        return size.split()[0].strip().replace("'", "")
    return ''


def get_job_latest_pod_container_returncode(kubectl: Executable | str, job_name: str, container_name: str) -> int:
    """
    Get the exit code(s) of the latest pod for a given Job/container.

    :param kubectl: Executable wrapper for kubectl.
    :param job_name: Name of the Job (used in the pod selector).
    :param container_name: Name of the container in the pod.
    :return: Exit code as an int.
    :raise RuntimeError: If stdout is empty or not castable to int.
    """
    jsonpath = (
            "{range .items[-1:]}{range .status.containerStatuses[?(@.name==\"" + container_name +
            "\")]}{.state.terminated.exitCode}{.lastState.terminated.exitCode}{end}{end}"
    )
    if isinstance(kubectl, str):
        kubectl = Executable('kubectl', kubectl, logger=log.get_logger(name='plain'))

    resp = kubectl.run("get pods --sort-by=.metadata.creationTimestamp --selector",
                       f"job-name={job_name} --output=jsonpath='{jsonpath}'")
    out = resp.stdout.strip()
    print('result:', out)
    if not out:
        raise RuntimeError(f"{job_name=}, {container_name=}: Failed to fetch return code! {resp.stderr}")
    try:
        return int(out)
    except ValueError as e:
        raise RuntimeError(
            f"{job_name=}, {container_name=}: Non-integer exit code output: {out!r}\n{resp.stderr}"
        ) from e
