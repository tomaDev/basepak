"""Functions for creating and managing k8s resources

Intended job flow
1. Ensure namespace
2. Ensure PVC (including creating PV if needed)
3. Create job
4. Redact saved job manifest yaml
5. Await job completion (can be separated out to Task validate phase)
"""
from __future__ import annotations

import functools
import json
import logging
import os
from pathlib import Path
from typing import Set, Iterable, Dict, Optional

from . import consts, time, log
from .execute import Executable, subprocess_stream
from .versioning import Version

DATE_FORMAT_DEFAULT = '%Y-%m-%dT%H:%M:%SZ'
EVENTS_WINDOW_DEFAULT = '1 hour'
RESOURCE_NOT_FOUND = 'Error from server (NotFound)'


def kubectl_dump(command: str | Executable, output_file: str | Path, mode: str = 'dry-run') -> None:
    """Runs kubectl command and saves output to file

    :param command: kubectl command to run
    :param output_file: file to save output to
    :param mode: 'dry-run' or 'normal'
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
    if os.path.getsize(error_file) == 0:
        os.remove(error_file)


def print_namespace_events(namespace: str) -> None:
    """Print k8s events for a namespace, sorted by creation time, supports kubectl v1.21 and later

    :param namespace: k8s namespace"""
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace, logger=log.get_logger(name='plain'))
    cmd = 'events'
    if get_kubectl_version() < Version('1.23'):
        cmd = 'get events --sort-by=.metadata.creationTimestamp'
    elif get_kubectl_version() < Version('1.26'):
        cmd = 'alpha events'
    try:
        kubectl.stream(cmd)
    except:  # noqa called on a best effort basis
        pass


@functools.lru_cache()
def get_kubectl_version() -> Version:
    """Get kubectl version

    :return: kubectl version as a Version object"""
    kubectl = Executable('kubectl', logger=log.get_logger(name='plain'))
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
    logger = log.get_logger(name='plain')
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace, 'get service', service_name, logger=logger)
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
                     file: Optional[str | Path] = None) -> str:
    """Ensure namespace exists in k8s, create if not present
    :param mode: execution mode
    :param logger: logger object
    :param namespace: namespace string
    :param file: file to get namespace from. If specified, namespace param is ignored
    :return: namespace
    """
    kubectl = Executable('kubectl', logger=log.get_logger('plain'))
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
    logger_plain = log.get_logger('plain')

    ensure_namespace(spec['MODE'], logger, namespace=spec['NAMESPACE'])
    if spec['MODE'] == 'dry-run':
        return
    kubectl = Executable('kubectl', 'kubectl --namespace', spec['NAMESPACE'], logger=logger_plain)

    if kubectl.run('get persistentvolumeclaim', spec['PERSISTENT_VOLUME_CLAIM_NAME'], check=False).stdout:
        logger.debug(f'persistentvolumeclaim {spec["PERSISTENT_VOLUME_CLAIM_NAME"]} exists\nSkipping creation..')
    else:
        logger.info('Creating persistent volume claim')
        from .templates import persistent_volume_claim
        persistent_volume_claim.generate_template(spec)
        pvc_manifest_path = os.path.join(spec['GENERATED_MANIFESTS_FOLDER'], 'persistent-volume-claim.yaml')
        kubectl.stream(f'apply --filename={pvc_manifest_path}')

    if pvc_single_desired_state := spec.get('PERSISTENT_VOLUME_CLAIM_DESIRED_STATE'):
        pvc_desired_states = [pvc_single_desired_state]
    else:
        pvc_desired_states = spec.get('PERSISTENT_VOLUME_CLAIM_DESIRED_STATES') or ['Bound']
    pvc_name = spec['PERSISTENT_VOLUME_CLAIM_NAME']

    pvc_phase_jsonpath = 'jsonpath="{.status.phase}"'
    # Running "get" and then "wait" for backwards compatability.
    # kubectl in k8s 1.21 errors out on --for=jsonpath="{.status.phase}"=Bound for pvc
    # So we run "get" first, to allow run for existing bound pvc
    # On k8s 1.21 it will error out otherwise, on later versions "wait" works
    pvc_status = kubectl.run('get persistentvolumeclaim --output', pvc_phase_jsonpath, pvc_name)
    if pvc_status.stdout not in pvc_desired_states:
        logger.warning(f'{pvc_status.stdout=}\nAwaiting state change. Desired options: {" ".join(pvc_desired_states)}')

        kubectl.set_args('wait persistentvolumeclaim', pvc_name, '--timeout=15s')
        for phase in pvc_desired_states + pvc_desired_states:  # checking every phase twice to avoid change drift
            out = kubectl.run(f'--for={pvc_phase_jsonpath}={phase}', show_cmd_level='warning')
            if not out.returncode:
                logger.info(f'PVC {pvc_name} is in desired state: {phase}')
                return
        kubectl.set_args('')
        pvc_status = kubectl.run('get persistentvolumeclaim --output', pvc_phase_jsonpath, pvc_name)
        raise RuntimeError(f'PVC {pvc_name} state: {pvc_status}! Desired options: {" ".join(pvc_desired_states)}')


def ensure_daemonset(spec: dict, logger: logging.Logger) -> None:
    """Ensure DaemonSet exists in k8s, create if not present
    :param spec: dict with DaemonSet parameters
    :param logger: logger object
    """
    logger_plain = log.get_logger('plain')
    namespace = spec['NAMESPACE']
    ensure_namespace(spec['MODE'], logger, namespace=namespace)
    ds = spec['DAEMONSET_NAME']
    kubectl = Executable('kubectl', logger=logger_plain)
    get_status = Executable('get_status', 'kubectl get daemonset', ds, '--output jsonpath={.status}',
                            ' --namespace', namespace, logger=logger_plain)
    resp = get_status.run(check=False)
    if resp.returncode:
        if resp.stderr.startswith(RESOURCE_NOT_FOUND):
            logger.info(f'{ds=} not found in {namespace=}. Creating...')
            if spec['MODE'] != 'dry-run':
                filename = 'journal-monitor'
                from .templates import daemonset
                daemonset.generate_template(spec, dump_folder=spec['CACHE_FOLDER'], filename=filename)
                kubectl.run('create --filename', str(spec['CACHE_FOLDER'] / f'{filename}.yaml'))
                resp = get_status.run()
        # todo: add support for other errors
        else:
            raise RuntimeError(resp.stderr)
    if spec['MODE'] == 'dry-run':
        return
    ds_status = json.loads(resp.stdout)
    retries = 3
    interval = 10
    while ds_status['desiredNumberScheduled'] != ds_status['numberReady'] and retries:
        log.log_as('json', ds_status, printer=logger_plain.info)
        logger.info('Waiting for desiredNumberScheduled == numberReady')
        retries -= 1
        time.sleep(interval)
        ds_status = json.loads(get_status.run().stdout)
    if not retries:
        raise RuntimeError(f'{ds=} not ready after {retries=} with {interval=} seconds')


def create_oneliner_job(spec: dict, command: str | Executable, container_name: str,
                        await_completion: Optional[bool] = False, mode: Optional[str] = 'normal') -> str:
    """Create a k8s job that runs a single command
    :param spec: dict with job parameters
    :param command: command to run in the job
    :param container_name: container name
    :param await_completion: wait for job completion
    :param mode: execution mode
    :return: job name
    """
    from .templates import batch_job
    logger = log.get_logger(name=spec.get('LOGGER_NAME'), level=spec.get('LOG_LEVEL') or 'INFO')
    ensure_pvc(spec, logger)
    spec = spec.copy()
    spec.update({
        'JOB_NAME': spec.get('JOB_NAME') or spec['INSTANCE_NAME'] + f'-{container_name}',
        'MODE': mode,
        'CONTAINER_NAME': container_name,
        'COMMAND': ['sh', '-c', f'{command}'],
    })
    manifests_folder = spec.setdefault('GENERATED_MANIFESTS_FOLDER', spec['CACHE_FOLDER'])
    yaml_path = Path(manifests_folder).joinpath(f'{container_name}.yaml')
    kubectl = Executable('kubectl', f'kubectl create --filename {yaml_path}', logger=log.get_logger('plain'))
    if mode != 'dry-run':
        spec['JOB_NAME'] = batch_job.generate_template(spec, manifests_folder, filename=container_name)
        kubectl.stream()
        if await_completion:
            await_k8s_job_completion(spec)
    return spec['JOB_NAME']


def await_k8s_job_completion(spec: dict) -> bool:
    """Wait for k8s job to complete
    :param spec: dict with job parameters"""
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

    kubectl = Executable('kubectl', f'kubectl --namespace {namespace}', logger=logger_plain)
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
        if 'gibby' in name:  # remove when gibby errors on 'Task failed and retry limit has been reached' and not hang
            retry_count = _check_gibby_logs_for_container_hang(name, namespace, kubectl, retry_count, logger_plain)
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
    kubectl.stream(f'logs --ignore-errors --selector=job-name={name} --since={int(wait_interval)*2}s', show_cmd=False)
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
    logger_plain = log.get_logger(name='plain', level=logger.level)
    kubectl = Executable('kubectl', 'kubectl --namespace', namespace, logger=logger_plain)
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
