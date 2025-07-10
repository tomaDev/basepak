from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Optional

POD_SPEC_DEFAULT = {
    # OnFailure - container restarts in the same pod on the same node.
    # Never - container restarts in a new pod. This is preferred, as switching nodes may solve the issue
    'restartPolicy': 'Never',
    'affinity': {
        'nodeAffinity': {
            'preferredDuringSchedulingIgnoredDuringExecution': [{
                'weight': 100,
                'preference': {
                    'matchExpressions': [{
                        'key': 'node-role.kubernetes.io/master',
                        'operator': 'DoesNotExist',
                    }, {
                        'key': 'node-role.kubernetes.io/control-plane',
                        'operator': 'DoesNotExist',
                    }, {'key': 'eks.amazonaws.com/capacityType',
                        'operator': 'NotIn',
                        'values': [
                            'ON_DEMAND',
                            ],
                    },
                    ]}}
            ]}}
}


def generate_template(
        params: Mapping, dump_folder: Optional[str | Path] = None, filename: Optional[str] = None
) -> tuple[str, str]:
    """Generate a k8s Job template
    :param params: job parameters
    :param dump_folder: target folder
    :param filename: manifest filename
    :return: job name, path to template
    """
    import os

    from .. import configer, consts, strings, time
    security_context = {} if params.get('-securityContext') is False else {
        'securityContext': params.get('-securityContext') or {  # False infers user input. None infers missing
            'runAsUser': params.get('RUN_AS_USER') or os.geteuid(),
            'runAsGroup': params.get('RUN_AS_GROUP') or os.getgid(),
        },
    }
    pod_spec = POD_SPEC_DEFAULT.copy()
    if params.get('RESTART_POLICY'):
        pod_spec['restartPolicy'] = params['RESTART_POLICY']
    if params.get('NODE_NAMES'):
        pod_spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution'] = {  # type: ignore
            'nodeSelectorTerms': [{
                'matchExpressions': [{
                    'key': 'kubernetes.io/hostname',
                    'operator': 'In',
                    'values': params['NODE_NAMES']
                }]}]}
    if params.get('-podSpec'):
        pod_spec.update(params['-podSpec'])

    job_name = strings.truncate_middle(params['JOB_NAME'])
    user_labels = params.get('METADATA', {}).get('labels', {}) | params.get('metadata', {}).get('labels', {})
    user_labels.setdefault(consts.IS_PURGEABLE_KEY, 'true')
    volume_name = params.get('VOLUME_NAME') or 'default-volume-name'
    template_batch_job = {
        'apiVersion': 'batch/v1',
        'kind': 'Job',
        'metadata': {
            'name': job_name,
            'namespace': params['NAMESPACE'],
            'labels': consts.DEFAULT_LABELS | user_labels,
            },
        'spec': {
            'ttlSecondsAfterFinished': time.str_to_seconds(params['RETENTION_PERIOD']),
            'activeDeadlineSeconds': time.str_to_seconds(params['JOB_TIMEOUT']),
            'template': {
                'spec': {
                    'containers': [{
                        'name': params.get('CONTAINER_NAME') or job_name,
                        # stable instead of latest, to avoid unnecessary pulls
                        'image': params.get('JOB_IMAGE') or params.get('DEFAULT_IMAGE') or 'busybox:stable',
                        'volumeMounts': [{
                                'name': volume_name,
                                'mountPath': params['JOB_MOUNT_PATH'],
                        }],
                        'command': params.get('COMMAND') or [],
                        'args': params.get('ARGS') or [],
                        'env': params.get('ENV_VARS') or [],
                        **security_context,
                    }],
                    **pod_spec,
                    'volumes': [{
                        'name': volume_name,
                        'persistentVolumeClaim': {
                            'claimName': params['PERSISTENT_VOLUME_CLAIM_NAME'],
                        }}]}}}}

    return job_name, configer.generate(template_batch_job, dump_folder, filename=filename)
