from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Optional


def generate_template(params: Mapping, dump_folder: Optional[str | Path] = None, filename: Optional[str] = None) -> str:
    """Generate a k8s DaemonSet template
    :param params: daemonset parameters
    :param dump_folder: target folder
    :param filename: manifest filename
    :return: daemonset name
    """
    from .. import configer, consts
    affinity = {}
    if params.get('NODE_NAMES'):
        affinity['affinity'] = {
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            'key': 'kubernetes.io/hostname',
                            'operator': 'In',
                            'values': params['NODE_NAMES']
                        }]}]}}}
    user_labels = params.get('METADATA', {}).get('labels', {}) | params.get('metadata', {}).get('labels', {})
    user_labels.setdefault(consts.IS_PURGEABLE_KEY, 'true')
    template_daemonset = {
        'apiVersion': 'apps/v1',
        'kind': 'DaemonSet',
        'metadata': {
            'name': params.get('DAEMONSET_NAME') or 'journal-monitor',
            'namespace': params['NAMESPACE'],
            'labels': consts.DEFAULT_LABELS | user_labels,
        },
        'spec': {
            'selector': {
                'matchLabels': {
                    'command': 'journalctl',
                }},
            'template': {
                'metadata': {
                    'labels': {
                        'command': 'journalctl',
                    }},
                'spec': {
                    'containers': [{
                        'name': 'journalctl-follower',
                        'image': 'rockylinux:8',
                        'volumeMounts': [{
                                'name': 'var-log-journal',
                                'mountPath': '/var/log/journal',
                        }],
                        'command': params.get('COMMAND') or [],
                        'args': params.get('ARGS') or [],
                        'env': params.get('ENV_VARS') or [],
                        'securityContext': {
                            'capabilities': {
                                'add': ['CAP_DAC_READ_SEARCH'],
                            }}}],
                    'volumes': [{
                        'name': 'var-log-journal',
                        'hostPath': {
                            'path': '/var/log/journal',
                        }}],
                    **affinity,
                }}}}

    configer.generate(template_daemonset, dump_folder, filename=filename)
    return template_daemonset['metadata']['name']
