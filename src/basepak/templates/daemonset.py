from __future__ import annotations

from pathlib import Path
from typing import Optional

from .. import consts
from .. import configer


def generate_template(params: dict[str, any], dump_folder: Optional[str | Path] = None, filename: Optional[str] = None):
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
    template_daemonset = {
        'apiVersion': 'apps/v1',
        'kind': 'DaemonSet',
        'metadata': {
            'name': 'journal-monitor',
            'namespace': params['NAMESPACE'],
            'labels': consts.DEFAULT_LABELS | params['METADATA'].get('labels', {}) | {consts.IS_PURGEABLE_KEY: 'true'}},
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
