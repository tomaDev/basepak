from typing import Dict

from .. import consts, configer


def generate_template(params: Dict[str, any]):
    template_persistent_volume_claim = {
        'apiVersion': 'v1',
        'kind': 'PersistentVolumeClaim',
        'metadata': {
            'name': params['PERSISTENT_VOLUME_CLAIM_NAME'],
            'namespace': params['NAMESPACE'],
            'labels': consts.DEFAULT_LABELS | params['METADATA'].get('labels', {}),
        },
        'spec': {
            'storageClassName': params.get('STORAGE_CLASS', consts.STORAGE_CLASS_DEFAULT),  # not 'or', to allow empty
            'accessModes': params.get('ACCESS_MODES', ['ReadWriteMany']),
            'resources': {
                'requests': {
                    'storage': params.get('DISK_REQUIRED', '1Gi')
                }
            }
        }
    }

    configer.generate(template_persistent_volume_claim, params['GENERATED_MANIFESTS_FOLDER'])
