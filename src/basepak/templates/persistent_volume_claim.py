from typing import Mapping


def generate_template(params: Mapping) -> str:
    from .. import consts, configer
    user_labels = params.get('METADATA', {}).get('labels', {}) | params.get('metadata', {}).get('labels', {})
    user_labels.setdefault(consts.IS_PURGEABLE_KEY, 'false')
    template_persistent_volume_claim = {
        'apiVersion': 'v1',
        'kind': 'PersistentVolumeClaim',
        'metadata': {
            'name': params['PERSISTENT_VOLUME_CLAIM_NAME'],
            'namespace': params['NAMESPACE'],
            'labels': consts.DEFAULT_LABELS | user_labels,
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
    return template_persistent_volume_claim['metadata']['name']
