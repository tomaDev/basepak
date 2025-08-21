from collections.abc import Mapping


def generate_template(params: Mapping) -> tuple[str, str]:
    """Generate a k8s PersistentVolumeClaim template
    :param params: PersistentVolumeClaim parameters
    :return: PersistentVolumeClaim name, path to template
    """
    from .. import configer, consts
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
            'storageClassName': params.get('STORAGE_CLASS', ''),
            'accessModes': params.get('ACCESS_MODES', ['ReadWriteMany']),
            'resources': {
                'requests': {
                    'storage': params.get('DISK_REQUIRED', '1Gi')
                }
            }
        }
    }

    path_to_template = configer.generate(template_persistent_volume_claim, params['GENERATED_MANIFESTS_FOLDER'])
    return template_persistent_volume_claim['metadata']['name'], path_to_template
