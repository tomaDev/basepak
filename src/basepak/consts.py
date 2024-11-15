import os

from .classes import ConstMeta

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')) as file:
    package_version = next((line.strip() for line in file if line.strip()), '0.0.0')

##############################################################################################################
# ### Inputs ###
RESOURCE_TEMPLATES = ('manifest', 'pv-nfs', 'pv-efs')  # RESOURCE_TEMPLATES[0] is the default


SHELLS = ('auto', 'bash', 'zsh')  # for completions. SHELLS[0] is the default
##############################################################################################################
# ### Iguazio API ###


class ClusterStatusActionMap(metaclass=ConstMeta):
    """Map of cluster statuses to actions for the cluster"""
    CONTINUE = ('online', 'degraded', 'onlineMaintenance', 'readOnly')
    RETRY = ('unknown', 'upgrading', 'standby', 'maintenance')  # just in case we decide to implement retry


class APIRoutes(metaclass=ConstMeta):
    """Routes for the Iguazio API"""
    BASE = 'http://{}:8001/api'  # noqa: used in string formatting
    SESSIONS = '/sessions'
    FETCH_SESSIONS = '/fetch_sessions'
    CLUSTERS = '/clusters'
    STORAGE_POOLS = '/storage_pools'
    STATISTICS = '/tunnel/{}.storage_device_info.0/statistics'
    EVENTS = '/manual_events'
    APP_SERVICES = '/app_services_manifests'
    EXTERNAL_VERSIONS = '/external_versions'
    APP_CLUSTERS = '/app_clusters'
    CONTAINERS = '/containers'
    TENANTS = '/tenants'
    USERS = '/users'
    APP_TENANTS = '/app_tenants'
    JOBS = '/jobs'


KOMPTON_DEPLOY_PATH_PATTERN = '/home/iguazio/installer/{igz_version}/deploy'
KOMPTON_INVENTORY_HOSTS_PATH_PATTERN = KOMPTON_DEPLOY_PATH_PATTERN + '/inventory/hosts'

##############################################################################################################
# ### Kubernetes ###
KUBE_CONFIG_DEFAULT_LOCATION = os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config'))


class LabelSelectors(metaclass=ConstMeta):
    """Label selectors for Kubernetes resources of Iguazio platform components"""
    MLRUN_DB = 'app.kubernetes.io/name=mlrun,app.kubernetes.io/component=db'
    MLRUN_DEPLOYMENTS = 'app.kubernetes.io/name=mlrun,app.kubernetes.io/component=api'
    PIPELINES_DB = 'app=pipelines,component=mysql-kf'
    PIPELINES_DEPLOYMENTS = 'app=pipelines,component!=mysql-kf,component!=ml-pipeline-ui'
    KEYCLOAK_DB = 'app.kubernetes.io/name=v3io-mysql,app.kubernetes.io/component=keycloak-db'
    KEYCLOAK_STATEFULSETS = 'app.kubernetes.io/name=keycloak'  # noqa: typo
    CONFIGMAPS = 'app.kubernetes.io/managed-by!=Helm,nuclio.io/app!=functionres,!mlrun/class'  # noqa: typo


class FieldSelectors(metaclass=ConstMeta):
    """Field selectors for Kubernetes resources of Iguazio platform components"""
    CONFIGMAPS = 'metadata.name!=kube-root-ca.crt'
    SECRETS = 'type!=kubernetes.io/service-account-token,' \
              'type!=helm.sh/release.v1,' \
              'metadata.name!=dex-web-server-tls,' \
              'metadata.name!=dex-web-server-ca,' \
              'metadata.name!=monitoring-etcd-ssl'


SECRET_NAME_BASE = 'platform-users'
IS_PURGEABLE_KEY = 'purgeable'
DEFAULT_LABELS = {'basepak/version': package_version}
RELOAD_KUBECONFIG_PAYLOAD = {
    "data": {
        "type": "app_cluster_configuration_reload",
        "attributes": {
            "app_tenants_online": False
        }
    }
}
BACKUP_NAMESPACE_DEFAULT = 'iguazio-backup'
PERSISTENT_VOLUME_DEFAULT = 'iguazio-backup'
STORAGE_CLASS_DEFAULT = 'iguazio-backup'
STORAGE_VOLUME_NAME = 'storage'
APP_SERVICE_TYPES_TO_DISABLE = ['jupyter']
APP_SERVICES_TO_RESTART = ['mlrun']

_PART_NAME = r'{range .items[*]}{.metadata.name}'
JSONPATH_READY = _PART_NAME + r'{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}'
JSONPATH_CONDITIONS = _PART_NAME + r'{"\n"}{range .status.conditions[*]}{.type}{"\t"}{.status}{"\n"}{end}{"\n"}{end}'

WAIT_INTERVAL = 30
JOB_TIMEOUT_DEFAULT = '1h'
RETRIES_DEFAULT = 6

NUCLIO_PROJECTS_PREFIX = 'nuclio-projects'
RUN_PHASE = 'run'
##############################################################################################################
# ### OS ###

OS_THRESHOLDS = {
    'CPU_PERCENT': 90,
    'MEMORY_PERCENT': 90,
}


MANOFEST_PATH = '/home/iguazio/igz/platform/manof/manofest.py'

CACHE_FOLDER = '.basepak'
