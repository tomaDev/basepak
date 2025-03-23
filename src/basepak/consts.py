import os

from basepak import __version__ as package_version

class ConstMeta(type):
    """Metaclass for creating immutable classes

    Inheriting from this class will prevent setting and deleting attributes. This differs from the frozen class in
    dataclasses in that it ensures immutability of the class itself, and not just the instances of the class.

    Usage example:
        class IgzVersionCutoffs(metaclass=ConstMeta):
            ROCKYLINUX8_SUPPORT = '3.6.0'
            ASM_MERGE = '3.5.5'
    """
    def __setattr__(cls, key, value):
        if key in cls.__dict__:
            raise AttributeError(f"Class {cls.__name__} immutable! Cannot modify constant attribute '{key}'")
        super().__setattr__(key, value)

    def __delattr__(cls, key):
        if key in cls.__dict__:
            raise AttributeError(f"Class {cls.__name__} immutable! Cannot delete constant attribute '{key}'")
        super().__delattr__(cls, key)

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

MANOFEST_PATH = '/home/iguazio/igz/platform/manof/manofest.py'
##############################################################################################################
# ### Kubernetes ###
KUBE_CONFIG_DEFAULT_LOCATION = os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config'))


class LabelSelectors(metaclass=ConstMeta):
    """Label selectors for Kubernetes resources of Iguazio platform components"""
    MLRUN_DB = 'app.kubernetes.io/name=mlrun,app.kubernetes.io/component=db'
    MLRUN_DEPLOYMENTS = 'app.kubernetes.io/name=mlrun,app.kubernetes.io/component!=db,app.kubernetes.io/component!=ui'
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


SECRET_NAME_BASE = 'platform-users'  # nosec B105: hardcoded secret name
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

_PART_NAME = r'{range .items[*]}{.metadata.name}'
JSONPATH_READY = _PART_NAME + r'{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}'
JSONPATH_CONDITIONS = _PART_NAME + r'{"\n"}{range .status.conditions[*]}{.type}{"\t"}{.status}{"\n"}{end}{"\n"}{end}'

WAIT_INTERVAL = 30
JOB_TIMEOUT_DEFAULT = '1h'
RETRIES_DEFAULT = 6
