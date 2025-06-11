from __future__ import annotations

import functools
import json
import logging
import socket
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from functools import partial
from typing import Callable, Dict, List, Optional, Tuple, Union

import requests
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_delay, wait_fixed

from . import (
    consts,
    exceptions,  # IDE mixup
    log,
)
from .credentials import Credentials
from .tasks import Eventer
from .units import Unit

EXCLUDE_CODES = {
    409: 'data container already exists'
}
_possible_propagation_banner = 'This might be due to delay in propagation of policy or permission'
RETRY_CODES = {
    401: _possible_propagation_banner,
    403: _possible_propagation_banner,
    404: _possible_propagation_banner,
    405: _possible_propagation_banner,
    425: 'Too Early. Waiting and retrying...',
    429: 'Too many requests. Waiting and retrying...',
}
DEFAULT_DATA_CONTAINERS = ('users', 'projects', 'bigdata')


def log_after(retry_state: RetryCallState) -> None:
    """Log after each attempt, including details on HTTP responses and errors
    :param retry_state: tenacity retry state object
    """
    exception = retry_state.outcome.exception()
    if isinstance(exception, RetryableHTTPError):
        logger = log.get_logger('plain')
        logger.warning(f'Retry {retry_state.attempt_number}, {retry_state.seconds_since_start=:.2f}s: {exception}')


def log_before(retry_state: RetryCallState) -> None:
    """Log before each attempt
    :param retry_state: tenacity retry state object
    """
    logger = log.get_logger('plain')
    if retry_state.attempt_number == 1:
        logger.warning(f'{retry_state.kwargs["method"].upper()} {retry_state.kwargs["url"]}')

    if json_ := retry_state.kwargs.get('json'):
        log.log_as('json', json_, printer=logger.debug)
    if data := retry_state.kwargs.get('data'):
        log.log_as('json', data, printer=logger.debug)


class RetryableHTTPError(requests.exceptions.HTTPError):
    """Raised when a retryable HTTP error occurs"""
    def __init__(self, response):
        self.response = response
        super().__init__(f'HTTP {response.status_code}: {response.reason}', response=response)

    def __str__(self):
        resp = self.response
        return f'HTTP {resp.status_code}: {resp.reason} - {RETRY_CODES.get(resp.status_code, resp.text)}'


@retry(
    retry=retry_if_exception_type(RetryableHTTPError),
    stop=stop_after_delay(60),
    wait=wait_fixed(5),
    before=log_before,
    after=log_after,
)
def run_request_retry_on_4xx(session: requests.Session, *, url: str, method: str, **kwargs) -> requests.Response:
    """Run a request and retry on 4xx errors
    :param session: requests session
    :param url: URL to request
    :param method: HTTP method
    :param kwargs: additional request arguments
    :return: response object
    """
    response = getattr(session, method)(url, **kwargs)
    if response.status_code in RETRY_CODES:
        raise RetryableHTTPError(response=response)
    logger = log.get_logger('plain')
    if response.status_code in EXCLUDE_CODES:
        logger.warning(f'HTTP {response.status_code}: {EXCLUDE_CODES[response.status_code]}')
        return response
    response.raise_for_status()
    logger.debug(f'HTTP {response.status_code}: {(response.text or response.reason).strip()}')
    return response


@functools.lru_cache
def _container_payload(container_name: str, description: str = '') -> dict:
    return {
        'data': {
            'type': 'container',
            'attributes': {
                'name': container_name,
                'description': description,
            }
        }
    }


def create_data_containers(
        session: requests.Session, base_url: str, tenant: str, logger: logging.Logger, *containers: str
) -> None:
    """Create iguazio data containers for the given tenant
    :param session: requests session
    :param base_url: base URL of the iguazio platform API
    :param tenant: tenant name
    :param logger: logger instance
    :param containers: data container names to create
    :raises RuntimeError: if any of the containers failed to create
    """
    url = base_url + consts.APIRoutes.CONTAINERS
    is_successful = True
    failed_containers = []
    for container in containers:
        try:
            logger.info(f'{tenant=} {container=}')
            run_request_retry_on_4xx(session, url=url, method='post', json=_container_payload(container))
        except requests.exceptions.HTTPError as e:
            logger.error(f'Failed to create {tenant=}, {container=}\n{e.response.text}')
            failed_containers.append(container)
            is_successful = False
    if not is_successful:
        raise RuntimeError(f'Failed to create data containers {" ".join(failed_containers)} for {tenant=}')


def delete_data_containers(session: requests.Session, base_url: str, logger: logging.Logger, tenant: str,
                           *containers: str | int) -> list[dict[str, str | int]]:
    """Delete iguazio data containers for the given tenant
    :param session: requests session
    :param base_url: base URL of the iguazio platform API
    :param logger: logger instance
    :param tenant: tenant name
    :param containers: data container names or ids to delete
    :return: list of deleted containers
    :raises RuntimeError: if any of the containers failed to delete
    """
    url = base_url + consts.APIRoutes.CONTAINERS
    if any(isinstance(container, str) for container in containers):
        names = {x for x in containers if isinstance(x, str)}
        containers_data = run_request(session, url=url, method='get').json()['data']
        ids = {x for x in containers if isinstance(x, int)}
        containers = ids | {x['id'] for x in containers_data if x['attributes']['name'] in names}

    is_successful = True
    failed_containers = []
    deleted_containers = []
    for id_ in containers:
        try:
            logger.info(f'{tenant=} {id_=}')
            resp = run_request_retry_on_4xx(session, url=url + '/' + str(id_), method='delete')
            if resp.status_code == 202:
                deleted_containers.append({
                    'container_id': id_,
                    'job_id': resp.json()['data']['relationships']['jobs']['data'][0]['id']
                })
        except requests.exceptions.HTTPError as e:
            logger.error(f'Failed to create {tenant=}, {id_=}\n{e.response.text}')
            failed_containers.append(id_)
            is_successful = False
    if not is_successful:
        raise RuntimeError(f'Failed to delete containers {" ".join(failed_containers)} for {tenant=}')
    return deleted_containers


class PlatformEvents(Eventer):
    """Events for the Iguazio platform API

    General Event Spec Document - https://iguazio.atlassian.net/wiki/spaces/ARC/pages/1867950/System+Events+-+spec
    """
    def __init__(self, url: str, credentials: Dict[str, str], session: Optional[requests.Session] = None,
                 classification: str = 'ua', component: str = 'Software') -> None:
        super().__init__(url)

        self.hostname = socket.gethostname()
        self.session = session or start_api_session(credentials, url)[0]
        self.credentials = credentials
        self.classification = classification
        self.component = component

        self.attributes_for_failed_events = {
            'severity': 'major',
            'kind': f'{self.component}.Run.Failed',
        }
        self.send_failed = partial(self.send_event, status='failed', **self.attributes_for_failed_events)
        self.send_aborted = partial(self.send_event, status='aborted', **self.attributes_for_failed_events)
        self.send_timeout = partial(self.send_event, status='timeout', **self.attributes_for_failed_events)

    @exceptions.retry_strategy_default
    def send_event(self, task: str, phase: str, status: str, **attributes) -> None:
        """Send event to the platform API

        :param task: name of the task
        :param phase: run, require, setup, execute, validate
        :param status: started, completed, succeeded, failed, aborted, timeout
        :param attributes:
            kind: {COMPONENT}.{TASK}.{PHASE}.{STATUS}  # e.g. ClusterBackup.Migration.Validate.Succeeded
            classification: system, ua
            severity: debug, info, warning, major, critical
        """
        default_msg = f'{task} {phase} {status}'
        default_kind = self.component + '.' + '.'.join(default_msg.split()).title().replace('_', '')  # CamelCase
        attributes.setdefault('severity', 'info')
        attributes.setdefault('visibility', 'internal')
        attributes.setdefault('kind', default_kind)
        attributes.setdefault('source', self.hostname)
        attributes.setdefault('classification', self.classification)
        attributes['description'] = str(attributes.get('description') or default_msg)
        if attributes['kind'] == self.attributes_for_failed_events['kind']:
            attributes['description'] = default_kind + ': ' + attributes['description']
        response = self.session.post(self.url, json=get_payload_body('event', attributes))
        if response.status_code == 401:  # TODO: make more generic. Non-events need reauth too
            self.session, _ = start_api_session(self.credentials, self.url)
            response = self.session.post(self.url, json=get_payload_body('event', attributes))
        response.raise_for_status()

    @staticmethod
    def parametrize(params: Mapping[str, any] | Iterable[str], op: Callable = lambda x: x) -> List[dict]:
        """Convert input according to the Platform API spec for event parameters_text

        :param params: sequence of strings or mapping
        :param op: function to apply to each value in params before converting to json
        :return: list of dicts with 'name' and 'value' keys"""
        def valuate(item):
            post_op = op(item)
            return post_op if isinstance(post_op, str) else json.dumps(post_op, cls=log.DateTimeEncoder)
        if isinstance(params, Mapping):
            return [{'name': k, 'value': valuate(v)} for k, v in params.items() if v is not None]
        return [{'name': item, 'value': valuate(item)} for item in params]


class DummyPlatformEvents(PlatformEvents):
    """Send Iguazio platform events to /dev/null"""
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def send_event(self, task: str, phase: str, status: str, **attributes,):
        pass


def get_payload_body(type_: str, attributes: dict) -> dict:
    """Get the payload body for the given type
    :param type_: the payload type
    :param attributes: payload attributes
    :return: payload body
    """
    return {'data': {'type': type_, 'attributes': attributes}}


@exceptions.retry_strategy_default
def start_api_session(
        creds: Tuple[str, str] | Dict[str, str],
        url: str,
        plane: Optional[str] = 'control',
        retry_on_4xx: bool = False,
) -> (requests.Session, requests.Response):
    """Start HTTP session with the cluster API

    :param creds: Tuple or dict with username and password
    :param url: URL to the system API. Default is the loopback address
    :param plane: 'control' or 'data'
    :param retry_on_4xx: toggle whether to retry on 4xx type errors
    :return: session and response
    """
    logger = log.get_logger()
    session = requests.Session()
    if isinstance(creds, dict):
        creds = tuple((creds.get('USERNAME'), creds.get('PASSWORD')))
    session.auth = creds
    session.timeout = 10
    logger.debug(f'Starting session for {url}\n'
                 f'User: "{session.auth[0]}"')
    session_body = get_payload_body('session', {'plane': plane, 'interface_kind': 'web', 'ttl': 60 * 60 * 24})  # 24h
    runnable = run_request_retry_on_4xx if retry_on_4xx else run_request
    post_response = runnable(session, url=url, method='post', json=session_body)
    return session, post_response


@exceptions.retry_strategy_default
def run_request(session: requests.Session, url: str, method: str = 'get', **kwargs) -> requests.Response:
    """Run a request and return the response
    :param session: requests session
    :param url: URL to request
    :param method: HTTP method
    :param kwargs: additional request arguments
    :return: response object
    """
    logger = log.get_logger()
    logger_plain = log.get_logger('plain')
    try:
        runnable = getattr(session, method.lower())
    except AttributeError:
        logger.error('No such request type: ' + method)
        sys.exit(1)
    logger_plain.debug(f'{method.upper()} {url}')
    if kwargs:
        log.log_as('json', kwargs, printer=logger_plain.debug)
    response = runnable(url, **kwargs)
    response.raise_for_status()
    return response


@exceptions.retry_strategy_default
def get_storage_pools_data(session: requests.session, base_url: str, recalculate: bool = True) -> List[dict]:
    """Get storage pools data from the platform API
    :param session: requests session
    :param base_url: base URL of the iguazio platform API
    :param recalculate: toggle whether to recalculate storage pools stats if it's missing/stale
    :return: storage pools data
    :raises ValueError: if no storage pools data is found
    :raises AssertionError: if 'usable_capacity' key is missing from storage pool attributes
    :raises KeyError: if recalculate=False and 'free_space' key is missing from storage pool attributes
    """
    storage_pools = run_request(session, f'{base_url}{consts.APIRoutes.STORAGE_POOLS}')
    storage_pools.raise_for_status()
    storage_pools_data = storage_pools.json().get('data')
    if not storage_pools_data:
        raise ValueError('No storage pools data found\n' + storage_pools.text)
    attributes = storage_pools_data[0].get('attributes')
    if not attributes:
        return _maybe_recalculate_storage_pools_stats(session, base_url, recalculate, ValueError,
                                                      'No attributes found in storage pools data')
    try:  # 'usable_capacity' is flaky. A single retry should solve it
        Unit(str(attributes['usable_capacity']) + ' B')
    except KeyError as e:
        time.sleep(5)
        raise AssertionError('"usable_capacity" key is missing in storage pool attributes') from e
    try:
        Unit(str(attributes['free_space']) + ' B')
        # The original error msg was:
        # "free_space" key missing from storage pool attributes\n'
        # 'Running on a new or restored system - storage pool stats take a few minutes to populate\n'
        # 'Running on a stable system - something is wrong!'
        # The main issue here is testing, since during testing `bkp backup run-backup` runs on a brand-new system right
        # after installation.
        # The fix idea is to trigger recalculation ONCE, and sleep 30s to allow completion.
        # However, triggering recalculation only once is incompatible with our retry strategy for the other errors.
        # So instead of extracting it as a special case, we trigger recalculation every time until it works / errors out
    except KeyError:
        return _maybe_recalculate_storage_pools_stats(session, base_url, recalculate, KeyError,
                                                      '"free_space" key missing from storage pool attributes\n')
    log.log_as('json', storage_pools.json(), printer=log.get_logger('plain').debug)
    return storage_pools_data


def _maybe_recalculate_storage_pools_stats(session, base_url, recalculate, msg_type, msg_text):
    if not recalculate:
        raise msg_type(msg_text)
    if not session or session.auth[0] != Credentials.get('IGUAZIO_ADMINISTRATOR', {}).get('USERNAME'):
        creds = Credentials.set()
        session, _ = start_api_session(creds.get('IGUAZIO_ADMINISTRATOR'), base_url + consts.APIRoutes.SESSIONS)
    for node in (node['name'] for node in get_sysconfig(base_url, session)['data_cluster']['nodes']):
        run_request(session, base_url + consts.APIRoutes.STATISTICS.format(node), method='post')
    time.sleep(30)  # no way to await completion due to IG-17830. Sleeping as a workaround
    return get_storage_pools_data(session, base_url, recalculate=False)


@exceptions.retry_strategy_default
@functools.lru_cache
def get_app_services(api_base_url: str, session: requests.sessions.Session) -> list:
    """Get app services from the platform API
    :param api_base_url: base URL of the iguazio platform API
    :param session: requests session
    :return: app services
    """
    app_services_response = run_request(session, api_base_url + consts.APIRoutes.APP_SERVICES).json()
    return app_services_response['data'][0]['attributes']['app_services']


def get_app_service_status(app_services: Sequence, app_service_name: str) -> dict:
    """Get the status of an app service
    :param app_services: app services data
    :param app_service_name: app service name
    :return: app service status
    """
    return next((service['status'] for service in app_services if service['spec']['name'] == app_service_name), {})


@exceptions.retry_strategy_default
@functools.lru_cache
def get_sysconfig(base_url: str, session: Optional[requests.sessions.Session] = None) -> dict:
    """Get initial system configuration from the platform API
    :param base_url: base URL of the iguazio platform API
    :param session: requests session. If session not provided, will create a new one from the global credentials
    :return: system configuration
    """
    if not session or session.auth[0] != Credentials.get('IGUAZIO_ADMINISTRATOR', {}).get('USERNAME'):
        creds = Credentials.set()
        session, _ = start_api_session(creds.get('IGUAZIO_ADMINISTRATOR'), base_url + consts.APIRoutes.SESSIONS)
    resp = run_request(session, base_url + consts.APIRoutes.APP_CLUSTERS).json()
    for key_ in ('data', 0, 'attributes', 'system_configuration'):
        resp = resp[key_]  # the iteration is for debugging, to know where KeyError occurred
    return json.loads(resp)['spec']


def get_app_name_prefix(base_url: str, session: Optional[requests.sessions.Session] = None) -> str:
    """Get the app cluster subdomain prefix from the platform API
    :param base_url: base URL of the iguazio platform API
    :param session: requests session
    :return: app cluster subdomain prefix
    """
    sysconfig = get_sysconfig(base_url, session)
    return sysconfig['data_cluster']['subdomain'].split('.', maxsplit=1)[1] + '-'


@exceptions.retry_strategy_default
def validate_cluster_status(session: requests.Session, spec: Mapping) -> None:
    """Validate iguazio cluster is ready for operations
    :param session: requests session
    :param spec: platform spec
    :raises ClusterNotReadyError: if the cluster is not ready for operations
    :raises requests.HTTPError: if the API request fails
    """
    response = session.get(spec['API_BASE_URL'] + consts.APIRoutes.CLUSTERS)
    response.raise_for_status()
    try:
        attributes = json.loads(response.text)['data'][0]['attributes']
    except KeyError as e:
        raise KeyError(f'Failed to parse response from {response.url}\n{response.text}') from e
    if attributes.get('operational_status_change_in_progress'):
        raise exceptions.ClusterNotReadyError('Operational status change in progress')
    if attributes['operational_status'] not in consts.ClusterStatusActionMap.CONTINUE:
        raise exceptions.ClusterNotReadyError(f'Cluster operational status - {attributes["operational_status"]}\n'
                                              f'Expected one of {consts.ClusterStatusActionMap.CONTINUE}')


def api_request(
        data_node_ip: str,
        endpoint: str,
        request_type,
        filter_: Union[List[str], str],
        data: Optional[str] = '',
        json_loads: Optional[str] = '',
        auth: Optional[str] = None,
) -> dict:
    """Make a request to the platform API
    :param auth: user:pass for basic auth
    :param data_node_ip: data node IP address
    :param endpoint: API endpoint
    :param request_type: HTTP method
    :param filter_: filter the response
    :param data: request data
    :param json_loads: keys to load as json
    :return: response
    """
    if not endpoint.startswith('/'):
        endpoint = '/' + endpoint
    base_url = consts.APIRoutes.BASE.format(data_node_ip or '127.0.0.1')
    creds = Credentials.set(auths={'IGUAZIO_ADMINISTRATOR': auth} if auth else None)
    session, _ = start_api_session(creds.get('IGUAZIO_ADMINISTRATOR'), base_url + consts.APIRoutes.SESSIONS)
    response = run_request(session, base_url + endpoint, request_type, data=data).json()
    if filter_:
        if isinstance(filter_, str):
            filter_ = filter_.split(',')
        for key_ in filter_:
            response = response[int(key_) if (isinstance(key_, int) or key_.isnumeric()) else key_]
    if json_loads:
        response = json.loads(response)
        for key_ in json_loads.split(','):
            response = response[int(key_) if key_.isnumeric() else key_]
    return response


def get_storage_stats(session: requests.Session, base_url: str, units: Optional[str] = 'auto') -> dict[str, str]:
    """Get storage statistics from the platform API
    :param session: requests session
    :param base_url: api base URL
    :param units: units to convert to
    :return: storage statistics
    :raises ValueError: if the storage pools data is missing
    """
    pools_data = get_storage_pools_data(session, base_url)
    usable_capacity = Unit.reduce(x['attributes']['usable_capacity'] for x in pools_data)
    free_space = Unit.reduce(x['attributes']['free_space'] for x in pools_data)
    result = {'usable-capacity': str(usable_capacity.as_unit(units)).strip()}
    try:
        used_capacity = usable_capacity - free_space  # If platform flaky, api call may return with missing fields
        result.update({
            'used-capacity': str(used_capacity.as_unit(units)).strip(),
            'usage-percentage': f'{used_capacity.convert_to(usable_capacity.unit) / usable_capacity.value * 100:.2f}',
        })
    except ValueError as e:
        log.get_logger(name='plain').warning(f'Failed to calculate used capacity\n{e}')
    return result
