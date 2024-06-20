import os
from contextlib import contextmanager
from typing import Optional, Iterable, Mapping

import igz_mgmt
from igz_mgmt import exceptions as igz_mgmt_exceptions
from tenacity import retry, wait_exponential, stop_after_attempt

from . import consts, log, time, platform_api
from .credentials import Credentials


@contextmanager
def client_context(
        user_from_credential_store: Optional[str] = 'USER',
        host_ip: Optional[str] = None,
) -> igz_mgmt.Client:
    if not host_ip:
        host_ip = os.environ.get('IGZ_NODE_MANAGEMENT_IP') or '127.0.0.1'
    creds = Credentials.get(user_from_credential_store)
    if not creds:
        raise ValueError('No user credentials found')
    logger = log.get_logger()
    endpoint = f'http://{host_ip}:8001' # noqa
    logger.info(f'User: {creds["USERNAME"]}')
    logger.info(endpoint)
    client = igz_mgmt.Client(endpoint=endpoint, username=creds['USERNAME'], password=creds['PASSWORD'], logger=logger)
    client.login()
    try:
        yield client
    finally:
        client.close()


@contextmanager
@retry(reraise=True, wait=wait_exponential(multiplier=2), stop=stop_after_attempt(10))
def client_context_with_asm(
        force_apply_all: igz_mgmt.constants.ForceApplyAllMode = igz_mgmt.constants.ForceApplyAllMode.disabled
):
    with client_context() as client:
        with igz_mgmt.AppServicesManifest.apply_services(client, force_apply_all) as asm:
            yield client, asm


# todo: document difference between K8sConfig.list and AppServicesManifest.get
# todo: consider creating a separate context manager for asm derived from K8sConfig.list
@retry(reraise=True, wait=wait_exponential(multiplier=2), stop=stop_after_attempt(10))
def bulk_update_app_services(
        desired_states_map: Optional[Mapping[str, igz_mgmt.constants.AppServiceDesiredStates]] = None,
        services_to_restart: Iterable[str] = (),
        force_apply_all: igz_mgmt.constants.ForceApplyAllMode = igz_mgmt.constants.ForceApplyAllMode.disabled
):
    with client_context_with_asm(force_apply_all) as (client, asm):
        logger = log.get_logger()
        for service_name in services_to_restart:
            logger.info(f'Restarting {service_name}')
            asm.restart(client, service_name)
        if not desired_states_map:
            return
        k8s_config = igz_mgmt.K8sConfig.list(client)
        for service_name, desired_state in desired_states_map.items():
            logger.info(f'Setting {service_name}: {desired_state}')
            svc = asm.resolve_service(service_name)
            if not svc:
                svc = next((x for x in k8s_config[0].app_services if x.spec.name == service_name), None)
            if not svc:
                logger.error(f'Service {service_name} not found in either CRD or DB. Skipping...')
                continue
            svc.spec.desired_state = desired_state
            asm.create_or_update(client, svc)


def get_desired_states_stash(created: str, service_types: Iterable[str]) -> dict:
    with client_context() as client:
        k8s_confing = igz_mgmt.K8sConfig.list(client)  # IG-22833 oy vey
        app_services = k8s_confing[0].app_services
    return {
        'created': created,
        'modified': time.create_timestamp(),
        'fulfilled': False,
        'services': {x.spec.name: x.spec.desired_state for x in app_services if x.spec.kind in service_types},
    }


def ensure_user(api_base_url: str, username: str, password: str, tenant: str):
    logger = log.get_logger(name='short')
    security_admin_creds = Credentials.get('SECURITY_ADMIN')
    logger.warning(f'Ensuring user "{username}" in {tenant=}')
    if not security_admin_creds:
        configs = platform_api.get_sysconfig(api_base_url)
        creators = {tenant['meta']['id']: tenant['spec']['resources'][0]['creator'] for tenant in configs['tenants']}
        logger.debug(f'Default Creators: {creators}')
        security_admin_creds = creators.get(tenant)
        if security_admin_creds:
            security_admin_creds['USERNAME'] = security_admin_creds.pop('username')
            security_admin_creds['PASSWORD'] = security_admin_creds.pop('password')
    if not security_admin_creds:
        logger.error(f'Tenant {tenant} not found in system configuration')
        raise ValueError(f'Tenant {tenant} not found in system configuration')
    security_admin_creds['USERNAME'] += f'@{tenant}'
    Credentials.set({f'SECURITY_ADMIN@{tenant}': security_admin_creds})
    with client_context(f'SECURITY_ADMIN@{tenant}') as client:
        logger.debug(f'Ensuring user {username} in tenant {tenant}')
        try:
            user = igz_mgmt.User.get_by_username(client, username)
        except igz_mgmt_exceptions.ResourceNotFoundException:
            policies = [
                igz_mgmt.constants.TenantManagementRoles.application_admin,
                igz_mgmt.constants.TenantManagementRoles.data,
                igz_mgmt.constants.TenantManagementRoles.developer,
                igz_mgmt.constants.TenantManagementRoles.security_admin,
                igz_mgmt.constants.TenantManagementRoles.service_admin,
            ]
            logger.debug(f'User {username} not found. Creating...')
            email_user = username if tenant in ('default-tenant', '', None) else f'{username}-{tenant}'
            igz_mgmt.User.create(
                client,
                first_name='John',
                last_name='Doe',
                email=f'{email_user}@iguazio.com',
                username=username,
                password=password,
                assigned_policies=policies,
            )
            logger.debug(f'User "{username}" created in tenant "{tenant}"')
            user = igz_mgmt.User.get_by_username(client, username)
        retries = 2 * consts.RETRIES_DEFAULT
        try:
            while not user.is_operational(client) and retries > 0:
                logger.warning(f'User {username} is not operational. Waiting...')
                retries -= 1
                time.sleep(5)
        except igz_mgmt_exceptions.ResourceNotFoundException as e:
            logger.error(f'User "{username}" may have gotten deleted during the wait. '
                         f'Please inquire who deleted the user and why, before retrying')
            raise e
