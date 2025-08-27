from __future__ import annotations

import logging
import subprocess
import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from basepak import k8s_utils
from basepak.time import sleep
from basepak.versioning import Version
from basepak import consts

SUPPORTED_MODES = ['dry-run', 'normal', 'unsafe']

PV_TEMPLATE = """
apiVersion: v1
kind: PersistentVolume
metadata:
  labels:
    created-by: pytest
  name: {name}
spec:
  accessModes:
  - ReadWriteMany
  capacity:
    storage: 50Gi
  local:
    path: /home/iguazio
  mountOptions:
  - bg
  - rsize=1048576
  - wsize=1048576
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - k8s-node2
  persistentVolumeReclaimPolicy: Retain
  storageClassName: basepak-test
  volumeMode: Filesystem
"""

def _get_fresh_resource_name(resource_type: str, label: str = '') -> str:
    resp = subprocess.run(f'kubectl get {resource_type} --output name', shell=True, capture_output=True, check=False)
    prefix = f'basepak-{label}-{resource_type}'
    return next(f'{prefix}-{i}' for i in range(999) if f'{prefix}-{i}' not in resp.stdout.decode())

@contextmanager
def _fresh_pod():
    pod_name = _get_fresh_resource_name('pod')

    while 'AlreadyExists' in subprocess.run(
            f'kubectl run {pod_name} --image=busybox:stable --restart=Never --command -- sleep 60',
            shell=True, check=False, capture_output=True
    ).stderr.decode():
        pod_name = _get_fresh_resource_name('pod')
    try:
        subprocess.check_call(['kubectl', 'wait', '--for=condition=Ready', f'pod/{pod_name}', '--timeout=30s'])
    except subprocess.CalledProcessError:
        subprocess.call(['kubectl', 'delete', 'pod', pod_name])
        raise
    try:
        yield pod_name
    finally:
        subprocess.run(f'kubectl delete pod {pod_name} --ignore-not-found --wait=false', shell=True, check=False)

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_namespace(mode):
    ns = _get_fresh_resource_name('ns', mode)

    # create
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), namespace=ns)
    assert result == ns

    # exists
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), namespace=ns)
    assert result == ns

    # teardown
    assert subprocess.run(f'kubectl delete namespace {ns} --ignore-not-found', shell=True, check=False).returncode == 0

# @pytest.mark.parametrize('empty_type', ['{}', ''])
@pytest.mark.parametrize('empty_type', ['{}', ''])
@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_namespace_with_file_empty(tmp_path, mode, empty_type):
    ns = _get_fresh_resource_name('ns', mode)

    file = tmp_path / f'secrets_{ns}.json'
    file.write_text(empty_type)
    # create
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), file=file)
    assert result == ns

    # exists
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), file=file)
    assert result == ns

    assert subprocess.run(f'kubectl delete namespace {ns} --ignore-not-found', shell=True, check=False).returncode == 0


@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_namespace_with_file_item(tmp_path, mode):
    ns = _get_fresh_resource_name('ns', mode)

    file = tmp_path / f'secrets_{ns}.json'
    file.write_text("""{
            "apiVersion": "v1",
            "data": {
                "val": "x"
            },
            "kind": "Secret",
            "metadata": {
                "name": "test",
                "namespace":""" f'"{ns}",'"""
            },
            "type": "Opaque"
        }"""
    )
    # create
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), file=file)
    assert result == ns

    # exists
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), file=file)
    assert result == ns

    assert subprocess.run(f'kubectl delete namespace {ns} --ignore-not-found', shell=True, check=False).returncode == 0


@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_namespace_with_file_list(tmp_path, mode):
    ns = _get_fresh_resource_name('ns', mode)

    file = tmp_path / f'secrets_{ns}.json'
    file.write_text("""{
    "apiVersion": "v1",
    "items": [
        {
            "apiVersion": "v1",
            "data": {
                "USERNAME": "x"
            },
            "kind": "Secret",
            "metadata": {
                "name": "test",
                "namespace":""" f'"{ns}",'"""
            },
            "type": "Opaque"
        },
        {
            "apiVersion": "v1",
            "data": {
                "USERNAME": "x"
            },
            "kind": "Secret",
            "metadata": {
                "name": "test",
                "namespace": "iguazio-backup",
            },
            "type": "Opaque"
        }
    ],
    "kind": "List",
    "metadata": {
        "resourceVersion": ""}}"""
    )
    # create
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), file=file)
    assert result == ns

    # exists
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), file=file)
    assert result == ns

    assert subprocess.run(f'kubectl delete namespace {ns} --ignore-not-found', shell=True, check=False).returncode == 0

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_pvc(tmp_path, mode):
    spec = {
        'MODE': mode,
        'PERSISTENT_VOLUME_CLAIM_NAME': 'test-pvc',
        'PERSISTENT_VOLUME_CLAIM_DESIRED_STATES': ['Pending'],
        'GENERATED_MANIFESTS_FOLDER': str(tmp_path),
    }

    ns = spec['NAMESPACE'] = _get_fresh_resource_name('ns', mode)

    # create
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # exists
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # teardown
    subprocess.run(f'kubectl delete namespace {ns} --ignore-not-found --wait=false', shell=True, check=False)


@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_pvc_bind(tmp_path, mode):
    pv_name = _get_fresh_resource_name('pv', mode)
    subprocess.run('kubectl create -f -', input=PV_TEMPLATE.format(name=pv_name).encode(), shell=True, check=False)
    spec = {
        'MODE': mode,
        'PERSISTENT_VOLUME_CLAIM_NAME': 'test-pvc',
        'PERSISTENT_VOLUME_CLAIM_DESIRED_STATES': ['Bound'],
        'GENERATED_MANIFESTS_FOLDER': str(tmp_path),
        'STORAGE_CLASS': 'basepak-test',
    }

    ns = spec['NAMESPACE'] = _get_fresh_resource_name('ns', mode)

    # create
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # exists
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # teardown
    subprocess.run(f'kubectl delete namespace {ns} --wait=false --ignore-not-found', shell=True, check=False)
    subprocess.run(f'kubectl delete {pv_name} --wait=false --ignore-not-found', shell=True, check=False)

@pytest.mark.parametrize(
    "mock_partitions,test_path,expected", [
        ([MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/home/user/file.txt', True),
        ([MagicMock(mountpoint='/', fstype='xfs', device='/dev/sdb2')], '/var/log/messages', True),
        ([MagicMock(mountpoint='/mnt/remote_share', fstype='nfs', device='server:/export/path'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/remote_share/data', False),
        ([MagicMock(mountpoint='/mnt/windows_share', fstype='cifs', device='//server/share'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/windows_share/file.docx', False),
        ([MagicMock(mountpoint='/mnt/ssh_access', fstype='fuse.sshfs', device='host:/remote_path'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/ssh_access/data', False),
        ([MagicMock(mountpoint='/mnt/s3', fstype='fuse.s3fs', device='s3.amazonaws.com:/mybucket'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/s3/documents/report.pdf', False),
        ([MagicMock(mountpoint='/mnt/gluster_volume', fstype='glusterfs', device='gluster-node1:/gv0'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/gluster_volume/data', False),
        ([MagicMock(mountpoint='/mnt/cephfs', fstype='ceph', device='mon1,mon2,mon3:/'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/cephfs/data', False),
    ]
)
def test_is_path_local_best_effort(mock_partitions, test_path, expected):
    with patch('psutil.disk_partitions', return_value=mock_partitions):
        assert k8s_utils.is_path_local_best_effort(test_path) is expected

def test_kubectl_upload_file_dry_run(tmp_path):
    with _fresh_pod() as pod:
        tmp_file = tmp_path / 'file.yaml'
        tmp_file.write_text('test content')

        k8s_utils.kubectl_cp(src=tmp_file, dest=f'{pod}:/tmp/file.yaml', mode='dry-run', retries=1)

def test_kubectl_upload_dir_dry_run(tmp_path):
    with _fresh_pod() as pod:
        tmp_dir = tmp_path / 'temporary'
        tmp_dir.mkdir()
        tmp_file = tmp_dir / 'file.yaml'
        tmp_file.write_text('test content')

        k8s_utils.kubectl_cp(src=tmp_dir, dest=f'{pod}:/tmp/dir', mode='dry-run', retries=1)

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_kubectl_upload_download_file(tmp_path, mode):
    with _fresh_pod() as pod:
        tmp_file = tmp_path / 'tmp.yaml'
        tmp_file.write_text('test content')

        remote_path = f'{pod}:/tmp/file --container {pod} '
        # upload
        k8s_utils.kubectl_cp(src=tmp_file, dest=remote_path, mode='unsafe' if mode == 'dry-run' else mode, retries=1)

        tmp_file.unlink()

        # download
        k8s_utils.kubectl_cp(dest=tmp_file, src=remote_path, mode=mode, retries=1)

@pytest.mark.parametrize(
    'test_path,expected', [
        ('/', True),
        ('/users', True),
        ('/users/non-existent-user', False),
        ('/nonexistent-mount', False),
    ])
def test_is_path_local(test_path, expected):
    assert k8s_utils.is_path_local(test_path) is expected

def test_get_kubectl_version():
    assert isinstance(k8s_utils.get_kubectl_version(), Version)

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_kubectl_upload_download_dir(tmp_path, mode):
    with _fresh_pod() as pod:
        remote_path = f'-c{pod} {pod}:/tmp/dir'
        local_dir = tmp_path / 'dir'
        local_dir.mkdir()
        for i in range(100):
            local_dir.joinpath(f'tmp-{i}.yaml').write_text(f'test content {i}\n' + 'abc' * 10_000)

        k8s_utils.kubectl_cp(src=local_dir, dest=remote_path, mode='unsafe' if mode == 'dry-run' else mode, retries=1)

        import shutil
        shutil.rmtree(local_dir)

        k8s_utils.kubectl_cp(dest=local_dir, src=remote_path, mode=mode, retries=1)

def test_kubectl_dump(tmp_path):
    tmp_file = tmp_path / 'tmp.yaml'
    k8s_utils.kubectl_dump('kubectl version --client', tmp_file, mode='unsafe')
    assert tmp_file.exists()
    assert tmp_file.read_text()

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_kubectl_transfer_large_file_between_pods(tmp_path, mode):
    with _fresh_pod() as pod1, _fresh_pod() as pod2:
        tmp_file = tmp_path / 'tmp.yaml'
        tmp_file.write_text('a' * 10_000_000)

        remote_path_pod1 = f'{pod1}:/tmp/file'
        remote_path_pod2 = f'{pod2}:/tmp/file'
        k8s_utils.kubectl_cp(src=tmp_file, dest=remote_path_pod1, mode='unsafe', retries=1)

        k8s_utils.kubectl_cp(src=remote_path_pod1, dest=remote_path_pod2, mode=mode, retries=1)


@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_await_k8s_job_completion_complete_plain(mode):
    name = _get_fresh_resource_name('job')
    if mode != 'dry-run':
        subprocess.run(f'kubectl create job {name} --image=busybox -- /bin/sh -c "sleep 5"', shell=True)
    k8s_utils.await_k8s_job_completion({
        'NAMESPACE': 'default',
        'JOB_NAME': name,
        'MODE': mode,
    })
    if mode != 'dry-run':
        subprocess.run(f'kubectl delete job {name} --wait=false', shell=True, check=False)


def test_await_k8s_job_completion_complete_with_fail():
    name = _get_fresh_resource_name('job')
    subprocess.run(f'kubectl create job {name} --image=busybox -- /bin/sh -c "sleep 10"', shell=True)
    sleep(5)
    subprocess.run(f'kubectl delete pod --selector=job-name={name} --wait=false', shell=True, check=False)
    k8s_utils.await_k8s_job_completion({
        'NAMESPACE': 'default',
        'JOB_NAME': name,
        'MODE': 'unsafe',
    })
    subprocess.run(f'kubectl delete job {name} --wait=false', shell=True, check=False)

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
@pytest.mark.parametrize('refresh_rate', (0,10,100))
def test_prep_binary(mode, tmp_path, refresh_rate):
    name = 'shred'
    spec = {
        'CACHE_FOLDER': tmp_path,
        'NAMESPACE': 'default',
        'JOB_IMAGE': consts.DEFAULT_IMAGE,
        'PATH_ON_IMAGE': f'/bin/{name}',
    }
    k8s_utils.prep_binary(mode=mode, spec=spec, name=name, refresh_rate_default=refresh_rate)

    if mode != 'dry-run':
        assert os.access(f'{tmp_path}/{name}', os.X_OK)
