from __future__ import annotations

import subprocess
from unittest.mock import patch, MagicMock
import logging

import pytest # type: ignore

from basepak import k8s_utils
from basepak.versioning import Version

SUPPORTED_MODES = ['dry-run', 'normal', 'unsafe']

pv_template = """
apiVersion: v1
kind: PersistentVolume
metadata:
  labels:
    created-by: pytest
  name: basepak-test
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

@pytest.mark.parametrize(
    "mock_partitions,test_path,expected,raises_error", [
        ([MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/home/user/file.txt', True, None),
        ([MagicMock(mountpoint='/', fstype='xfs', device='/dev/sdb2')], '/var/log/messages', True, None),
        ([MagicMock(mountpoint='/mnt/remote_share', fstype='nfs', device='server:/export/path'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/remote_share/data', False, None),
        ([MagicMock(mountpoint='/mnt/windows_share', fstype='cifs', device='//server/share'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/windows_share/file.docx', False, None),
        ([MagicMock(mountpoint='/mnt/ssh_access', fstype='fuse.sshfs', device='host:/remote_path'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/ssh_access/data', False, None),
        ([MagicMock(mountpoint='/mnt/s3', fstype='fuse.s3fs', device='s3.amazonaws.com:/mybucket'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/s3/documents/report.pdf', False, None),
        ([MagicMock(mountpoint='/mnt/gluster_volume', fstype='glusterfs', device='gluster-node1:/gv0'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/gluster_volume/data', False, None),
        ([MagicMock(mountpoint='/mnt/cephfs', fstype='ceph', device='mon1,mon2,mon3:/'),
          MagicMock(mountpoint='/', fstype='ext4', device='/dev/sda1')], '/mnt/cephfs/data', False, None),
    ]
)
def test_is_path_local_best_effort(mock_partitions, test_path, expected, raises_error):
    with patch('psutil.disk_partitions', return_value=mock_partitions):
        if raises_error:
            with pytest.raises(raises_error):
                k8s_utils.is_path_local_best_effort(test_path)
        else:
            assert k8s_utils.is_path_local_best_effort(test_path) is expected

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
    out = k8s_utils.get_kubectl_version()
    assert isinstance(out, Version)

def test_kubectl_dump(tmp_path):
    tmp_file = tmp_path / 'tmp.yaml'
    k8s_utils.kubectl_dump('kubectl version --client', tmp_file, mode='unsafe')
    assert tmp_file.exists()
    assert tmp_file.read_text()

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_namespace(mode):
    ns = _get_fresh_namespace()

    # create
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), namespace=ns)
    assert result == ns

    # exists
    result = k8s_utils.ensure_namespace(mode, logging.getLogger(), namespace=ns)
    assert result == ns

    # teardown
    assert subprocess.run(f'kubectl delete namespace {ns} --ignore-not-found', shell=True).returncode == 0


@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_pvc(tmp_path, mode):
    spec = {
        'MODE': mode,
        'PERSISTENT_VOLUME_CLAIM_NAME': 'test-pvc',
        'PERSISTENT_VOLUME_CLAIM_DESIRED_STATES': ['pending'],
        'GENERATED_MANIFESTS_FOLDER': str(tmp_path),
    }

    ns = spec['NAMESPACE'] = _get_fresh_namespace()

    # create
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # exists
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # teardown
    subprocess.run(f'kubectl delete namespace {ns} --ignore-not-found --wait=false', shell=True)


@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_ensure_pvc_bind(tmp_path, mode):
    assert subprocess.run('kubectl create -f -', input=pv_template.encode(), shell=True).returncode == 0
    spec = {
        'MODE': mode,
        'PERSISTENT_VOLUME_CLAIM_NAME': 'test-pvc',
        'PERSISTENT_VOLUME_CLAIM_DESIRED_STATES': ['bound'],
        'GENERATED_MANIFESTS_FOLDER': str(tmp_path),
        'STORAGE_CLASS': 'basepak-test',
    }

    ns = spec['NAMESPACE'] = _get_fresh_namespace()

    # create
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # exists
    k8s_utils.ensure_pvc(spec, logging.getLogger())

    # teardown
    subprocess.run(f'kubectl delete namespace {ns} --wait=false --ignore-not-found', shell=True)
    subprocess.run('kubectl delete -f -', input=pv_template.encode(), shell=True)


def _get_fresh_namespace():
    resp = subprocess.run('kubectl get namespace', shell=True, capture_output=True)
    ns = 'basepak-test'
    return next(f'{ns}-{i}' for i in range(100) if f'{ns}-{i}' not in resp.stdout.decode())


def _get_fresh_pod_name():
    resp = subprocess.run('kubectl get pod --output name', shell=True, capture_output=True)
    prefix = 'basepak-pod'
    return next(f'{prefix}-{i}' for i in range(999) if f'{prefix}-{i}' not in resp.stdout.decode())

from contextlib import contextmanager

@contextmanager
def fresh_pod(pod_name):
    subprocess.check_call(['kubectl', 'run', pod_name, '--image=busybox:stable', '--restart=Never', '--command', '--',
                           'sleep', 'infinity',])
    try:
        subprocess.check_call(['kubectl', 'wait', '--for=condition=Ready', f'pod/{pod_name}', '--timeout=60s'])
    except subprocess.CalledProcessError:
        subprocess.call(['kubectl', 'delete', 'pod', pod_name])
        raise
    try:
        yield pod_name
    finally:
        subprocess.run(f'kubectl delete pod {pod_name} --ignore-not-found --wait=false', shell=True)

def test_kubectl_upload_file_dry_run(tmp_path):
    with fresh_pod(_get_fresh_pod_name()) as pod:
        tmp_file = tmp_path / 'file.yaml'
        tmp_file.write_text('test content')

        k8s_utils.kubectl_cp(src=tmp_file, dest=f'{pod}:/tmp/file.yaml', mode='dry-run', retries=1)

def test_kubectl_upload_dir_dry_run(tmp_path):
    with fresh_pod(_get_fresh_pod_name()) as pod:
        tmp_dir = tmp_path / 'temporary'
        tmp_dir.mkdir()
        tmp_file = tmp_dir / 'file.yaml'
        tmp_file.write_text('test content')

        k8s_utils.kubectl_cp(src=tmp_dir, dest=f'{pod}:/tmp/dir', mode='dry-run', retries=1)

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_kubectl_upload_download_file(tmp_path, mode):
    with fresh_pod(_get_fresh_pod_name()) as pod:
        tmp_file = tmp_path / 'tmp.yaml'
        tmp_file.write_text('test content')

        remote_path = f'{pod}:/tmp/file'
        k8s_utils.kubectl_cp(src=tmp_file, dest=remote_path, mode='unsafe' if mode == 'dry-run' else mode, retries=1)

        tmp_file.unlink()

        k8s_utils.kubectl_cp(dest=tmp_file, src=remote_path, mode=mode, retries=1)

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_kubectl_upload_download_dir(tmp_path, mode):
    with fresh_pod(_get_fresh_pod_name()) as pod:
        remote_path = f'{pod}:/tmp/dir'
        local_dir = tmp_path / 'dir'
        local_dir.mkdir()
        for i in range(3):
            local_dir.joinpath(f'tmp-{i}.yaml').write_text(f'test content {i}')

        k8s_utils.kubectl_cp(src=local_dir, dest=remote_path, mode='unsafe' if mode == 'dry-run' else mode, retries=1)

        import shutil
        shutil.rmtree(local_dir)

        k8s_utils.kubectl_cp(dest=local_dir, src=remote_path, mode=mode, retries=1)

@pytest.mark.parametrize('mode', SUPPORTED_MODES)
def test_kubectl_transfer_large_file_between_pods(tmp_path, mode):
    with fresh_pod(_get_fresh_pod_name()) as pod1, fresh_pod(_get_fresh_pod_name()) as pod2:
        tmp_file = tmp_path / 'tmp.yaml'
        tmp_file.write_text('a' * 100_000_000)

        remote_path_pod1 = f'{pod1}:/tmp/file'
        remote_path_pod2 = f'{pod2}:/tmp/file'
        k8s_utils.kubectl_cp(src=tmp_file, dest=remote_path_pod1, mode='unsafe', retries=1)

        k8s_utils.kubectl_cp(src=remote_path_pod1, dest=remote_path_pod2, mode=mode, retries=1)
