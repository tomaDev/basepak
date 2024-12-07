import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from basepak import k8s_utils
from basepak.versioning import Version
import pytest

def test_get_kubectl_version():
    out = k8s_utils.get_kubectl_version()
    assert isinstance(out, Version)

def test_kubectl_dump():
    tmp = tempfile.mktemp()
    k8s_utils.kubectl_dump('kubectl version --client', tmp, mode='unsafe')
    assert Path(tmp).exists()
    assert Path(tmp).read_text()
    os.unlink(tmp)

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
    "test_path,expected", [
        ('/', True),
        ('/users', True),
        ('/users/non-existent-user', False),
        ('/nonexistent-mount', False),
        ]
)
def test_is_path_local(test_path, expected):
    assert k8s_utils.is_path_local(test_path) is expected

# TODO: mock k8s api responses to test the rest of the code
