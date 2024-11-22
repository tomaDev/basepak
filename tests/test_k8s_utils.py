import os
import tempfile
from pathlib import Path

from basepak import k8s_utils
from basepak.versioning import Version


def test_get_kubectl_version():
    out = k8s_utils.get_kubectl_version()
    assert isinstance(out, Version)

def test_kubectl_dump():
    tmp = tempfile.mktemp()
    k8s_utils.kubectl_dump('kubectl version --client', tmp, mode='unsafe')
    assert Path(tmp).exists()
    assert Path(tmp).read_text()
    os.unlink(tmp)

# todo: mock k8s api responses to test the rest of the code
