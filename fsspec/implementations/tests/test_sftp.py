import os
import shlex
import subprocess
import time
from tarfile import TarFile
import pytest
import fsspec
pytest.importorskip('paramiko')

def make_tarfile(files_to_pack, tmp_path):
    """Create a tarfile with some files."""
    pass