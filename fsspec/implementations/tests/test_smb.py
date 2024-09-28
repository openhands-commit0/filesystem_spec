"""
Test SMBFileSystem class using a docker container
"""
import logging
import os
import shlex
import subprocess
import time
import pytest
import fsspec
pytest.importorskip('smbprotocol')
if os.environ.get('WSL_INTEROP'):
    port_test = [9999]
else:
    default_port = 445
    port_test = [None, default_port, 9999]