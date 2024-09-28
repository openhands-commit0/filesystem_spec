import errno
import io
import os
import secrets
import shutil
from contextlib import suppress
from functools import cached_property, wraps
from urllib.parse import parse_qs
from fsspec.spec import AbstractFileSystem
from fsspec.utils import get_package_version_without_import, infer_storage_options, mirror_from, tokenize
PYARROW_VERSION = None

class ArrowFSWrapper(AbstractFileSystem):
    """FSSpec-compatible wrapper of pyarrow.fs.FileSystem.

    Parameters
    ----------
    fs : pyarrow.fs.FileSystem

    """
    root_marker = '/'

    def __init__(self, fs, **kwargs):
        global PYARROW_VERSION
        PYARROW_VERSION = get_package_version_without_import('pyarrow')
        self.fs = fs
        super().__init__(**kwargs)

@mirror_from('stream', ['read', 'seek', 'tell', 'write', 'readable', 'writable', 'close', 'size', 'seekable'])
class ArrowFile(io.IOBase):

    def __init__(self, fs, stream, path, mode, block_size=None, **kwargs):
        self.path = path
        self.mode = mode
        self.fs = fs
        self.stream = stream
        self.blocksize = self.block_size = block_size
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.close()

class HadoopFileSystem(ArrowFSWrapper):
    """A wrapper on top of the pyarrow.fs.HadoopFileSystem
    to connect it's interface with fsspec"""
    protocol = 'hdfs'

    def __init__(self, host='default', port=0, user=None, kerb_ticket=None, replication=3, extra_conf=None, **kwargs):
        """

        Parameters
        ----------
        host: str
            Hostname, IP or "default" to try to read from Hadoop config
        port: int
            Port to connect on, or default from Hadoop config if 0
        user: str or None
            If given, connect as this username
        kerb_ticket: str or None
            If given, use this ticket for authentication
        replication: int
            set replication factor of file for write operations. default value is 3.
        extra_conf: None or dict
            Passed on to HadoopFileSystem
        """
        from pyarrow.fs import HadoopFileSystem
        fs = HadoopFileSystem(host=host, port=port, user=user, kerb_ticket=kerb_ticket, replication=replication, extra_conf=extra_conf)
        super().__init__(fs=fs, **kwargs)