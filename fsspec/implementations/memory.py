from __future__ import annotations
import logging
from datetime import datetime, timezone
from errno import ENOTEMPTY
from io import BytesIO
from pathlib import PurePath, PureWindowsPath
from typing import Any, ClassVar
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import stringify_path
logger = logging.getLogger('fsspec.memoryfs')

class MemoryFileSystem(AbstractFileSystem):
    """A filesystem based on a dict of BytesIO objects

    This is a global filesystem so instances of this class all point to the same
    in memory filesystem.
    """
    store: ClassVar[dict[str, Any]] = {}
    pseudo_dirs = ['']
    protocol = 'memory'
    root_marker = '/'

    def pipe_file(self, path, value, **kwargs):
        """Set the bytes of given file

        Avoids copies of the data if possible
        """
        path = self._strip_protocol(stringify_path(path))
        if isinstance(value, bytes):
            data = value
        else:
            data = value.read()
        self.store[path] = MemoryFile(self, path, data)

class MemoryFile(BytesIO):
    """A BytesIO which can't close and works as a context manager

    Can initialise with data. Each path should only be active once at any moment.

    No need to provide fs, path if auto-committing (default)
    """

    def __init__(self, fs=None, path=None, data=None):
        logger.debug('open file %s', path)
        self.fs = fs
        self.path = path
        self.created = datetime.now(tz=timezone.utc)
        self.modified = datetime.now(tz=timezone.utc)
        if data:
            super().__init__(data)
            self.seek(0)

    def __enter__(self):
        return self