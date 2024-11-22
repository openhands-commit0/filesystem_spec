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

    @classmethod
    def _strip_protocol(cls, path):
        """Remove protocol from path"""
        path = stringify_path(path)
        if path.startswith('memory://'):
            path = path[9:]
        return path.lstrip('/')

    @classmethod
    def current(cls):
        """Return the most recently instantiated instance"""
        if not cls._cache:
            return cls()
        return list(cls._cache.values())[-1]

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

    def cat(self, path):
        """Return contents of file as bytes"""
        path = self._strip_protocol(stringify_path(path))
        if path not in self.store:
            return None
        return self.store[path].getvalue()

    def du(self, path, total=True, maxdepth=None, **kwargs):
        """Space used by files within a path"""
        path = self._strip_protocol(stringify_path(path))
        sizes = {}
        for p, f in self.store.items():
            if p.startswith(path):
                sizes[p] = len(f.getvalue())
        if total:
            return sum(sizes.values())
        return sizes

    def open(self, path, mode='rb', **kwargs):
        """Open a file"""
        path = self._strip_protocol(stringify_path(path))
        if mode == 'rb':
            if path not in self.store:
                raise FileNotFoundError(path)
            return MemoryFile(self, path, self.store[path].getvalue())
        elif mode == 'wb':
            f = MemoryFile(self, path)
            self.store[path] = f
            return f
        else:
            raise NotImplementedError("Mode %s not supported" % mode)

    def put(self, lpath, rpath, recursive=False, **kwargs):
        """Copy file(s) from local"""
        if recursive:
            for f in LocalFileSystem().find(lpath):
                data = open(f, 'rb').read()
                rp = rpath + '/' + os.path.relpath(f, lpath)
                self.pipe_file(rp, data)
        else:
            data = open(lpath, 'rb').read()
            self.pipe_file(rpath, data)

    def get(self, rpath, lpath, recursive=False, **kwargs):
        """Copy file(s) to local"""
        if recursive:
            paths = self.find(rpath)
            for path in paths:
                data = self.cat(path)
                lp = os.path.join(lpath, os.path.relpath(path, rpath))
                os.makedirs(os.path.dirname(lp), exist_ok=True)
                with open(lp, 'wb') as f:
                    f.write(data)
        else:
            data = self.cat(rpath)
            with open(lpath, 'wb') as f:
                f.write(data)

    def ls(self, path, detail=True, **kwargs):
        """List objects at path"""
        path = self._strip_protocol(stringify_path(path))
        paths = []
        for p in self.store:
            if p.startswith(path):
                paths.append(p)
        if detail:
            return [self.info(p) for p in paths]
        return paths

    def info(self, path, **kwargs):
        """Get info about file"""
        path = self._strip_protocol(stringify_path(path))
        if path not in self.store:
            raise FileNotFoundError(path)
        f = self.store[path]
        return {
            'name': path,
            'size': len(f.getvalue()),
            'type': 'file',
            'created': f.created,
            'modified': f.modified
        }

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
        else:
            super().__init__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.modified = datetime.now(tz=timezone.utc)
        return None

    def close(self):
        pass  # BytesIO can't be closed in memory

    def discard(self):
        pass  # BytesIO can't be discarded in memory