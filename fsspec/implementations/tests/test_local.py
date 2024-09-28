import bz2
import gzip
import os
import os.path
import pickle
import posixpath
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
import pytest
import fsspec
from fsspec import compression
from fsspec.core import OpenFile, get_fs_token_paths, open_files
from fsspec.implementations.local import LocalFileSystem, make_path_posix
from fsspec.tests.test_utils import WIN
files = {'.test.accounts.1.json': b'{"amount": 100, "name": "Alice"}\n{"amount": 200, "name": "Bob"}\n{"amount": 300, "name": "Charlie"}\n{"amount": 400, "name": "Dennis"}\n', '.test.accounts.2.json': b'{"amount": 500, "name": "Alice"}\n{"amount": 600, "name": "Bob"}\n{"amount": 700, "name": "Charlie"}\n{"amount": 800, "name": "Dennis"}\n'}
csv_files = {'.test.fakedata.1.csv': b'a,b\n1,2\n', '.test.fakedata.2.csv': b'a,b\n3,4\n'}
odir = os.getcwd()

@contextmanager
def filetexts(d, open=open, mode='t'):
    """Dumps a number of textfiles to disk

    d - dict
        a mapping from filename to text like {'a.csv': '1,1
2,2'}

    Since this is meant for use in tests, this context manager will
    automatically switch to a temporary current directory, to avoid
    race conditions when running tests in parallel.
    """
    pass

def test_urlpath_expand_read():
    """Make sure * is expanded in file paths when reading."""
    pass

def test_urlpath_expand_write():
    """Make sure * is expanded in file paths when writing."""
    pass