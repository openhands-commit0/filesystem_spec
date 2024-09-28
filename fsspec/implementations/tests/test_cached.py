import json
import os
import pickle
import shutil
import tempfile
import pytest
import fsspec
from fsspec.compression import compr
from fsspec.exceptions import BlocksizeMismatchError
from fsspec.implementations.cache_mapper import BasenameCacheMapper, HashCacheMapper, create_cache_mapper
from fsspec.implementations.cached import CachingFileSystem, LocalTempFile, WholeFileCacheFileSystem
from fsspec.implementations.local import make_path_posix
from fsspec.implementations.zip import ZipFileSystem
from fsspec.tests.conftest import win
from .test_ftp import FTPFileSystem

def test_equality(tmpdir):
    """Test sane behaviour for equality and hashing.

    Make sure that different CachingFileSystem only test equal to each other
    when they should, and do not test equal to the filesystem they rely upon.
    Similarly, make sure their hashes differ when they should and are equal
    when they should not.

    Related: GitHub#577, GitHub#578
    """
    pass

def test_str():
    """Test that the str representation refers to correct class."""
    pass