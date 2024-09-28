import collections.abc
import os.path
import pytest
import fsspec
from fsspec.implementations.tests.test_archive import archive_data, tempzip

def test_fsspec_get_mapper():
    """Added for #788"""
    pass