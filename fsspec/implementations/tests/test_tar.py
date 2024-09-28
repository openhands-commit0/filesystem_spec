from __future__ import annotations
import os
import shutil
import tarfile
import tempfile
from io import BytesIO
from pathlib import Path
import pytest
import fsspec
from fsspec.core import OpenFile
from fsspec.implementations.cached import WholeFileCacheFileSystem
from fsspec.implementations.tar import TarFileSystem
from fsspec.implementations.tests.test_archive import archive_data, temptar

@pytest.mark.parametrize('recipe', [{'mode': 'w', 'suffix': '.tar', 'magic': b'a\x00\x00\x00\x00'}, {'mode': 'w:gz', 'suffix': '.tar.gz', 'magic': b'\x1f\x8b\x08\x08'}, {'mode': 'w:bz2', 'suffix': '.tar.bz2', 'magic': b'BZh91AY'}, {'mode': 'w:xz', 'suffix': '.tar.xz', 'magic': b'\xfd7zXZ\x00\x00'}], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_compressions(recipe):
    """
    Run tests on all available tar file compression variants.
    """
    pass

@pytest.mark.parametrize('recipe', [{'mode': 'w', 'suffix': '.tar', 'magic': b'a\x00\x00\x00\x00'}, {'mode': 'w:gz', 'suffix': '.tar.gz', 'magic': b'\x1f\x8b\x08\x08'}, {'mode': 'w:bz2', 'suffix': '.tar.bz2', 'magic': b'BZh91AY'}, {'mode': 'w:xz', 'suffix': '.tar.xz', 'magic': b'\xfd7zXZ\x00\x00'}], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_filesystem_direct(recipe, tmpdir):
    """
    Run tests through a real fsspec filesystem implementation.
    Here: `LocalFileSystem`.
    """
    pass

@pytest.mark.parametrize('recipe', [{'mode': 'w', 'suffix': '.tar', 'magic': b'a\x00\x00\x00\x00'}, {'mode': 'w:gz', 'suffix': '.tar.gz', 'magic': b'\x1f\x8b\x08\x08'}, {'mode': 'w:bz2', 'suffix': '.tar.bz2', 'magic': b'BZh91AY'}, {'mode': 'w:xz', 'suffix': '.tar.xz', 'magic': b'\xfd7zXZ\x00\x00'}], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_filesystem_cached(recipe, tmpdir):
    """
    Run tests through a real, cached, fsspec filesystem implementation.
    Here: `TarFileSystem` over `WholeFileCacheFileSystem` over `LocalFileSystem`.
    """
    pass

@pytest.mark.parametrize('compression', ['', 'gz', 'bz2', 'xz'], ids=['tar', 'tar-gz', 'tar-bz2', 'tar-xz'])
def test_ls_with_folders(compression: str, tmp_path: Path):
    """
    Create a tar file that doesn't include the intermediate folder structure,
    but make sure that the reading filesystem is still able to resolve the
    intermediate folders, like the ZipFileSystem.
    """
    pass