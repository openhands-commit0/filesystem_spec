import bz2
import gzip
import lzma
import os
import pickle
import tarfile
import tempfile
import zipfile
from contextlib import contextmanager
from io import BytesIO
import pytest
import fsspec
archive_data = {'a': b'', 'b': b'hello', 'deeply/nested/path': b'stuff'}

@contextmanager
def tempzip(data=None):
    """
    Provide test cases with temporary synthesized Zip archives.
    """
    pass

@contextmanager
def temparchive(data=None):
    """
    Provide test cases with temporary synthesized 7-Zip archives.
    """
    pass

@contextmanager
def temptar(data=None, mode='w', suffix='.tar'):
    """
    Provide test cases with temporary synthesized .tar archives.
    """
    pass

@contextmanager
def temptargz(data=None, mode='w', suffix='.tar.gz'):
    """
    Provide test cases with temporary synthesized .tar.gz archives.
    """
    pass

@contextmanager
def temptarbz2(data=None, mode='w', suffix='.tar.bz2'):
    """
    Provide test cases with temporary synthesized .tar.bz2 archives.
    """
    pass

@contextmanager
def temptarxz(data=None, mode='w', suffix='.tar.xz'):
    """
    Provide test cases with temporary synthesized .tar.xz archives.
    """
    pass

class ArchiveTestScenario:
    """
    Describe a test scenario for any type of archive.
    """

    def __init__(self, protocol=None, provider=None, variant=None):
        self.protocol = protocol
        self.provider = provider
        self.variant = variant

def pytest_generate_tests(metafunc):
    """
    Generate test scenario parametrization arguments with appropriate labels (idlist).

    On the one hand, this yields an appropriate output like::

        fsspec/implementations/tests/test_archive.py::TestArchive::test_empty[zip] PASSED  # noqa

    On the other hand, it will support perfect test discovery, like::

        pytest fsspec -vvv -k "zip or tar or libarchive"

    https://docs.pytest.org/en/latest/example/parametrize.html#a-quick-port-of-testscenarios
    """
    pass
scenario_zip = ArchiveTestScenario(protocol='zip', provider=tempzip)
scenario_tar = ArchiveTestScenario(protocol='tar', provider=temptar)
scenario_targz = ArchiveTestScenario(protocol='tar', provider=temptargz, variant='gz')
scenario_tarbz2 = ArchiveTestScenario(protocol='tar', provider=temptarbz2, variant='bz2')
scenario_tarxz = ArchiveTestScenario(protocol='tar', provider=temptarxz, variant='xz')
scenario_libarchive = ArchiveTestScenario(protocol='libarchive', provider=temparchive)

class TestAnyArchive:
    """
    Validate that all filesystem adapter implementations for archive files
    will adhere to the same specification.
    """
    scenarios = [scenario_zip, scenario_tar, scenario_targz, scenario_tarbz2, scenario_tarxz, scenario_libarchive]