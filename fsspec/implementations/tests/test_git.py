import os
import shutil
import subprocess
import tempfile
import pytest
import fsspec
from fsspec.implementations.local import make_path_posix
pygit2 = pytest.importorskip('pygit2')