import os
from pathlib import PurePosixPath, PureWindowsPath
import pytest
from fsspec.implementations.local import LocalFileSystem, make_path_posix