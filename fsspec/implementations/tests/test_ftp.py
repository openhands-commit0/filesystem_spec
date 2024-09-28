import os
import subprocess
import sys
import time
import pytest
import fsspec
from fsspec import open_files
from fsspec.implementations.ftp import FTPFileSystem
ftplib = pytest.importorskip('ftplib')
here = os.path.dirname(os.path.abspath(__file__))