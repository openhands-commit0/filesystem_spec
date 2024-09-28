import pickle
import shlex
import subprocess
import time
import pytest
import fsspec
requests = pytest.importorskip('requests')
from fsspec.implementations.webhdfs import WebHDFS