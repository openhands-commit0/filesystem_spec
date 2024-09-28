import os
import shlex
import subprocess
import time
import pytest
import fsspec
pytest.importorskip('notebook')
requests = pytest.importorskip('requests')