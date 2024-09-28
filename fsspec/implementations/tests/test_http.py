import asyncio
import io
import json
import os
import sys
import time
import aiohttp
import pytest
import fsspec.asyn
import fsspec.utils
from fsspec.implementations.http import HTTPStreamFile
from fsspec.tests.conftest import data, reset_files, server, win