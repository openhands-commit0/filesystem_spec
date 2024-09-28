import pytest
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.spec import AbstractFileSystem
PATH = 'path/to/dir'
ARGS = ['foo', 'bar']
KWARGS = {'baz': 'baz', 'qux': 'qux'}