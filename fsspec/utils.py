from __future__ import annotations
import contextlib
import logging
import math
import os
import pathlib
import re
import sys
import tempfile
from functools import partial
from hashlib import md5
from importlib.metadata import version
from typing import IO, TYPE_CHECKING, Any, Callable, Iterable, Iterator, Sequence, TypeVar
from urllib.parse import urlsplit

def _unstrip_protocol(name: str) -> str:
    """Convert back to "protocol://path" format

    Parameters
    ----------
    name : str
        Input path, like "protocol://path" or "path"

    Returns
    -------
    str
        Path with protocol prefix
    """
    if "://" in name:
        return name
    return "file://" + name

def is_exception(obj: Any) -> bool:
    """Test if an object is an Exception or subclass"""
    return isinstance(obj, BaseException) or (isinstance(obj, type) and issubclass(obj, BaseException))

def get_protocol(urlpath: str) -> str | None:
    """Return protocol from given URL or None if no protocol is found"""
    if "://" in urlpath:
        return urlpath.split("://")[0]
    return None

def setup_logging(logger: logging.Logger | None=None, level: str | int="INFO") -> None:
    """Configure logging for fsspec

    Parameters
    ----------
    logger: logging.Logger or None
        Logger to configure. If None, uses the root logger
    level: str or int
        Logging level, like "INFO" or logging.INFO
    """
    if logger is None:
        logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)

@contextlib.contextmanager
def nullcontext(enter_result=None):
    """Context manager that does nothing

    Useful for conditional context manager usage where one branch does nothing.
    """
    yield enter_result

def isfilelike(obj: Any) -> bool:
    """Test if an object implements the file-like protocol (read/write/seek)"""
    return hasattr(obj, "read") and hasattr(obj, "seek") and hasattr(obj, "write")
if TYPE_CHECKING:
    from typing_extensions import TypeGuard
    from fsspec.spec import AbstractFileSystem
DEFAULT_BLOCK_SIZE = 5 * 2 ** 20
T = TypeVar('T')

def infer_storage_options(urlpath: str, inherit_storage_options: dict[str, Any] | None=None) -> dict[str, Any]:
    """Infer storage options from URL path and merge it with existing storage
    options.

    Parameters
    ----------
    urlpath: str or unicode
        Either local absolute file path or URL (hdfs://namenode:8020/file.csv)
    inherit_storage_options: dict (optional)
        Its contents will get merged with the inferred information from the
        given path

    Returns
    -------
    Storage options dict.

    Examples
    --------
    >>> infer_storage_options('/mnt/datasets/test.csv')  # doctest: +SKIP
    {"protocol": "file", "path", "/mnt/datasets/test.csv"}
    >>> infer_storage_options(
    ...     'hdfs://username:pwd@node:123/mnt/datasets/test.csv?q=1',
    ...     inherit_storage_options={'extra': 'value'},
    ... )  # doctest: +SKIP
    {"protocol": "hdfs", "username": "username", "password": "pwd",
    "host": "node", "port": 123, "path": "/mnt/datasets/test.csv",
    "url_query": "q=1", "extra": "value"}
    """
    result = {}
    if inherit_storage_options:
        result.update(inherit_storage_options)

    if not isinstance(urlpath, str):
        return result

    parsed_path = urlsplit(urlpath)
    protocol = parsed_path.scheme or 'file'
    result['protocol'] = protocol

    if protocol == 'file':
        result['path'] = urlpath
    else:
        path = parsed_path.path
        if parsed_path.netloc:
            if '@' in parsed_path.netloc:
                auth, netloc = parsed_path.netloc.split('@', 1)
                if ':' in auth:
                    result['username'], result['password'] = auth.split(':', 1)
                else:
                    result['username'] = auth
            else:
                netloc = parsed_path.netloc

            if ':' in netloc:
                host, port = netloc.split(':', 1)
                try:
                    port = int(port)
                except ValueError:
                    port = None
                result['host'] = host
                if port:
                    result['port'] = port
            else:
                result['host'] = netloc

        result['path'] = path or '/'
        if parsed_path.query:
            result['url_query'] = parsed_path.query

    return result
compressions: dict[str, str] = {}

def infer_compression(filename: str) -> str | None:
    """Infer compression, if available, from filename.

    Infer a named compression type, if registered and available, from filename
    extension. This includes builtin (gz, bz2, zip) compressions, as well as
    optional compressions. See fsspec.compression.register_compression.
    """
    if not isinstance(filename, str):
        return None
    for ext, comp in compressions.items():
        if filename.endswith('.' + ext):
            return comp
    return None

def build_name_function(max_int: float) -> Callable[[int], str]:
    """Returns a function that receives a single integer
    and returns it as a string padded by enough zero characters
    to align with maximum possible integer

    >>> name_f = build_name_function(57)

    >>> name_f(7)
    '07'
    >>> name_f(31)
    '31'
    >>> build_name_function(1000)(42)
    '0042'
    >>> build_name_function(999)(42)
    '042'
    >>> build_name_function(0)(0)
    '0'
    """
    width = len(str(int(max_int)))
    return lambda x: str(x).zfill(width)

def seek_delimiter(file: IO[bytes], delimiter: bytes, blocksize: int) -> bool:
    """Seek current file to file start, file end, or byte after delimiter seq.

    Seeks file to next chunk delimiter, where chunks are defined on file start,
    a delimiting sequence, and file end. Use file.tell() to see location afterwards.
    Note that file start is a valid split, so must be at offset > 0 to seek for
    delimiter.

    Parameters
    ----------
    file: a file
    delimiter: bytes
        a delimiter like ``b'\\n'`` or message sentinel, matching file .read() type
    blocksize: int
        Number of bytes to read from the file at once.


    Returns
    -------
    Returns True if a delimiter was found, False if at file start or end.

    """
    if file.tell() == 0:
        return False

    last = b''
    while True:
        current = file.read(blocksize)
        if not current:
            return False
        full = last + current
        possible = full.find(delimiter)
        if possible < 0:
            last = full[-len(delimiter):]
            file.seek(-(len(delimiter) - 1), 1)
        else:
            file.seek(-(len(full) - possible - len(delimiter)), 1)
            return True

def read_block(f: IO[bytes], offset: int, length: int | None, delimiter: bytes | None=None, split_before: bool=False) -> bytes:
    """Read a block of bytes from a file

    Parameters
    ----------
    f: File
        Open file
    offset: int
        Byte offset to start read
    length: int
        Number of bytes to read, read through end of file if None
    delimiter: bytes (optional)
        Ensure reading starts and stops at delimiter bytestring
    split_before: bool (optional)
        Start/stop read *before* delimiter bytestring.


    If using the ``delimiter=`` keyword argument we ensure that the read
    starts and stops at delimiter boundaries that follow the locations
    ``offset`` and ``offset + length``.  If ``offset`` is zero then we
    start at zero, regardless of delimiter.  The bytestring returned WILL
    include the terminating delimiter string.

    Examples
    --------

    >>> from io import BytesIO  # doctest: +SKIP
    >>> f = BytesIO(b'Alice, 100\\nBob, 200\\nCharlie, 300')  # doctest: +SKIP
    >>> read_block(f, 0, 13)  # doctest: +SKIP
    b'Alice, 100\\nBo'

    >>> read_block(f, 0, 13, delimiter=b'\\n')  # doctest: +SKIP
    b'Alice, 100\\nBob, 200\\n'

    >>> read_block(f, 10, 10, delimiter=b'\\n')  # doctest: +SKIP
    b'Bob, 200\\nCharlie, 300'
    """
    if offset < 0:
        raise ValueError("Offset must be non-negative")

    if delimiter and offset > 0:
        f.seek(0)
        found = False
        while True:
            block = f.read(2**16)
            if not block:
                break
            if delimiter in block:
                found = True
                break
        if not found:
            raise ValueError("Delimiter not found")
        f.seek(offset)

    if length is None:
        length = 2**30

    f.seek(offset)
    bytes_read = f.read(length)

    if delimiter:
        if split_before:
            if bytes_read.endswith(delimiter):
                bytes_read = bytes_read[:-len(delimiter)]
        else:
            if not bytes_read.endswith(delimiter):
                bytes_read += f.read(len(delimiter))
                while not bytes_read.endswith(delimiter):
                    block = f.read(2**16)
                    if not block:
                        break
                    bytes_read += block

    return bytes_read

def tokenize(*args: Any, **kwargs: Any) -> str:
    """Deterministic token

    (modified from dask.base)

    >>> tokenize([1, 2, '3'])
    '9d71491b50023b06fc76928e6eddb952'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    if kwargs is None:
        kwargs = {}
    if kwargs:
        args = args + (sorted(kwargs.items()),)
    return md5(str(args).encode()).hexdigest()

def stringify_path(filepath: str | os.PathLike[str] | pathlib.Path) -> str:
    """Attempt to convert a path-like object to a string.

    Parameters
    ----------
    filepath: object to be converted

    Returns
    -------
    filepath_str: maybe a string version of the object

    Notes
    -----
    Objects supporting the fspath protocol are coerced according to its
    __fspath__ method.

    For backwards compatibility with older Python version, pathlib.Path
    objects are specially coerced.

    Any other object is passed through unchanged, which includes bytes,
    strings, buffers, or anything else that's not even path-like.
    """
    if isinstance(filepath, str):
        return filepath
    if hasattr(filepath, '__fspath__'):
        return filepath.__fspath__()
    if isinstance(filepath, pathlib.Path):
        return str(filepath)
    return filepath

def common_prefix(paths: Iterable[str]) -> str:
    """For a list of paths, find the shortest prefix common to all"""
    paths = list(paths)
    if not paths:
        return ''
    if len(paths) == 1:
        return paths[0]

    # Convert Windows paths to POSIX paths
    paths = [p.replace('\\', '/') for p in paths]

    s1 = min(paths)
    s2 = max(paths)
    for i, c in enumerate(s1):
        if c != s2[i]:
            return s1[:i]
    return s1

def other_paths(paths: list[str], path2: str | list[str], exists: bool=False, flatten: bool=False) -> list[str]:
    """In bulk file operations, construct a new file tree from a list of files

    Parameters
    ----------
    paths: list of str
        The input file tree
    path2: str or list of str
        Root to construct the new list in. If this is already a list of str, we just
        assert it has the right number of elements.
    exists: bool (optional)
        For a str destination, it is already exists (and is a dir), files should
        end up inside.
    flatten: bool (optional)
        Whether to flatten the input directory tree structure so that the output files
        are in the same directory.

    Returns
    -------
    list of str
    """
    if isinstance(path2, str):
        path2 = path2.replace('\\', '/')
        if path2.endswith('/'):
            exists = True
        if not exists:
            path2 = path2.rstrip('/')
        if exists:
            if not path2.endswith('/'):
                path2 = path2 + '/'
            if flatten:
                return [path2 + os.path.basename(p) for p in paths]
            else:
                cp = common_prefix(paths)
                return [path2 + p[len(cp):].lstrip('/') for p in paths]
        else:
            if len(paths) > 1:
                raise ValueError("If not exists and str target, source must be single file")
            return [path2]
    else:
        if len(paths) != len(path2):
            raise ValueError("Different lengths for source and destination")
        return path2

def can_be_local(path: str) -> bool:
    """Can the given URL be used with open_local?"""
    if not isinstance(path, str):
        return False
    path = path.replace('\\', '/')
    if path.startswith('file://'):
        return True
    if '://' not in path:
        return True
    if path.startswith('simplecache::'):
        return True
    return False

def get_package_version_without_import(name: str) -> str | None:
    """For given package name, try to find the version without importing it

    Import and package.__version__ is still the backup here, so an import
    *might* happen.

    Returns either the version string, or None if the package
    or the version was not readily  found.
    """
    try:
        return version(name)
    except Exception:
        return None

def mirror_from(origin_name: str, methods: Iterable[str]) -> Callable[[type[T]], type[T]]:
    """Mirror attributes and methods from the given
    origin_name attribute of the instance to the
    decorated class"""
    def wrapper(cls: type[T]) -> type[T]:
        def make_method(method: str) -> Callable:
            def _method(self, *args, **kwargs):
                origin = getattr(self, origin_name)
                return getattr(origin, method)(*args, **kwargs)
            return _method

        for method in methods:
            setattr(cls, method, make_method(method))
        return cls
    return wrapper

def merge_offset_ranges(paths: list[str], starts: list[int] | int, ends: list[int] | int, max_gap: int=0, max_block: int | None=None, sort: bool=True) -> tuple[list[str], list[int], list[int]]:
    """Merge adjacent byte-offset ranges when the inter-range
    gap is <= `max_gap`, and when the merged byte range does not
    exceed `max_block` (if specified). By default, this function
    will re-order the input paths and byte ranges to ensure sorted
    order. If the user can guarantee that the inputs are already
    sorted, passing `sort=False` will skip the re-ordering.
    """
    if isinstance(starts, int):
        starts = [starts] * len(paths)
    if isinstance(ends, int):
        ends = [ends] * len(paths)
    if len(paths) != len(starts) or len(paths) != len(ends):
        raise ValueError("paths, starts, and ends must have same length")

    if sort:
        # Sort by path and start position
        items = sorted(zip(paths, starts, ends))
        paths, starts, ends = zip(*items)
        paths, starts, ends = list(paths), list(starts), list(ends)

    if not paths:
        return [], [], []

    out_paths = [paths[0]]
    out_starts = [starts[0]]
    out_ends = [ends[0]]

    for path, start, end in zip(paths[1:], starts[1:], ends[1:]):
        if (path == out_paths[-1] and
            (start - out_ends[-1] <= max_gap) and
            (max_block is None or end - out_starts[-1] <= max_block)):
            # Merge with previous range
            out_ends[-1] = end
        else:
            # Start new range
            out_paths.append(path)
            out_starts.append(start)
            out_ends.append(end)

    return out_paths, out_starts, out_ends

def file_size(filelike: IO[bytes]) -> int:
    """Find length of any open read-mode file-like"""
    try:
        return filelike.seek(0, ESPIPE)
    except (IOError, AttributeError):
        pass

    pos = filelike.tell()
    filelike.seek(0, 2)
    size = filelike.tell()
    filelike.seek(pos)
    return size

@contextlib.contextmanager
def atomic_write(path: str, mode: str='wb'):
    """
    A context manager that opens a temporary file next to `path` and, on exit,
    replaces `path` with the temporary file, thereby updating `path`
    atomically.
    """
    dir_path = os.path.dirname(path) or '.'
    basename = os.path.basename(path)
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode=mode, prefix=basename + '.', suffix='.tmp',
            dir=dir_path, delete=False
        ) as f:
            temp_path = f.name
            yield f

        # On Windows, we need to close the file before renaming
        os.replace(temp_path, path)
        temp_path = None
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass

def glob_translate(pat):
    """Translate a pathname with shell wildcards to a regular expression."""
    if not pat:
        return ''

    # Convert Windows paths to POSIX paths
    pat = pat.replace('\\', '/')

    # Special case for matching a literal '*'
    if pat == '*':
        return '[^/]*'

    # Special case for matching a literal '**'
    if pat == '**':
        return '.*'

    # Convert shell wildcards to regex
    i, n = 0, len(pat)
    res = []
    while i < n:
        c = pat[i]
        i = i + 1
        if c == '*':
            if i < n and pat[i] == '*':
                # Handle **
                i = i + 1
                if i < n and pat[i] == '/':
                    i = i + 1
                    res.append('(?:/.+)?')
                else:
                    res.append('.*')
            else:
                # Handle *
                res.append('[^/]*')
        elif c == '?':
            res.append('[^/]')
        elif c == '[':
            j = i
            if j < n and pat[j] == '!':
                j = j + 1
            if j < n and pat[j] == ']':
                j = j + 1
            while j < n and pat[j] != ']':
                j = j + 1
            if j >= n:
                res.append('\\[')
            else:
                stuff = pat[i:j].replace('\\', '\\\\')
                i = j + 1
                if stuff[0] == '!':
                    stuff = '^' + stuff[1:]
                elif stuff[0] == '^':
                    stuff = '\\' + stuff
                res.append('[' + stuff + ']')
        else:
            res.append(re.escape(c))
    return ''.join(res)