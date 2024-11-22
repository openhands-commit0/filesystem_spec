import asyncio
import io
import logging
import re
import weakref
from copy import copy
from urllib.parse import urlparse
import aiohttp
import yarl
from fsspec.asyn import AbstractAsyncStreamedFile, AsyncFileSystem, sync, sync_wrapper
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import DEFAULT_BLOCK_SIZE, glob_translate, isfilelike, nullcontext, tokenize

def get_client(**kwargs):
    """Create an aiohttp.ClientSession with the given kwargs"""
    return aiohttp.ClientSession(**kwargs)
from ..caching import AllBytes
ex = re.compile('<(a|A)\\s+(?:[^>]*?\\s+)?(href|HREF)=["\'](?P<url>[^"\']+)')
ex2 = re.compile('(?P<url>http[s]?://[-a-zA-Z0-9@:%_+.~#?&/=]+)')
logger = logging.getLogger('fsspec.http')

class HTTPFileSystem(AsyncFileSystem):
    """
    Simple File-System for fetching data via HTTP(S)

    ``ls()`` is implemented by loading the parent page and doing a regex
    match on the result. If simple_link=True, anything of the form
    "http(s)://server.com/stuff?thing=other"; otherwise only links within
    HTML href tags will be used.
    """
    sep = '/'

    def __init__(self, simple_links=True, block_size=None, same_scheme=True, size_policy=None, cache_type='bytes', cache_options=None, asynchronous=False, loop=None, client_kwargs=None, get_client=get_client, encoded=False, **storage_options):
        """
        NB: if this is called async, you must await set_client

        Parameters
        ----------
        block_size: int
            Blocks to read bytes; if 0, will default to raw requests file-like
            objects instead of HTTPFile instances
        simple_links: bool
            If True, will consider both HTML <a> tags and anything that looks
            like a URL; if False, will consider only the former.
        same_scheme: True
            When doing ls/glob, if this is True, only consider paths that have
            http/https matching the input URLs.
        size_policy: this argument is deprecated
        client_kwargs: dict
            Passed to aiohttp.ClientSession, see
            https://docs.aiohttp.org/en/stable/client_reference.html
            For example, ``{'auth': aiohttp.BasicAuth('user', 'pass')}``
        get_client: Callable[..., aiohttp.ClientSession]
            A callable which takes keyword arguments and constructs
            an aiohttp.ClientSession. It's state will be managed by
            the HTTPFileSystem class.
        storage_options: key-value
            Any other parameters passed on to requests
        cache_type, cache_options: defaults used in open
        """
        super().__init__(self, asynchronous=asynchronous, loop=loop, **storage_options)
        self.block_size = block_size if block_size is not None else DEFAULT_BLOCK_SIZE
        self.simple_links = simple_links
        self.same_schema = same_scheme
        self.cache_type = cache_type
        self.cache_options = cache_options
        self.client_kwargs = client_kwargs or {}
        self.get_client = get_client
        self.encoded = encoded
        self.kwargs = storage_options
        self._session = None
        request_options = copy(storage_options)
        self.use_listings_cache = request_options.pop('use_listings_cache', False)
        request_options.pop('listings_expiry_time', None)
        request_options.pop('max_paths', None)
        request_options.pop('skip_instance_cache', None)
        self.kwargs = request_options

    @classmethod
    def _strip_protocol(cls, path):
        """For HTTP, we always want to keep the full URL"""
        pass

    async def _ls(self, url, detail=True, **kwargs):
        """Get all files in a directory based on links in an HTML page"""
        session = await self.set_session()
        try:
            async with session.get(self.encode_url(url), **self.kwargs) as r:
                self._raise_not_found_for_status(r, url)
                text = await r.text()
        except Exception as e:
            if detail:
                raise
            return []

        if self.simple_links:
            links = ex2.findall(text) + [u[2] for u in ex.findall(text)]
        else:
            links = [u[2] for u in ex.findall(text)]

        out = set()
        parts = urlparse(url)
        for l in links:
            if l.startswith('/') and len(l) > 1:
                # absolute URL on this server
                l = parts.scheme + '://' + parts.netloc + l
            if l.startswith('http'):
                if self.same_schema and l.startswith(parts.scheme):
                    out.add(l)
                elif not self.same_schema:
                    out.add(l)
        if detail:
            return [await self._info(u) for u in out]
        return list(sorted(out))

    ls = sync_wrapper(_ls)

    def _raise_not_found_for_status(self, response, url):
        """
        Raises FileNotFoundError for 404s, otherwise uses raise_for_status.
        """
        pass

    def _open(self, path, mode='rb', block_size=None, autocommit=None, cache_type=None, cache_options=None, size=None, **kwargs):
        """Make a file-like object

        Parameters
        ----------
        path: str
            Full URL with protocol
        mode: string
            must be "rb"
        block_size: int or None
            Bytes to download in one request; use instance value if None. If
            zero, will return a streaming Requests file-like instance.
        kwargs: key-value
            Any other parameters, passed to requests calls
        """
        pass

    def ukey(self, url):
        """Unique identifier; assume HTTP files are static, unchanging"""
        pass

    async def _info(self, url, **kwargs):
        """Get info of URL

        Tries to access location via HEAD, and then GET methods, but does
        not fetch the data.

        It is possible that the server does not supply any size information, in
        which case size will be given as None (and certain operations on the
        corresponding file will not work).
        """
        pass

    async def _glob(self, path, maxdepth=None, **kwargs):
        """
        Find files by glob-matching.

        This implementation is idntical to the one in AbstractFileSystem,
        but "?" is not considered as a character for globbing, because it is
        so common in URLs, often identifying the "query" part.
        """
        pass

class HTTPFile(AbstractBufferedFile):
    """
    A file-like object pointing to a remove HTTP(S) resource

    Supports only reading, with read-ahead of a predermined block-size.

    In the case that the server does not supply the filesize, only reading of
    the complete file in one go is supported.

    Parameters
    ----------
    url: str
        Full URL of the remote resource, including the protocol
    session: aiohttp.ClientSession or None
        All calls will be made within this session, to avoid restarting
        connections where the server allows this
    block_size: int or None
        The amount of read-ahead to do, in bytes. Default is 5MB, or the value
        configured for the FileSystem creating this file
    size: None or int
        If given, this is the size of the file in bytes, and we don't attempt
        to call the server to find the value.
    kwargs: all other key-values are passed to requests calls.
    """

    def __init__(self, fs, url, session=None, block_size=None, mode='rb', cache_type='bytes', cache_options=None, size=None, loop=None, asynchronous=False, **kwargs):
        if mode != 'rb':
            raise NotImplementedError('File mode not supported')
        self.asynchronous = asynchronous
        self.loop = loop
        self.url = url
        self.session = session
        self.details = {'name': url, 'size': size, 'type': 'file'}
        super().__init__(fs=fs, path=url, mode=mode, block_size=block_size, cache_type=cache_type, cache_options=cache_options, **kwargs)

    def read(self, length=-1):
        """Read bytes from file

        Parameters
        ----------
        length: int
            Read up to this many bytes. If negative, read all content to end of
            file. If the server has not supplied the filesize, attempting to
            read only part of the data will raise a ValueError.
        """
        pass

    async def async_fetch_all(self):
        """Read whole file in one shot, without caching

        This is only called when position is still at zero,
        and read() is called without a byte-count.
        """
        pass
    _fetch_all = sync_wrapper(async_fetch_all)

    def _parse_content_range(self, headers):
        """Parse the Content-Range header"""
        pass

    async def async_fetch_range(self, start, end):
        """Download a block of data

        The expectation is that the server returns only the requested bytes,
        with HTTP code 206. If this is not the case, we first check the headers,
        and then stream the output - if the data size is bigger than we
        requested, an exception is raised.
        """
        pass
    _fetch_range = sync_wrapper(async_fetch_range)

    def __reduce__(self):
        return (reopen, (self.fs, self.url, self.mode, self.blocksize, self.cache.name if self.cache else 'none', self.size))
magic_check = re.compile('([*[])')

class HTTPStreamFile(AbstractBufferedFile):

    def __init__(self, fs, url, mode='rb', loop=None, session=None, **kwargs):
        self.asynchronous = kwargs.pop('asynchronous', False)
        self.url = url
        self.loop = loop
        self.session = session
        if mode != 'rb':
            raise ValueError
        self.details = {'name': url, 'size': None}
        super().__init__(fs=fs, path=url, mode=mode, cache_type='none', **kwargs)

        async def cor():
            r = await self.session.get(self.fs.encode_url(url), **kwargs).__aenter__()
            self.fs._raise_not_found_for_status(r, url)
            return r
        self.r = sync(self.loop, cor)
        self.loop = fs.loop

    async def _read(self, length=-1):
        """Read bytes from stream"""
        if length < 0:
            out = await self.r.read()
        else:
            out = await self.r.read(length)
        return out

    read = sync_wrapper(_read)

    def __reduce__(self):
        return (reopen, (self.fs, self.url, self.mode, self.blocksize, self.cache.name))

class AsyncStreamFile(AbstractAsyncStreamedFile):

    def __init__(self, fs, url, mode='rb', loop=None, session=None, size=None, **kwargs):
        self.url = url
        self.session = session
        self.r = None
        if mode != 'rb':
            raise ValueError
        self.details = {'name': url, 'size': None}
        self.kwargs = kwargs
        super().__init__(fs=fs, path=url, mode=mode, cache_type='none')
        self.size = size

async def _file_info(url, session, size_policy='head', **kwargs):
    """Call HEAD on the server to get details about the file (size/checksum etc.)

    Default operation is to explicitly allow redirects and use encoding
    'identity' (no compression) to get the true size of the target.
    """
    pass

async def _file_size(url, session=None, **kwargs):
    """Return the size of a file by URL

    Uses HTTP HEAD request by default
    """
    info = await _file_info(url, session=session, **kwargs)
    return info.get('size', None)

file_size = sync_wrapper(_file_size)