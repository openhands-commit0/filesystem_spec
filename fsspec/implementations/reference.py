import base64
import collections
import io
import itertools
import logging
import math
import os
from functools import lru_cache
from typing import TYPE_CHECKING
import fsspec.core
try:
    import ujson as json
except ImportError:
    if not TYPE_CHECKING:
        import json
from ..asyn import AsyncFileSystem
from ..callbacks import DEFAULT_CALLBACK
from ..core import filesystem, open, split_protocol
from ..utils import isfilelike, merge_offset_ranges, other_paths
logger = logging.getLogger('fsspec.reference')

class ReferenceNotReachable(RuntimeError):

    def __init__(self, reference, target, *args):
        super().__init__(*args)
        self.reference = reference
        self.target = target

    def __str__(self):
        return f'Reference "{self.reference}" failed to fetch target {self.target}'

class RefsValuesView(collections.abc.ValuesView):

    def __iter__(self):
        for val in self._mapping.zmetadata.values():
            yield json.dumps(val).encode()
        yield from self._mapping._items.values()
        for field in self._mapping.listdir():
            chunk_sizes = self._mapping._get_chunk_sizes(field)
            if len(chunk_sizes) == 0:
                yield self._mapping[field + '/0']
                continue
            yield from self._mapping._generate_all_records(field)

class RefsItemsView(collections.abc.ItemsView):

    def __iter__(self):
        return zip(self._mapping.keys(), self._mapping.values())

class LazyReferenceMapper(collections.abc.MutableMapping):
    """This interface can be used to read/write references from Parquet stores.
    It is not intended for other types of references.
    It can be used with Kerchunk's MultiZarrToZarr method to combine
    references into a parquet store.
    Examples of this use-case can be found here:
    https://fsspec.github.io/kerchunk/advanced.html?highlight=parquet#parquet-storage"""

    def __init__(self, root, fs=None, out_root=None, cache_size=128, categorical_threshold=10):
        """

        This instance will be writable, storing changes in memory until full partitions
        are accumulated or .flush() is called.

        To create an empty lazy store, use .create()

        Parameters
        ----------
        root : str
            Root of parquet store
        fs : fsspec.AbstractFileSystem
            fsspec filesystem object, default is local filesystem.
        cache_size : int, default=128
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
        categorical_threshold : int
            Encode urls as pandas.Categorical to reduce memory footprint if the ratio
            of the number of unique urls to total number of refs for each variable
            is greater than or equal to this number. (default 10)
        """
        self.root = root
        self.chunk_sizes = {}
        self.out_root = out_root or self.root
        self.cat_thresh = categorical_threshold
        self.cache_size = cache_size
        self.dirs = None
        self.url = self.root + '/{field}/refs.{record}.parq'
        self.fs = fsspec.filesystem('file') if fs is None else fs

    def __getattr__(self, item):
        if item in ('_items', 'record_size', 'zmetadata'):
            self.setup()
            return self.__dict__[item]
        raise AttributeError(item)

    @staticmethod
    def create(root, storage_options=None, fs=None, record_size=10000, **kwargs):
        """Make empty parquet reference set

        First deletes the contents of the given directory, if it exists.

        Parameters
        ----------
        root: str
            Directory to contain the output; will be created
        storage_options: dict | None
            For making the filesystem to use for writing is fs is None
        fs: FileSystem | None
            Filesystem for writing
        record_size: int
            Number of references per parquet file
        kwargs: passed to __init__

        Returns
        -------
        LazyReferenceMapper instance
        """
        pass

    def listdir(self, basename=True):
        """List top-level directories"""
        pass

    def ls(self, path='', detail=True):
        """Shortcut file listings"""
        pass

    def _load_one_key(self, key):
        """Get the reference for one key

        Returns bytes, one-element list or three-element list.
        """
        pass

    @lru_cache(4096)
    def _key_to_record(self, key):
        """Details needed to construct a reference for one key"""
        pass

    def _get_chunk_sizes(self, field):
        """The number of chunks along each axis for a given field"""
        pass

    def _generate_record(self, field, record):
        """The references for a given parquet file of a given field"""
        pass

    def _generate_all_records(self, field):
        """Load all the references within a field by iterating over the parquet files"""
        pass

    def __hash__(self):
        return id(self)

    def __getitem__(self, key):
        return self._load_one_key(key)

    def __setitem__(self, key, value):
        if '/' in key and (not self._is_meta(key)):
            field, chunk = key.rsplit('/', 1)
            record, i, _ = self._key_to_record(key)
            subdict = self._items.setdefault((field, record), {})
            subdict[i] = value
            if len(subdict) == self.record_size:
                self.write(field, record)
        else:
            self._items[key] = value
            new_value = json.loads(value.decode() if isinstance(value, bytes) else value)
            self.zmetadata[key] = {**self.zmetadata.get(key, {}), **new_value}

    def __delitem__(self, key):
        if key in self._items:
            del self._items[key]
        elif key in self.zmetadata:
            del self.zmetadata[key]
        elif '/' in key and (not self._is_meta(key)):
            field, _ = key.rsplit('/', 1)
            record, i, _ = self._key_to_record(key)
            subdict = self._items.setdefault((field, record), {})
            subdict[i] = None
            if len(subdict) == self.record_size:
                self.write(field, record)
        else:
            self._items[key] = None

    def flush(self, base_url=None, storage_options=None):
        """Output any modified or deleted keys

        Parameters
        ----------
        base_url: str
            Location of the output
        """
        pass

    def __len__(self):
        count = 0
        for field in self.listdir():
            if field.startswith('.'):
                count += 1
            else:
                count += math.prod(self._get_chunk_sizes(field))
        count += len(self.zmetadata)
        count += sum((1 for _ in self._items if not isinstance(_, tuple)))
        return count

    def __iter__(self):
        metas = set(self.zmetadata)
        metas.update(self._items)
        for bit in metas:
            if isinstance(bit, str):
                yield bit
        for field in self.listdir():
            for k in self._keys_in_field(field):
                if k in self:
                    yield k

    def __contains__(self, item):
        try:
            self._load_one_key(item)
            return True
        except KeyError:
            return False

    def _keys_in_field(self, field):
        """List key names in given field

        Produces strings like "field/x.y" appropriate from the chunking of the array
        """
        pass

class ReferenceFileSystem(AsyncFileSystem):
    """View byte ranges of some other file as a file system
    Initial version: single file system target, which must support
    async, and must allow start and end args in _cat_file. Later versions
    may allow multiple arbitrary URLs for the targets.
    This FileSystem is read-only. It is designed to be used with async
    targets (for now). This FileSystem only allows whole-file access, no
    ``open``. We do not get original file details from the target FS.
    Configuration is by passing a dict of references at init, or a URL to
    a JSON file containing the same; this dict
    can also contain concrete data for some set of paths.
    Reference dict format:
    {path0: bytes_data, path1: (target_url, offset, size)}
    https://github.com/fsspec/kerchunk/blob/main/README.md
    """
    protocol = 'reference'

    def __init__(self, fo, target=None, ref_storage_args=None, target_protocol=None, target_options=None, remote_protocol=None, remote_options=None, fs=None, template_overrides=None, simple_templates=True, max_gap=64000, max_block=256000000, cache_size=128, **kwargs):
        """
        Parameters
        ----------
        fo : dict or str
            The set of references to use for this instance, with a structure as above.
            If str referencing a JSON file, will use fsspec.open, in conjunction
            with target_options and target_protocol to open and parse JSON at this
            location. If a directory, then assume references are a set of parquet
            files to be loaded lazily.
        target : str
            For any references having target_url as None, this is the default file
            target to use
        ref_storage_args : dict
            If references is a str, use these kwargs for loading the JSON file.
            Deprecated: use target_options instead.
        target_protocol : str
            Used for loading the reference file, if it is a path. If None, protocol
            will be derived from the given path
        target_options : dict
            Extra FS options for loading the reference file ``fo``, if given as a path
        remote_protocol : str
            The protocol of the filesystem on which the references will be evaluated
            (unless fs is provided). If not given, will be derived from the first
            URL that has a protocol in the templates or in the references, in that
            order.
        remote_options : dict
            kwargs to go with remote_protocol
        fs : AbstractFileSystem | dict(str, (AbstractFileSystem | dict))
            Directly provide a file system(s):
                - a single filesystem instance
                - a dict of protocol:filesystem, where each value is either a filesystem
                  instance, or a dict of kwargs that can be used to create in
                  instance for the given protocol

            If this is given, remote_options and remote_protocol are ignored.
        template_overrides : dict
            Swap out any templates in the references file with these - useful for
            testing.
        simple_templates: bool
            Whether templates can be processed with simple replace (True) or if
            jinja  is needed (False, much slower). All reference sets produced by
            ``kerchunk`` are simple in this sense, but the spec allows for complex.
        max_gap, max_block: int
            For merging multiple concurrent requests to the same remote file.
            Neighboring byte ranges will only be merged when their
            inter-range gap is <= ``max_gap``. Default is 64KB. Set to 0
            to only merge when it requires no extra bytes. Pass a negative
            number to disable merging, appropriate for local target files.
            Neighboring byte ranges will only be merged when the size of
            the aggregated range is <= ``max_block``. Default is 256MB.
        cache_size : int
            Maximum size of LRU cache, where cache_size*record_size denotes
            the total number of references that can be loaded in memory at once.
            Only used for lazily loaded references.
        kwargs : passed to parent class
        """
        super().__init__(**kwargs)
        self.target = target
        self.template_overrides = template_overrides
        self.simple_templates = simple_templates
        self.templates = {}
        self.fss = {}
        self._dircache = {}
        self.max_gap = max_gap
        self.max_block = max_block
        if isinstance(fo, str):
            dic = dict(**ref_storage_args or target_options or {}, protocol=target_protocol)
            ref_fs, fo2 = fsspec.core.url_to_fs(fo, **dic)
            if ref_fs.isfile(fo2):
                with fsspec.open(fo, 'rb', **dic) as f:
                    logger.info('Read reference from URL %s', fo)
                    text = json.load(f)
                self._process_references(text, template_overrides)
            else:
                logger.info('Open lazy reference dict from URL %s', fo)
                self.references = LazyReferenceMapper(fo2, fs=ref_fs, cache_size=cache_size)
        else:
            self._process_references(fo, template_overrides)
        if isinstance(fs, dict):
            self.fss = {k: fsspec.filesystem(k.split(':', 1)[0], **opts) if isinstance(opts, dict) else opts for k, opts in fs.items()}
            if None not in self.fss:
                self.fss[None] = filesystem('file')
            return
        if fs is not None:
            remote_protocol = fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
            self.fss[remote_protocol] = fs
        if remote_protocol is None:
            for ref in self.templates.values():
                if callable(ref):
                    ref = ref()
                protocol, _ = fsspec.core.split_protocol(ref)
                if protocol and protocol not in self.fss:
                    fs = filesystem(protocol, **remote_options or {})
                    self.fss[protocol] = fs
        if remote_protocol is None:
            for ref in self.references.values():
                if callable(ref):
                    ref = ref()
                if isinstance(ref, list) and ref[0]:
                    protocol, _ = fsspec.core.split_protocol(ref[0])
                    if protocol not in self.fss:
                        fs = filesystem(protocol, **remote_options or {})
                        self.fss[protocol] = fs
                        break
        if remote_protocol and remote_protocol not in self.fss:
            fs = filesystem(remote_protocol, **remote_options or {})
            self.fss[remote_protocol] = fs
        self.fss[None] = fs or filesystem('file')

    def pipe_file(self, path, value, **_):
        """Temporarily add binary data or reference as a file"""
        pass

    def _process_references0(self, references):
        """Make reference dict for Spec Version 0"""
        pass

    def save_json(self, url, **storage_options):
        """Write modified references into new location"""
        pass