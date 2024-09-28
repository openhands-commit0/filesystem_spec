import logging
import tarfile
import fsspec
from fsspec.archive import AbstractArchiveFileSystem
from fsspec.compression import compr
from fsspec.utils import infer_compression
typemap = {b'0': 'file', b'5': 'directory'}
logger = logging.getLogger('tar')

class TarFileSystem(AbstractArchiveFileSystem):
    """Compressed Tar archives as a file-system (read-only)

    Supports the following formats:
    tar.gz, tar.bz2, tar.xz
    """
    root_marker = ''
    protocol = 'tar'
    cachable = False

    def __init__(self, fo='', index_store=None, target_options=None, target_protocol=None, compression=None, **kwargs):
        super().__init__(**kwargs)
        target_options = target_options or {}
        if isinstance(fo, str):
            self.of = fsspec.open(fo, protocol=target_protocol, **target_options)
            fo = self.of.open()
        if compression is None:
            name = None
            try:
                if hasattr(fo, 'original'):
                    name = fo.original
                elif hasattr(fo, 'path'):
                    name = fo.path
                elif hasattr(fo, 'name'):
                    name = fo.name
                elif hasattr(fo, 'info'):
                    name = fo.info()['name']
            except Exception as ex:
                logger.warning(f'Unable to determine file name, not inferring compression: {ex}')
            if name is not None:
                compression = infer_compression(name)
                logger.info(f'Inferred compression {compression} from file name {name}')
        if compression is not None:
            fo = compr[compression](fo)
        self._fo_ref = fo
        self.fo = fo
        self.tar = tarfile.TarFile(fileobj=self.fo)
        self.dir_cache = None
        self.index_store = index_store
        self.index = None
        self._index()