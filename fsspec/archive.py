from fsspec import AbstractFileSystem
from fsspec.utils import tokenize

class AbstractArchiveFileSystem(AbstractFileSystem):
    """
    A generic superclass for implementing Archive-based filesystems.

    Currently, it is shared amongst
    :class:`~fsspec.implementations.zip.ZipFileSystem`,
    :class:`~fsspec.implementations.libarchive.LibArchiveFileSystem` and
    :class:`~fsspec.implementations.tar.TarFileSystem`.
    """

    def __str__(self):
        return f'<Archive-like object {type(self).__name__} at {id(self)}>'
    __repr__ = __str__

    def _all_dirnames(self, paths):
        """Returns *all* directory names for each path in paths, including intermediate
        ones.

        Parameters
        ----------
        paths: Iterable of path strings
        """
        pass