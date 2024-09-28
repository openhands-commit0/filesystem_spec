import os
import pygit2
from fsspec.spec import AbstractFileSystem
from .memory import MemoryFile

class GitFileSystem(AbstractFileSystem):
    """Browse the files of a local git repo at any hash/tag/branch

    (experimental backend)
    """
    root_marker = ''
    cachable = True

    def __init__(self, path=None, fo=None, ref=None, **kwargs):
        """

        Parameters
        ----------
        path: str (optional)
            Local location of the repo (uses current directory if not given).
            May be deprecated in favour of ``fo``. When used with a higher
            level function such as fsspec.open(), may be of the form
            "git://[path-to-repo[:]][ref@]path/to/file" (but the actual
            file path should not contain "@" or ":").
        fo: str (optional)
            Same as ``path``, but passed as part of a chained URL. This one
            takes precedence if both are given.
        ref: str (optional)
            Reference to work with, could be a hash, tag or branch name. Defaults
            to current working tree. Note that ``ls`` and ``open`` also take hash,
            so this becomes the default for those operations
        kwargs
        """
        super().__init__(**kwargs)
        self.repo = pygit2.Repository(fo or path or os.getcwd())
        self.ref = ref or 'master'