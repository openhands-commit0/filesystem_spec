from __future__ import annotations
import os
import pickle
import time
from typing import TYPE_CHECKING
from fsspec.utils import atomic_write
try:
    import ujson as json
except ImportError:
    if not TYPE_CHECKING:
        import json
if TYPE_CHECKING:
    from typing import Any, Dict, Iterator, Literal
    from typing_extensions import TypeAlias
    from .cached import CachingFileSystem
    Detail: TypeAlias = Dict[str, Any]

class CacheMetadata:
    """Cache metadata.

    All reading and writing of cache metadata is performed by this class,
    accessing the cached files and blocks is not.

    Metadata is stored in a single file per storage directory in JSON format.
    For backward compatibility, also reads metadata stored in pickle format
    which is converted to JSON when next saved.
    """

    def __init__(self, storage: list[str]):
        """

        Parameters
        ----------
        storage: list[str]
            Directories containing cached files, must be at least one. Metadata
            is stored in the last of these directories by convention.
        """
        if not storage:
            raise ValueError('CacheMetadata expects at least one storage location')
        self._storage = storage
        self.cached_files: list[Detail] = [{}]
        self._force_save_pickle = False

    def _load(self, fn: str) -> Detail:
        """Low-level function to load metadata from specific file"""
        pass

    def _save(self, metadata_to_save: Detail, fn: str) -> None:
        """Low-level function to save metadata to specific file"""
        pass

    def _scan_locations(self, writable_only: bool=False) -> Iterator[tuple[str, str, bool]]:
        """Yield locations (filenames) where metadata is stored, and whether
        writable or not.

        Parameters
        ----------
        writable: bool
            Set to True to only yield writable locations.

        Returns
        -------
        Yields (str, str, bool)
        """
        pass

    def check_file(self, path: str, cfs: CachingFileSystem | None) -> Literal[False] | tuple[Detail, str]:
        """If path is in cache return its details, otherwise return ``False``.

        If the optional CachingFileSystem is specified then it is used to
        perform extra checks to reject possible matches, such as if they are
        too old.
        """
        pass

    def clear_expired(self, expiry_time: int) -> tuple[list[str], bool]:
        """Remove expired metadata from the cache.

        Returns names of files corresponding to expired metadata and a boolean
        flag indicating whether the writable cache is empty. Caller is
        responsible for deleting the expired files.
        """
        pass

    def load(self) -> None:
        """Load all metadata from disk and store in ``self.cached_files``"""
        pass

    def on_close_cached_file(self, f: Any, path: str) -> None:
        """Perform side-effect actions on closing a cached file.

        The actual closing of the file is the responsibility of the caller.
        """
        pass

    def pop_file(self, path: str) -> str | None:
        """Remove metadata of cached file.

        If path is in the cache, return the filename of the cached file,
        otherwise return ``None``.  Caller is responsible for deleting the
        cached file.
        """
        pass

    def save(self) -> None:
        """Save metadata to disk"""
        pass

    def update_file(self, path: str, detail: Detail) -> None:
        """Update metadata for specific file in memory, do not save"""
        pass