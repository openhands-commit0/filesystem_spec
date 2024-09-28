import json
from contextlib import suppress
from pathlib import PurePath
from typing import Any, Callable, ClassVar, Dict, List, Mapping, Optional, Sequence, Tuple
from .registry import _import_class, get_filesystem_class
from .spec import AbstractFileSystem

class FilesystemJSONEncoder(json.JSONEncoder):
    include_password: ClassVar[bool] = True

    def make_serializable(self, obj: Any) -> Any:
        """
        Recursively converts an object so that it can be JSON serialized via
        :func:`json.dumps` and :func:`json.dump`, without actually calling
        said functions.
        """
        pass

class FilesystemJSONDecoder(json.JSONDecoder):

    def __init__(self, *, object_hook: Optional[Callable[[Dict[str, Any]], Any]]=None, parse_float: Optional[Callable[[str], Any]]=None, parse_int: Optional[Callable[[str], Any]]=None, parse_constant: Optional[Callable[[str], Any]]=None, strict: bool=True, object_pairs_hook: Optional[Callable[[List[Tuple[str, Any]]], Any]]=None) -> None:
        self.original_object_hook = object_hook
        super().__init__(object_hook=self.custom_object_hook, parse_float=parse_float, parse_int=parse_int, parse_constant=parse_constant, strict=strict, object_pairs_hook=object_pairs_hook)

    def unmake_serializable(self, obj: Any) -> Any:
        """
        Inverse function of :meth:`FilesystemJSONEncoder.make_serializable`.
        """
        pass