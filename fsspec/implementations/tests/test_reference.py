import json
import os
import pytest
import fsspec
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.reference import LazyReferenceMapper, ReferenceFileSystem, ReferenceNotReachable
from fsspec.tests.conftest import data, realfile, reset_files, server, win
jdata = '{\n    "metadata": {\n        ".zattrs": {\n            "Conventions": "UGRID-0.9.0"\n        },\n        ".zgroup": {\n            "zarr_format": 2\n        },\n        "adcirc_mesh/.zarray": {\n            "chunks": [\n                1\n            ],\n            "dtype": "<i4",\n            "shape": [\n                1\n            ],\n            "zarr_format": 2\n        },\n        "adcirc_mesh/.zattrs": {\n            "_ARRAY_DIMENSIONS": [\n                "mesh"\n            ],\n            "cf_role": "mesh_topology"\n        },\n        "adcirc_mesh/.zchunkstore": {\n            "adcirc_mesh/0": {\n                "offset": 8928,\n                "size": 4\n            },\n            "source": {\n                "array_name": "/adcirc_mesh",\n                "uri": "https://url"\n            }\n        }\n    },\n    "zarr_consolidated_format": 1\n}\n'