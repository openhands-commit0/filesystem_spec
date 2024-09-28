import base64
import urllib
import requests
import requests.exceptions
from requests.adapters import HTTPAdapter, Retry
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile

class DatabricksException(Exception):
    """
    Helper class for exceptions raised in this module.
    """

    def __init__(self, error_code, message):
        """Create a new DatabricksException"""
        super().__init__(message)
        self.error_code = error_code
        self.message = message

class DatabricksFileSystem(AbstractFileSystem):
    """
    Get access to the Databricks filesystem implementation over HTTP.
    Can be used inside and outside of a databricks cluster.
    """

    def __init__(self, instance, token, **kwargs):
        """
        Create a new DatabricksFileSystem.

        Parameters
        ----------
        instance: str
            The instance URL of the databricks cluster.
            For example for an Azure databricks cluster, this
            has the form adb-<some-number>.<two digits>.azuredatabricks.net.
        token: str
            Your personal token. Find out more
            here: https://docs.databricks.com/dev-tools/api/latest/authentication.html
        """
        self.instance = instance
        self.token = token
        self.session = requests.Session()
        self.retries = Retry(total=10, backoff_factor=0.05, status_forcelist=[408, 429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=self.retries))
        self.session.headers.update({'Authorization': f'Bearer {self.token}'})
        super().__init__(**kwargs)

    def ls(self, path, detail=True, **kwargs):
        """
        List the contents of the given path.

        Parameters
        ----------
        path: str
            Absolute path
        detail: bool
            Return not only the list of filenames,
            but also additional information on file sizes
            and types.
        """
        pass

    def makedirs(self, path, exist_ok=True):
        """
        Create a given absolute path and all of its parents.

        Parameters
        ----------
        path: str
            Absolute path to create
        exist_ok: bool
            If false, checks if the folder
            exists before creating it (and raises an
            Exception if this is the case)
        """
        pass

    def mkdir(self, path, create_parents=True, **kwargs):
        """
        Create a given absolute path and all of its parents.

        Parameters
        ----------
        path: str
            Absolute path to create
        create_parents: bool
            Whether to create all parents or not.
            "False" is not implemented so far.
        """
        pass

    def rm(self, path, recursive=False, **kwargs):
        """
        Remove the file or folder at the given absolute path.

        Parameters
        ----------
        path: str
            Absolute path what to remove
        recursive: bool
            Recursively delete all files in a folder.
        """
        pass

    def mv(self, source_path, destination_path, recursive=False, maxdepth=None, **kwargs):
        """
        Move a source to a destination path.

        A note from the original [databricks API manual]
        (https://docs.databricks.com/dev-tools/api/latest/dbfs.html#move).

        When moving a large number of files the API call will time out after
        approximately 60s, potentially resulting in partially moved data.
        Therefore, for operations that move more than 10k files, we strongly
        discourage using the DBFS REST API.

        Parameters
        ----------
        source_path: str
            From where to move (absolute path)
        destination_path: str
            To where to move (absolute path)
        recursive: bool
            Not implemented to far.
        maxdepth:
            Not implemented to far.
        """
        pass

    def _open(self, path, mode='rb', block_size='default', **kwargs):
        """
        Overwrite the base class method to make sure to create a DBFile.
        All arguments are copied from the base method.

        Only the default blocksize is allowed.
        """
        pass

    def _send_to_api(self, method, endpoint, json):
        """
        Send the given json to the DBFS API
        using a get or post request (specified by the argument `method`).

        Parameters
        ----------
        method: str
            Which http method to use for communication; "get" or "post".
        endpoint: str
            Where to send the request to (last part of the API URL)
        json: dict
            Dictionary of information to send
        """
        pass

    def _create_handle(self, path, overwrite=True):
        """
        Internal function to create a handle, which can be used to
        write blocks of a file to DBFS.
        A handle has a unique identifier which needs to be passed
        whenever written during this transaction.
        The handle is active for 10 minutes - after that a new
        write transaction needs to be created.
        Make sure to close the handle after you are finished.

        Parameters
        ----------
        path: str
            Absolute path for this file.
        overwrite: bool
            If a file already exist at this location, either overwrite
            it or raise an exception.
        """
        pass

    def _close_handle(self, handle):
        """
        Close a handle, which was opened by :func:`_create_handle`.

        Parameters
        ----------
        handle: str
            Which handle to close.
        """
        pass

    def _add_data(self, handle, data):
        """
        Upload data to an already opened file handle
        (opened by :func:`_create_handle`).
        The maximal allowed data size is 1MB after
        conversion to base64.
        Remember to close the handle when you are finished.

        Parameters
        ----------
        handle: str
            Which handle to upload data to.
        data: bytes
            Block of data to add to the handle.
        """
        pass

    def _get_data(self, path, start, end):
        """
        Download data in bytes from a given absolute path in a block
        from [start, start+length].
        The maximum number of allowed bytes to read is 1MB.

        Parameters
        ----------
        path: str
            Absolute path to download data from
        start: int
            Start position of the block
        end: int
            End position of the block
        """
        pass

class DatabricksFile(AbstractBufferedFile):
    """
    Helper class for files referenced in the DatabricksFileSystem.
    """
    DEFAULT_BLOCK_SIZE = 1 * 2 ** 20

    def __init__(self, fs, path, mode='rb', block_size='default', autocommit=True, cache_type='readahead', cache_options=None, **kwargs):
        """
        Create a new instance of the DatabricksFile.

        The blocksize needs to be the default one.
        """
        if block_size is None or block_size == 'default':
            block_size = self.DEFAULT_BLOCK_SIZE
        assert block_size == self.DEFAULT_BLOCK_SIZE, f'Only the default block size is allowed, not {block_size}'
        super().__init__(fs, path, mode=mode, block_size=block_size, autocommit=autocommit, cache_type=cache_type, cache_options=cache_options or {}, **kwargs)

    def _initiate_upload(self):
        """Internal function to start a file upload"""
        pass

    def _upload_chunk(self, final=False):
        """Internal function to add a chunk of data to a started upload"""
        pass

    def _fetch_range(self, start, end):
        """Internal function to download a block of data"""
        pass

    def _to_sized_blocks(self, length, start=0):
        """Helper function to split a range from 0 to total_length into bloksizes"""
        pass