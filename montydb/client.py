import platform
import sys

from . import errors, _version
from .base import BaseObject, ClientOptions
from .configure import provide_storage, provide_repository, session_config
from .database import MontyDatabase
from .types import string_types


class MontyClient(BaseObject):

    def __init__(self,
                 repository=None,
                 document_class=dict,
                 tz_aware=None,
                 **kwargs):
        """Client for a MontyDB instance

        The `repository` argument can be a montydb URI. A montydb URI is simply
        a repository path that prefixed with montydb scheme.
        For example:
            `montydb:///foo/bar/db_repo` -> Point to a absolute dir path
            `montydb://db_repo` -> Point to a relative dir path
            `montydb://` -> Point to current working dir or pinned dir
            `montydb://:memory:` -> Use memory storage

        Args:
            repository (str): A dir path for on-disk storage or `:memory:` for
                memory storage only, or a montydb URI.
            document_class (cls, optional): default class to use for documents
                returned from queries on this client. Default class is `dict`.
            tz_aware (bool, optional): if `True`, `datetime.datetime` instances
                returned as values in document by this client will be timezone
                aware (otherwise they will be naive).
            **kwargs: Other optional keyword arguments will pass into storage
                engine as write concern arguments.

        """
        repository = provide_repository(repository)
        storage_cls = provide_storage(repository)
        storage_instance = storage_cls.launch(repository)

        self._storage = storage_instance
        wconcern = self._storage.wconcern_parser(**kwargs)

        options = kwargs
        options["document_class"] = document_class
        options["tz_aware"] = tz_aware or False
        self.__options = ClientOptions(options, wconcern)
        super(MontyClient, self).__init__(self.__options.codec_options,
                                          self.__options.write_concern)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.address == other.address
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("MontyClient({})".format(
            ", ".join([
                "repository={!r}".format(
                    self.address
                ),
                "document_class={}.{}".format(
                    self.__options._options["document_class"].__module__,
                    self.__options._options["document_class"].__name__
                ),
                "storage_engine={}".format(
                    self._storage
                ),
            ]))
        )

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(
                "MontyClient has no attribute {0!r}. To access the {0}"
                " database, use client[{0!r}].".format(name))
        return self.get_database(name)

    def __getitem__(self, key):
        return self.get_database(key)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._storage.is_open:
            self.close()

    @property
    def address(self):
        return self._storage.repository

    def close(self):
        self._storage.close()

    def database_names(self):
        """
        Return a list of database names.
        """
        return self._storage.database_list()

    list_database_names = database_names

    def drop_database(self, name_or_database):
        """
        Remove database.
        # Could raise OSError: Device or resource busy
        if db file is locked by other connection...
        """
        name = name_or_database
        if isinstance(name_or_database, MontyDatabase):
            name = name_or_database.name
        elif not isinstance(name_or_database, string_types):
            raise TypeError("name_or_database must be an instance of "
                            "basestring or a Database")

        self._storage.database_drop(name)

    def get_database(self, name):
        """
        Get a database, create one if not exists.
        """
        # verify database name
        if platform.system() == "Windows":
            is_invalid = set('/\\. "$*<>:|?').intersection(set(name))
        else:
            is_invalid = set('/\\. "$').intersection(set(name))

        if is_invalid or not name:
            raise errors.OperationFailure("Invalid database name.")
        else:
            return MontyDatabase(self, name)

    def server_info(self):
        mongo_version = session_config()["mongo_version"]
        return {
            "version": _version.__version__,
            "versionArray": list(_version.version_info),
            "mongoVersion": mongo_version,
            "mongoVersionArray": list(mongo_version.split(".")),
            "storageEngine": self._storage.nice_name(),
            "python": sys.version,
            "platform": platform.platform(),
            "machine": platform.machine(),
        }
