import platform

from . import errors
from .base import BaseObject, ClientOptions
from .database import MontyDatabase
from .engine import RepositoryEngine

from .vendor.six import string_types


class MontyClient(BaseObject):

    def __init__(self, repository=None, document_class=dict, **kwargs):
        """
        if repository is None, repository = os.getcwd()
        """
        kwargs["document_class"] = document_class
        self._options = ClientOptions(kwargs)
        super(MontyClient, self).__init__(self._options.codec_options,
                                          self._options.write_concern)

        self._engine = RepositoryEngine(repository)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.address == other.address
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("MontyClient({})".format(
            ", ".join([
                "repository={!r}".format(self.address),
                "document_class={}.{}".format(
                    self._options._options["document_class"].__module__,
                    self._options._options["document_class"].__name__),
                "sqlite_jmode={!r}".format(
                    self._options._options["sqlite_jmode"])
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
        if self._engine.opened:
            self.close()

    @property
    def address(self):
        """
        """
        return self._engine.database_repository

    def close(self):
        # do something
        self._engine.opened = False

    def database_names(self):
        """
        Return a list of database names.
        """
        return self._engine.database_list()

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
        self._engine.drop_database(name)

    def get_database(self, name):
        """
        Get a database, create one if not exists.
        """
        # verify database name
        if platform.system() == "Windows":
            is_invaild = set('/\. "$*<>:|?').intersection(set(name))
        else:
            is_invaild = set('/\. "$').intersection(set(name))

        if is_invaild or not name:
            raise errors.OperationFailure("Invaild database name.")
        else:
            return MontyDatabase(self, name)
