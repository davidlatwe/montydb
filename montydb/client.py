import platform
import shutil
import sys
import os

from bson.py3compat import string_type

from . import errors
from .base import BaseObject, ClientOptions
from .database import MontyDatabase


FS_ENC = sys.getfilesystemencoding()


class MontyClient(BaseObject):

    def __init__(self, repository=None, document_class=dict, **kwargs):
        kwargs["document_class"] = document_class
        self.__options = ClientOptions(kwargs)
        super(MontyClient, self).__init__(self.__options.codec_options,
                                          self.__options.write_concern)

        if repository is None:
            repository = os.getcwd()
        if not os.path.isdir(repository):
            os.makedirs(repository)

        self.__storage = None

        self.__repository = repository
        self.__opened = True

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
                "sqlite_jmode={!r}".format(
                    self.__options._options["sqlite_jmode"]
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
        if self.__opened:
            self.close()

    def __database_path(self, database_name):
        return os.path.join(self.__repository, database_name)

    @property
    def address(self):
        return self.__repository

    def close(self):
        self.__opened = False
        self.__storage.close()

    def database_names(self):
        """
        Return a list of database names.
        """
        return [
            db_dir.decode(FS_ENC) for db_dir in os.listdir(self.__repository)
            if os.path.isdir(self.__database_path(db_dir))
        ]

    def drop_database(self, name_or_database):
        """
        Remove database.
        # Could raise OSError: Device or resource busy
        if db file is locked by other connection...
        """
        name = name_or_database
        if isinstance(name_or_database, MontyDatabase):
            name = name_or_database.name
        elif not isinstance(name_or_database, string_type):
            raise TypeError("name_or_database must be an instance of "
                            "basestring or a Database")

        db_dir = self.__database_path(name)
        if os.path.isdir(db_dir):
            shutil.rmtree(db_dir)

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
