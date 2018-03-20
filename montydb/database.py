
from bson.py3compat import string_type

from . import errors
from .collection import MontyCollection
from .engine import DatabaseEngine
from .base import BaseObject


class MontyDatabase(BaseObject):

    def __init__(self, client, name, codec_options=None, write_concern=None):
        """
        """
        super(MontyDatabase, self).__init__(
            codec_options or client.codec_options,
            write_concern or client.write_concern)

        self._engine = DatabaseEngine(client._engine, name)
        self._client = client
        self._name = name

        client._engine.databases[name] = self

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self._client == other.client and
                    self._name == other.name)
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "MontyDatabase({!r}, {!r})".format(self._client, self._name)

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(
                "MontyDatabase has no attribute {0!r}. To access the {0}"
                " collection, use database[{0!r}].".format(name))
        return self.get_collection(name)

    def __getitem__(self, key):
        return self.get_collection(key)

    @property
    def name(self):
        """
        The name of this Database.
        """
        return self._name

    @property
    def client(self):
        """
        """
        return self._client

    def collection_names(self):
        return self._engine.collection_list()

    def create_collection(self, name):
        """
        Create a collection, before any insertion,
        raise error if exists.
        """
        if self._engine.collection_exists(name):
            error_msg = "collection {} already exists".format(
                name.encode("utf-8"))
            raise errors.CollectionInvalid(error_msg)
        else:
            collection = self.get_collection(name)
            self._engine.create_collection(name)
            return collection

    def drop_collection(self, name_or_collection):
        """
        """
        name = name_or_collection
        if isinstance(name_or_collection, MontyCollection):
            name = name_or_collection.name
        elif not isinstance(name_or_collection, string_type):
            raise TypeError("name_or_collection must be an instance of "
                            "basestring")
        self._engine.drop_collection(name)

    def get_collection(self, name):
        """
        Get a collection.
        """
        # verify collection name
        is_invaild = False
        for invaild_char in ('$', '\0', '\x00'):
            if invaild_char in name:
                is_invaild = True
        if name.startswith('system.'):
            is_invaild = True

        if is_invaild or not name:
            raise errors.OperationFailure("Invaild collection name.")
        else:
            return MontyCollection(self, name)
