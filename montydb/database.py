from . import errors
from .base import BaseObject
from .collection import MontyCollection
from .types import encode_, string_types


INVALID_CHARS = ("$", "\0", "\x00")


class MontyDatabase(BaseObject):
    def __init__(self, client, name, codec_options=None, write_concern=None):
        super(MontyDatabase, self).__init__(
            codec_options or client.codec_options, write_concern or client.write_concern
        )

        self._client = client
        self._name = name
        self._components = (self,)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._client == other.client and self._name == other.name

        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "MontyDatabase({!r}, {!r})".format(self._client, self._name)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(
                "MontyDatabase has no attribute {0!r}. To access the {0}"
                " collection, use database[{0!r}].".format(name)
            )
        return self.get_collection(name)

    def __getitem__(self, key):
        return self.get_collection(key)

    @property
    def name(self):
        """
        Return database's name.
        """
        return self._name

    @property
    def client(self):
        return self._client

    def collection_names(self):
        return self.client._storage.collection_list(self)

    list_collection_names = collection_names

    def create_collection(self, name, codec_options=None, write_concern=None, **kwargs):
        """
        Create a collection or raise an error if it already exists.
        """
        if self.client._storage.collection_exists(self, name):
            error_msg = "collection {} already exists".format(encode_(name))

            raise errors.CollectionInvalid(error_msg)

        collection = self.get_collection(name, codec_options, write_concern, **kwargs)
        self.client._storage.collection_create(self, name)

        return collection

    def drop_collection(self, name_or_collection):
        if isinstance(name_or_collection, MontyCollection):
            name = name_or_collection.name
        elif not isinstance(name_or_collection, string_types):
            raise TypeError("name_or_collection must be an instance of basestring")
        else:
            name = name_or_collection

        self.client._storage.collection_drop(self, name)

    def get_collection(self, name, codec_options=None, write_concern=None, **kwargs):
        """
        Return a collection if valid, otherwise throw an error.
        """
        if not name:
            raise errors.OperationFailure("Collection name may not be blank")

        for c in INVALID_CHARS:
            if c in name:
                raise errors.OperationFailure(
                    "Collection name contains special characters: {}".format(name)
                )

        if name.startswith("system."):
            raise errors.OperationFailure(
                "Invalid prefix in collection name: {}".format(name)
            )

        return MontyCollection(
            self, name, False, codec_options, write_concern, **kwargs
        )
