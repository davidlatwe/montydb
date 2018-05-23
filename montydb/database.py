
from bson.py3compat import string_type

from . import errors
from .collection import MontyCollection
from .base import BaseObject
from .engine.helpers import encode_


class MontyDatabase(BaseObject):

    def __init__(self, client, name, codec_options=None, write_concern=None):
        """
        """
        super(MontyDatabase, self).__init__(
            codec_options or client.codec_options,
            write_concern or client.write_concern)

        self._client = client
        self._name = name

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
        return self.client._storage.collection_list(self._name)

    def create_collection(self,
                          name,
                          codec_options=None,
                          write_concern=None,
                          **kwargs):
        """
        Create a collection, before any insertion,
        raise error if exists.
        """
        if self.client._storage.collection_exists(self._name, name):
            error_msg = "collection {} already exists".format(encode_(name))
            raise errors.CollectionInvalid(error_msg)
        else:
            collection = self.get_collection(name,
                                             codec_options,
                                             write_concern,
                                             **kwargs)
            self.client._storage.collection_create(self._name, name)
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
        self.client._storage.collection_drop(self._name, name)

    def get_collection(self,
                       name,
                       codec_options=None,
                       write_concern=None,
                       **kwargs):
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
            return MontyCollection(self,
                                   name,
                                   False,
                                   codec_options,
                                   write_concern,
                                   **kwargs)
