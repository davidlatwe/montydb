
import os

from abc import ABCMeta, abstractmethod
from bson import BSON
from bson.py3compat import PY3

from ..engine.helpers import with_metaclass

if PY3:
    from configparser import ConfigParser
else:
    from ConfigParser import ConfigParser


class StorageError(Exception):
    """Base class for all storage exceptions."""


class StorageDuplicateKeyError(StorageError):
    """Raise when an insert or update fails due to a duplicate key error."""


class AbstractStorage(with_metaclass(ABCMeta, object)):
    """
    """

    config_fname = "monty.storage.cfg"

    def __init__(self, repository, storage_config):
        self.is_opened = True
        self._repository = repository
        self._config = storage_config

    def __repr__(self):
        return "MontyStorage(engine: {!r})".format(self.__class__.__name__)

    def __getattribute__(self, attr):
        def obj_attr(attr_):
            return object.__getattribute__(self, attr_)

        if not (attr == "is_opened" or obj_attr("is_opened")):
            # Run re-open if not checking open status nor is opened.
            obj_attr("_re_open")()

        return obj_attr(attr)

    def __getattr__(self, attr):
        def delegate(subject, *args, **kwargs):
            delegator = self
            for inst in subject._components:
                delegator = delegator.contractor_cls(delegator, inst)
            return getattr(delegator, attr)(*args, **kwargs)
        return delegate

    @classmethod
    def nice_name(cls):
        return cls.__name__

    @classmethod
    def launch(cls, repository):
        """Load config from repository and return a storage instance
        """
        config_file = os.path.join(repository, cls.config_fname)
        config = dict()

        # Load config from repo
        if os.path.isfile(config_file):
            parser = ConfigParser()
            parser.read([config_file])

            section = cls.nice_name()
            if parser.has_section(section):
                config = {k: v for k, v in parser.items(section)}

        # Pass to cls.config
        storage_config = cls.config(**config)

        # Return an instance
        return cls(repository, storage_config)

    def _re_open(self):
        """Auto re-open"""
        self.is_opened = True

    def close(self):
        """Could do some clean up"""
        self.is_opened = False

    def wconcern_parser(self, **client_kwargs):
        """
        Parsing storage specific write concern

        Optional, use Monty WriteConcern by default.
        Recive MontyClient kwargs, should parse kwargs and return a instance
        of `montydb.base.WriteConcern` class.
        """
        pass

    @property
    def repository(self):
        return self._repository

    @property
    def contractor_cls(self):
        raise NotImplementedError("")

    @classmethod
    @abstractmethod
    def config(cls, **storage_kwargs):
        # Do not put write concern in config
        return NotImplemented

    @abstractmethod
    def database_create(self):
        return NotImplemented

    @abstractmethod
    def database_drop(self):
        return NotImplemented

    @abstractmethod
    def database_list(self):
        return NotImplemented


class AbstractDatabase(with_metaclass(ABCMeta, object)):

    def __init__(self, storage, subject):
        self._name = subject._name
        self._storage = storage

    @property
    def contractor_cls(self):
        raise NotImplementedError("")

    @abstractmethod
    def collection_exists(self):
        return NotImplemented

    @abstractmethod
    def collection_create(self):
        return NotImplemented

    @abstractmethod
    def collection_drop(self):
        return NotImplemented

    @abstractmethod
    def collection_list(self):
        return NotImplemented


class AbstractCollection(with_metaclass(ABCMeta, object)):

    def __init__(self, database, subject):
        self._name = subject._name
        self._database = database
        self.wconcern = subject.write_concern
        self.coptions = subject.codec_options

    def _encode_doc(self, doc):
        return BSON.encode(doc, False, self.coptions)

    @property
    def contractor_cls(self):
        raise NotImplementedError("")

    @abstractmethod
    def write_one(self):
        return NotImplemented

    @abstractmethod
    def write_many(self):
        return NotImplemented

    @abstractmethod
    def update_one(self):
        return NotImplemented

    @abstractmethod
    def update_many(self):
        return NotImplemented


class AbstractCursor(with_metaclass(ABCMeta, object)):

    def __init__(self, collection, subject):
        self._collection = collection

    def _decode_doc(self, doc):
        """
        """
        return BSON(doc).decode(self._collection.coptions)

    @abstractmethod
    def query(self):
        return NotImplemented
