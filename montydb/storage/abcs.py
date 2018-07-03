from abc import ABCMeta, abstractmethod
from bson import BSON, SON

from ..engine.helpers import with_metaclass


class StorageConfig(object):
    """Base class of storage config"""
    config = NotImplemented
    schema = NotImplemented


class AbstractStorage(with_metaclass(ABCMeta, object)):
    """
    """

    def __init__(self, repository, storage_config):
        self.is_opened = True
        self._repository = repository
        self._config = storage_config

    def __repr__(self):
        return "MontyStorage(engine={!r})".format(self.__class__.__name__)

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

    def _re_open(self):
        """Auto re-open"""
        self.is_opened = True
        self._config.reload(repository=self._repository)

    def close(self):
        """Could do some clean up"""
        self.is_opened = False

    def wconcern_parser(self, client_kwargs):
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
        Document field order matters

        In order to match sub-document like MongoDB, decode to SON for internal
        usage.
        """
        coptions = self._collection.coptions.with_options(document_class=SON)
        return BSON(doc).decode(coptions)

    @abstractmethod
    def query(self):
        return NotImplemented
