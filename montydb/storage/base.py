from abc import ABCMeta, abstractmethod

from ..engine.helpers import with_metaclass


class AbstractStorage(with_metaclass(ABCMeta, object)):
    """
    """

    def __init__(self, repository, storage_config):
        self.is_opened = False
        self._repository = repository
        self._config = storage_config
        self._config.lock()

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
        return self._delegate(attr)

    def _delegate(self, attr):
        def to_db_level(db_name, *args, **kwargs):
            db = self.db_cls(db_name, self)
            return db.__getattribute__(attr)(*args, **kwargs)
        return to_db_level

    def _re_open(self):
        """Auto re-open"""
        self.is_opened = True

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
    def db_cls(self):
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

    def __init__(self, name, storage):
        self._name = name
        self._storage = storage

    def __getattr__(self, attr):
        return self._delegate(attr)

    def _delegate(self, attr):
        def to_table_level(collection_name, *args, **kwargs):
            table = self.col_cls(collection_name, self)
            return table.__getattribute__(attr)(*args, **kwargs)
        return to_table_level

    @property
    def col_cls(self):
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

    def __init__(self, name, database, write_concern, codec_options):
        self._name = name
        self._database = database
        self._write_concern = write_concern
        self._codec_options = codec_options

    def __getattr__(self, attr):
        return self._delegate(attr)

    def _delegate(self, attr):
        def to_cursor_level(*args, **kwargs):
            cursor = self.cursor_cls(self)
            return cursor.__getattribute__(attr)(*args, **kwargs)
        return to_cursor_level

    @property
    def cursor_cls(self):
        raise NotImplementedError("")

    @abstractmethod
    def insert_one(self):
        return NotImplemented

    @abstractmethod
    def insert_many(self):
        return NotImplemented

    @abstractmethod
    def replace_one(self):
        return NotImplemented

    @abstractmethod
    def update_one(self):
        return NotImplemented


class AbstractCursor(with_metaclass(ABCMeta, object)):

    def __init__(self, collection):
        self._collection = collection
        self._codec_options = collection._codec_options

    @abstractmethod
    def query(self):
        return NotImplemented
