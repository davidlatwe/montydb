
from bson import ObjectId, SON, BSON

from .base import (
    AbstractStorage,
    AbstractDatabase,
    AbstractCollection,
    AbstractCursor,
)


MEMORY_CONFIG = """
storage:
  engine: MemoryStorage
  module: {}
""".format(__name__)

MEMORY_REPOSITORY = ":memory:"


class MemoryStorage(AbstractStorage):
    """
    """

    def __init__(self, repository, storage_config):
        super(MemoryStorage, self).__init__(repository, storage_config)
        self._repo = SON()

    def database_create(self, db_name):
        self._repo[db_name] = SON()

    def database_drop(self, db_name):
        if db_name in self._repo:
            del self._repo[db_name]

    def database_list(self):
        return list(self._repo.keys())


class MemoryDatabase(AbstractDatabase):
    """
    """

    @property
    def _db(self):
        return self._storage._repo[self._name]

    def db_exists(self):
        return self._name in self._storage._repo

    def collection_exists(self, col_name):
        if self.db_exists():
            return col_name in self._db
        return False

    def collection_create(self, col_name):
        if not self.db_exists():
            self._storage.database_create(self._name)
        self._db[col_name] = SON()

    def collection_drop(self, col_name):
        if self.collection_exists(col_name):
            del self._db[col_name]

    def collection_list(self):
        if not self.db_exists():
            return []
        return list(self._db.keys())


MemoryStorage.db_cls = MemoryDatabase


class MemoryCollection(AbstractCollection):
    """
    """

    @property
    def _col(self):
        if not self._database.collection_exists(self._name):
            self._database.collection_create(self._name)
        return self._database._db[self._name]

    def _encode_doc(self, doc):
        return BSON.encode(doc, False, self.coptions)

    def insert_one(self, doc):
        if "_id" not in doc:
            doc["_id"] = ObjectId()

        self._col[str(doc["_id"])] = self._encode_doc(doc)

        return doc["_id"]

    def insert_many(self, docs, ordered):
        for doc in docs:
            if "_id" not in doc:
                doc["_id"] = ObjectId()

            self._col[str(doc["_id"])] = self._encode_doc(doc)

        return [doc["_id"] for doc in docs]

    def replace_one(self):
        return NotImplemented

    def update(self):
        return NotImplemented


MemoryDatabase.col_cls = MemoryCollection


class MemoryCursor(AbstractCursor):
    """
    """

    @property
    def _col(self):
        return self._collection._col

    def _decode_doc(self, doc):
        return BSON(doc).decode(self._collection.coptions)

    def query(self, max_scan):
        docs = [self._decode_doc(doc) for doc in self._col.values()]
        if not max_scan:
            return docs
        else:
            return docs[:max_scan]


MemoryCollection.cursor_cls = MemoryCursor
