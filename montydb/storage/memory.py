
from itertools import islice
from bson import SON

from .abcs import (
    AbstractStorage,
    AbstractDatabase,
    AbstractCollection,
    AbstractCursor,
)
from .errors import StorageDuplicateKeyError


class MemoryStorage(AbstractStorage):
    """
    """

    def __init__(self, repository, storage_config):
        super(MemoryStorage, self).__init__(repository, storage_config)
        self._repo = SON()

    @classmethod
    def config(cls, **storage_kwargs):
        return dict()

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


MemoryStorage.contractor_cls = MemoryDatabase


class MemoryCollection(AbstractCollection):
    """
    """

    @property
    def _col(self):
        if not self._col_exists():
            self._database.collection_create(self._name)
        return self._database._db[self._name]

    def _col_exists(self):
        return self._database.collection_exists(self._name)

    def _id_unique(self, id):
        if id in self._col:
            raise StorageDuplicateKeyError()

    def write_one(self, doc):
        id = doc["_id"]
        self._id_unique(str(id))
        self._col[str(id)] = self._encode_doc(doc)
        return id

    def write_many(self, docs, ordered=True):
        ids = list()
        for doc in docs:
            id = doc["_id"]
            self._id_unique(str(id))
            self._col[str(id)] = self._encode_doc(doc)
            ids.append(id)
        return ids

    def update_one(self, doc):
        self._col[str(doc["_id"])] = self._encode_doc(doc)

    def update_many(self, docs):
        for doc in docs:
            self._col[str(doc["_id"])] = self._encode_doc(doc)

    def delete_one(self, id):
        del self._col[str(id)]

    def delete_many(self, ids):
        for id in ids:
            del self._col[str(id)]


MemoryDatabase.contractor_cls = MemoryCollection


class MemoryCursor(AbstractCursor):
    """
    """

    @property
    def _col(self):
        if self._collection._col_exists():
            return self._collection._col
        return SON()

    def query(self, max_scan):
        docs = (self._decode_doc(doc) for doc in self._col.values())
        if not max_scan:
            return docs
        else:
            return islice(docs, max_scan)


MemoryCollection.contractor_cls = MemoryCursor
