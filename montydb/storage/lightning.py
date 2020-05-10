
import os
import lmdb
import shutil
from itertools import islice

from ..types import unicode_
from . import (
    AbstractStorage,
    AbstractDatabase,
    AbstractCollection,
    AbstractCursor,

    StorageDuplicateKeyError,
)

# Functional > Performence > Natural

LMDB_DB_EXT = ".mdb"


def _to_bytes(s):
    return s.encode()


class LMDBKVEngine(object):
    """Per collection"""

    dbname = _to_bytes("documents")

    def __init__(self, options=None):
        """
        """
        opt = options or dict()
        opt.update({
            "subdir": False,
            "max_dbs": 1,
        })
        self.opt = opt
        self._path = None

    def set_path(self, path):
        self._path = path

    def open(self):
        return lmdb.open(self._path, **self.opt)

    def iter_docs(self):
        # (TODO) Implement custom context __exit__ to handle error or
        #        StopIteration ?
        if not os.path.isfile(self._path):
            return []
        docs = list()
        with self.open() as env:
            db = env.open_db(self.dbname, reverse_key=False)
            with env.begin(db, write=False) as txn:
                cursor = txn.cursor()
                for k, encoded_doc in cursor.iternext(keys=True, values=True):
                    # print(k)
                    docs.append(encoded_doc)
        return docs

    def write(self, pairs, overwrite=False):
        if not os.path.isfile(self._path):
            return
        dup = False
        with self.open() as env:
            # (TODO) Use int number as key and implement this to all storage,
            #        as a default index.
            #        On deletion, remember the int number key in query and
            #        use it for deletion, not use document id.
            db = env.open_db(self.dbname, reverse_key=False)
            with env.begin(db, write=True) as txn:
                for doc_id, encoded_doc in pairs:
                    id = _to_bytes(str(doc_id))
                    # print("->", id)
                    if not txn.put(id, encoded_doc, overwrite=overwrite):
                        dup = True
                        break
        if dup:
            raise StorageDuplicateKeyError()

    def delete(self, doc_ids):
        if not os.path.isfile(self._path):
            return
        with self.open() as env:
            db = env.open_db(self.dbname, reverse_key=False)
            with env.begin(db, write=True) as txn:
                cursor = txn.cursor()
                for doc_id in doc_ids:
                    id = _to_bytes(str(doc_id))
                    if cursor.set_key(id):
                        cursor.delete()


class LMDBStorage(AbstractStorage):
    """
    """

    def __init__(self, repository, storage_config):
        super(LMDBStorage, self).__init__(repository, storage_config)
        self._conn = LMDBKVEngine(self._config)

    def _db_path(self, db_name):
        """
        Get Monty database dir path.
        """
        return os.path.join(self._repository, db_name)

    @classmethod
    def nice_name(cls):
        return "lightning"

    @classmethod
    def config(cls, map_size=10485760, **kwargs):
        """
        """
        return {
            "map_size": int(map_size),
        }

    def database_create(self, db_name):
        if not os.path.isdir(self._db_path(db_name)):
            os.makedirs(self._db_path(db_name))

    def database_drop(self, db_name):
        db_path = self._db_path(db_name)
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)

    def database_list(self):
        return [
            name for name in os.listdir(unicode_(self._repository))
            if os.path.isdir(self._db_path(name))
        ]


class LMDBDatabase(AbstractDatabase):
    """
    """

    def __init__(self, storage, subject):
        super(LMDBDatabase, self).__init__(storage, subject)
        self._db_path = storage._db_path(self._name)
        self._conn = storage._conn

    def _col_path(self, col_name):
        """
        Get database file path, which is Monty collection.
        """
        return os.path.join(self._db_path, col_name) + LMDB_DB_EXT

    def database_exists(self):
        return os.path.isdir(self._db_path)

    def collection_exists(self, col_name):
        return os.path.isfile(self._col_path(col_name))

    def collection_create(self, col_name):
        if not self.database_exists():
            self._storage.database_create(self._name)
        self._conn.set_path(self._col_path(col_name))
        self._conn.open()

    def collection_drop(self, col_name):
        if self.collection_exists(col_name):
            os.remove(self._col_path(col_name))

    def collection_list(self):
        if not self.database_exists():
            return []
        return [os.path.splitext(name)[0]
                for name in os.listdir(unicode_(self._db_path))
                if name.endswith(LMDB_DB_EXT)]


LMDBStorage.contractor_cls = LMDBDatabase


class LMDBCollection(AbstractCollection):
    """
    """

    def __init__(self, database, subject):
        super(LMDBCollection, self).__init__(database, subject)
        self._conn = database._conn
        self._conn.set_path(database._col_path(self._name))

    def _ensure_table(func):
        def make_table(self, *args, **kwargs):
            if not self._database.collection_exists(self._name):
                self._database.collection_create(self._name)
            return func(self, *args, **kwargs)
        return make_table

    @_ensure_table
    def write_one(self, doc, check_keys=True):
        id = doc["_id"]
        encoded = self._encode_doc(doc, check_keys)
        self._conn.write([(id, encoded)])

        return id

    @_ensure_table
    def write_many(self, docs, check_keys=True, ordered=True):
        ids = list()

        def produce_encoded_docs():
            for doc in docs:
                id = doc["_id"]
                yield id, self._encode_doc(doc, check_keys)
                ids.append(id)

        self._conn.write(produce_encoded_docs())

        return ids

    def update_one(self, doc):
        id = doc["_id"]
        encoded = self._encode_doc(doc)
        self._conn.write([(id, encoded)], overwrite=True)

    def update_many(self, docs):
        def produce_encoded_docs():
            for doc in docs:
                yield doc["_id"], self._encode_doc(doc)

        self._conn.write(produce_encoded_docs(), overwrite=True)

    def delete_one(self, id):
        self._conn.delete([id])

    def delete_many(self, ids):
        self._conn.delete(ids)


LMDBDatabase.contractor_cls = LMDBCollection


class LMDBCursor(AbstractCursor):
    """
    """

    def __init__(self, collection, subject):
        super(LMDBCursor, self).__init__(collection, subject)
        self._conn = self._collection._conn

    def query(self, max_scan):
        docs = (self._decode_doc(doc) for doc in self._conn.iter_docs())
        if not max_scan:
            return docs
        else:
            return islice(docs, max_scan)


LMDBCollection.contractor_cls = LMDBCursor
