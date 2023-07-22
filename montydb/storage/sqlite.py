
import os
import shutil
import sqlite3
import contextlib
from collections import OrderedDict

from ..base import WriteConcern
from ..types import unicode_, bson
from . import (
    AbstractStorage,
    AbstractDatabase,
    AbstractCollection,
    AbstractCursor,

    StorageDuplicateKeyError,
)


sqlite_324 = sqlite3.sqlite_version_info >= (3, 24, 0)


"""
(NOTE) SQLite3 pragmas DEFAULT value:

* Applies to the database
    - journal_mode=delete

* Applies to the connection
    - busy_timeout=5000
    - foreign_keys=OFF
    - automatic_index=ON
"""


SQLITE_DB_EXT = ".collection"
SQLITE_RECORD_TABLE = "documents"


"""SQL"""

CREATE_TABLE = """
    CREATE TABLE [{}](
        k text NOT NULL,
        v text NOT NULL,
        PRIMARY KEY(k)
    );
"""

INSERT_RECORD = """
    INSERT INTO [{}](k, v) VALUES (?, ?);
"""

UPSERT_RECORD = """
    INSERT INTO [{}] (v, k) VALUES(?, ?)
        ON CONFLICT(k)
        DO UPDATE SET v=excluded.v;
"""  # need sqlite_version >= 3.24.0

UPDATE_RECORD = """
    UPDATE [{}] SET v = (?) WHERE k = (?);
"""

INSORE_RECORD = """
    INSERT OR IGNORE INTO [{}](v, k) VALUES (?, ?);
"""  # for sqlite_version < 3.24, UPDATE + INSERT_IGNORE = UPSERT SCRIPT

DELETE_RECORD = """
    DELETE FROM [{}] WHERE k = (?);
"""

SELECT_ALL_RECORD = """
    SELECT v FROM [{}];
"""

SELECT_LIMIT_RECORD = """
    SELECT v FROM [{0}] LIMIT {1};
"""

SELECT_ALL_KEYS = """
    SELECT k FROM [{}];
"""


class SQLiteKVEngine(object):

    def __init__(self, config):
        self.__conn = None

        self.__db_pragmas = {
            key: config[key]
            for key in [
                "journal_mode",
            ]
            if key in config
        }

        self.__conn_kwargs = {
            key: cast(config[key])
            for key, cast in [
                ("check_same_thread", bool)
            ]
            if key in config
        }

    @property
    def db_pragmas(self):
        return self._assemble_pragmas(self.__db_pragmas)

    def _connect(self, db_file, wconcern=None):
        self.__conn = sqlite3.connect(db_file, **self.__conn_kwargs)
        self.__conn.text_factory = str

        wcon_pragmas = ""
        if wconcern:
            wcon_doc = wconcern.document
            # wtimeout (milliseconds) -> busy_timeout (milliseconds)
            timeout = wcon_doc.get("wtimeout")
            if timeout:
                del wcon_doc["wtimeout"]
                wcon_doc["busy_timeout"] = timeout

            wcon_pragmas = self._assemble_pragmas(wcon_doc)

        # update connection pragmas and write_concern pragmas
        self.__conn.executescript(self.db_pragmas + ";" + wcon_pragmas)

        return contextlib.closing(self.__conn)

    def _assemble_pragmas(self, pragma_dict):
        return ";".join(["PRAGMA {0}={1}".format(k, v)
                         for k, v in pragma_dict.items()])

    def create_table(self, db_file):
        with self._connect(db_file) as conn:
            with conn:
                conn.execute(CREATE_TABLE.format(SQLITE_RECORD_TABLE))

    def write_one(self, db_file, params, wconcern=None):
        with self._connect(db_file, wconcern) as conn:
            with conn:
                sql = INSERT_RECORD.format(SQLITE_RECORD_TABLE)
                conn.execute(sql, params)

    def write_many(self, db_file, seq_params, wconcern=None):
        with self._connect(db_file, wconcern) as conn:
            with conn:
                sql = INSERT_RECORD.format(SQLITE_RECORD_TABLE)
                conn.executemany(sql, seq_params)

    def update_one(self, db_file, params, wconcern=None):
        with self._connect(db_file, wconcern) as conn:
            with conn:
                sql = UPDATE_RECORD.format(SQLITE_RECORD_TABLE)
                conn.execute(sql, params)

    def update_many(self, db_file, seq_params, wconcern=None):
        with self._connect(db_file, wconcern) as conn:
            with conn:
                sql = UPDATE_RECORD.format(SQLITE_RECORD_TABLE)
                conn.executemany(sql, seq_params)

    def delete_one(self, db_file, params, wconcern=None):
        with self._connect(db_file, wconcern) as conn:
            with conn:
                sql = DELETE_RECORD.format(SQLITE_RECORD_TABLE)
                conn.execute(sql, params)

    def delete_many(self, db_file, seq_params, wconcern=None):
        with self._connect(db_file, wconcern) as conn:
            with conn:
                sql = DELETE_RECORD.format(SQLITE_RECORD_TABLE)
                conn.executemany(sql, seq_params)

    def read_all(self, db_file, limit):
        if not os.path.isfile(db_file):
            return []
        with self._connect(db_file) as conn:
            with conn:
                if limit:
                    sql = SELECT_LIMIT_RECORD.format(
                        SQLITE_RECORD_TABLE, limit)
                else:
                    sql = SELECT_ALL_RECORD.format(SQLITE_RECORD_TABLE)

                return conn.execute(sql).fetchall()

    def read_all_keys(self, db_file):
        if not os.path.isfile(db_file):
            return []
        with self._connect(db_file) as conn:
            with conn:
                sql = SELECT_ALL_KEYS.format(SQLITE_RECORD_TABLE)
                return conn.execute(sql).fetchall()


class SQLiteWriteConcern(WriteConcern):
    """

    Args:
        busy_timeout (int): Default 5000

        synchronous (int, str): Default "NORMAL"
            - type: integer
              enum: [0, 1, 2, 3]
            - type: string
              enum: ["OFF", NORMAL, FULL, EXTRA, "0", "1", "2", "3"]

        automatic_index (bool, str): Default False
            - type: boolean
            - type: string
              enum: ["ON", "OFF"]

    """

    def __init__(self,
                 busy_timeout=5000,
                 synchronous="NORMAL",
                 automatic_index=False):

        super(SQLiteWriteConcern, self).__init__(busy_timeout)

        if synchronous is not None:
            self._document["synchronous"] = synchronous
        if automatic_index is not None:
            self._document["automatic_index"] = automatic_index

    def __repr__(self):
        return ("SQLiteWriteConcern({})".format(
            ", ".join("%s=%s" % kvt for kvt in self.document.items()),))


class SQLiteStorage(AbstractStorage):
    """
    """

    def __init__(self, repository, storage_config):
        super(SQLiteStorage, self).__init__(repository, storage_config)
        self._conn = SQLiteKVEngine(self._config)

    def _db_path(self, db_name):
        """
        Get Monty database dir path.
        """
        return os.path.join(self._repository, db_name)

    @classmethod
    def nice_name(cls):
        return "sqlite"

    @classmethod
    def config(cls, journal_mode="WAL", check_same_thread=True, **kwargs):
        """

        Args:
            journal_mode (str): Default "WAL"
                type: string
                enum: [DELETE, TRUNCATE, PERSIST, MEMORY, WAL, "OFF"]
            check_same_thread (bool): Default True
                See `sqlite3.connect`

        """
        return {
            "journal_mode": journal_mode,
            "check_same_thread": check_same_thread,
        }

    def wconcern_parser(self,
                        wtimeout=None,
                        busy_timeout=None,
                        synchronous=None,
                        automatic_index=None,
                        **kwargs):
        return SQLiteWriteConcern(wtimeout or busy_timeout,
                                  synchronous,
                                  automatic_index)

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


class SQLiteDatabase(AbstractDatabase):

    def __init__(self, storage, subject):
        super(SQLiteDatabase, self).__init__(storage, subject)
        self._db_path = storage._db_path(self._name)

    def _col_path(self, col_name):
        """
        Get SQLite database file path, which is Monty collection.
        """
        return os.path.join(self._db_path, col_name) + SQLITE_DB_EXT

    @property
    def _conn(self):
        return self._storage._conn

    def database_exists(self):
        return os.path.isdir(self._db_path)

    def collection_exists(self, col_name):
        return os.path.isfile(self._col_path(col_name))

    def collection_create(self, col_name):
        if not self.database_exists():
            self._storage.database_create(self._name)
        self._conn.create_table(self._col_path(col_name))

    def collection_drop(self, col_name):
        if self.collection_exists(col_name):
            os.remove(self._col_path(col_name))

    def collection_list(self):
        if not self.database_exists():
            return []
        return [os.path.splitext(name)[0]
                for name in os.listdir(unicode_(self._db_path))]


SQLiteStorage.contractor_cls = SQLiteDatabase


class SQLiteCollection(AbstractCollection):

    def __init__(self, database, subject):
        super(SQLiteCollection, self).__init__(database, subject)

        self._col_path = self._database._col_path(self._name)

    def _ensure_table(func):
        def make_table(self, *args, **kwargs):
            if not self._database.collection_exists(self._name):
                self._database.collection_create(self._name)
            return func(self, *args, **kwargs)
        return make_table

    @property
    def _conn(self):
        return self._database._conn

    @_ensure_table
    def write_one(self, doc, check_keys=True):
        """
        """
        _id = doc["_id"]
        try:
            self._conn.write_one(
                self._col_path,
                (bson.id_encode(_id),
                 self._encode_doc(doc, check_keys),),
                self.wconcern
            )
        except sqlite3.IntegrityError:
            raise StorageDuplicateKeyError()

        return _id

    @_ensure_table
    def write_many(self, docs, check_keys=True, ordered=True):
        """
        """
        _docs = OrderedDict()
        ids = list()
        keys = self._conn.read_all_keys(self._col_path)
        has_duplicated_key = False
        for doc in docs:
            _id = doc["_id"]
            b_id = bson.id_encode(_id)
            if b_id in keys or b_id in _docs:
                has_duplicated_key = True
                break

            _docs[b_id] = self._encode_doc(doc, check_keys)
            ids.append(_id)

        self._conn.write_many(
            self._col_path,
            _docs.items(),
            self.wconcern
        )

        if has_duplicated_key:
            raise StorageDuplicateKeyError()

        return ids

    def update_one(self, doc):
        """
        """
        self._conn.update_one(
            self._col_path,
            (self._encode_doc(doc), bson.id_encode(doc["_id"])),
            self.wconcern
        )

    def update_many(self, docs):
        """
        """
        self._conn.update_many(
            self._col_path,
            [(self._encode_doc(doc), bson.id_encode(doc["_id"]))
             for doc in docs],
            self.wconcern
        )

    def delete_one(self, id):
        self._conn.delete_one(
            self._col_path,
            (bson.id_encode(id),),
            self.wconcern
        )

    def delete_many(self, ids):
        self._conn.delete_many(
            self._col_path,
            [(bson.id_encode(id),) for id in ids],
            self.wconcern
        )


SQLiteDatabase.contractor_cls = SQLiteCollection


class SQLiteCursor(AbstractCursor):

    def __init__(self, collection, subject):
        super(SQLiteCursor, self).__init__(collection, subject)

    @property
    def _conn(self):
        return self._collection._conn

    @property
    def _col_path(self):
        return self._collection._col_path

    def query(self, max_scan):
        docs = self._conn.read_all(self._col_path, max_scan)
        return (self._decode_doc(doc[0]) for doc in docs)


SQLiteCollection.contractor_cls = SQLiteCursor
