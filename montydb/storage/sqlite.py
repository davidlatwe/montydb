
import os
import shutil
import sqlite3
import contextlib

from bson import BSON
from bson.py3compat import _unicode

from ..base import WriteConcern
from .base import (
    StorageConfig,
    AbstractStorage,
    AbstractDatabase,
    AbstractCollection,
    AbstractCursor,
)


"""
(NOTE) SQLite3 pragmas DEFAULT value:

* Applies to the database
    - journal_mode=delete

* Applies to the connection
    - busy_timeout=5000
    - foreign_keys=OFF
    - automatic_index=ON
"""


SQLITE_CONFIG = """
storage:
  engine: SQLiteStorage
  config: SQLiteConfig
  module: {}
connection:
  journal_mode: WAL
write_concern:
  # These will be picked up by wconcern
  synchronous: 1
  automatic_index: OFF
  busy_timeout: 5000
""".format(__name__)


SQLITE_CONFIG_SCHEMA = """
type: object
required:
  - connection
  - write_concern
properties:
  connection:
    type: object
    properties:
      journal_mode:
        type: string
        enum: [DELETE, TRUNCATE, PERSIST, MEMORY, WAL, "OFF"]
  write_concern:
    type: object
    properties:
      synchronous:
        oneOf:
          - type: string
            enum: ["OFF", NORMAL, FULL, EXTRA, "0", "1", "2", "3"]
          - type: integer
            enum: [0, 1, 2, 3]
      automatic_index:
        oneOf:
          - type: boolean
          - type: string
            enum: ["ON", "OFF"]
      busy_timeout:
        type: integer
"""


class SQLiteConfig(StorageConfig):
    """SQLite storage configuration settings

    Default configuration and schema of SQLite storage
    """
    config = SQLITE_CONFIG
    schema = SQLITE_CONFIG_SCHEMA


SQLITE_DB_EXT = ".collection"
SQLITE_RECORD_TABLE = "documents"


"""SQL"""

CREATE_TABLE = """
    CREATE TABLE [{}](
        k text NOT NULL,
        v blob NOT NULL,
        PRIMARY KEY(k)
    );
"""

INSERT_RECORD = """
    INSERT OR REPLACE INTO [{}](k, v) VALUES (?, ?)
"""
# (TODO) REPLACE will messed up the documents' nature order

SELECT_ALL_RECORD = """
    SELECT v FROM [{}]
"""

SELECT_LIMIT_RECORD = """
    SELECT v FROM [{0}] LIMIT {1}
"""


class SQLiteKVEngine(object):

    def __init__(self, db_pragmas):
        self.__db_pragmas = db_pragmas
        self.__conn = None

    @property
    def db_pragmas(self):
        return self._assemble_pragmas(self.__db_pragmas)

    def _connect(self, db_file, wconcern=None):
        self.__conn = sqlite3.connect(db_file)
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


class SQLiteWriteConcern(WriteConcern):
    """
    """

    def __init__(self,
                 wtimeout=None,
                 synchronous=None,
                 automatic_index=None):

        super(SQLiteWriteConcern, self).__init__(wtimeout)

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
        self._conn = SQLiteKVEngine(self._config.connection)

    def _db_path(self, db_name):
        """
        Get Monty database dir path.
        """
        return os.path.join(self._repository, db_name)

    def wconcern_parser(self, client_kwargs):
        _client_btimeout = client_kwargs.get("busy_timeout")
        # Default from config
        wcon_pragmas = self._config.write_concern
        wtimeout = client_kwargs.get(
            "wtimeout",
            _client_btimeout or wcon_pragmas.get("busy_timeout"))
        synchronous = client_kwargs.get(
            "synchronous",
            wcon_pragmas.get("synchronous"))
        automatic_index = client_kwargs.get(
            "automatic_index",
            wcon_pragmas.get("automatic_index"))

        return SQLiteWriteConcern(wtimeout,
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
            name for name in os.listdir(_unicode(self._repository))
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
                for name in os.listdir(_unicode(self._db_path))]


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

    def _encode_doc(self, doc):
        # Preserve BSON types
        encoded = BSON.encode(doc, False, self.coptions)
        return sqlite3.Binary(encoded)

    @property
    def _conn(self):
        return self._database._conn

    @_ensure_table
    def write_one(self, doc):
        """
        """
        self._conn.write_one(
            self._col_path,
            (str(doc["_id"]), self._encode_doc(doc),),
            self.wconcern
        )

        return doc["_id"]

    @_ensure_table
    def write_many(self, docs, ordered=True):
        """
        """
        self._conn.write_many(
            self._col_path,
            [(str(doc["_id"]), self._encode_doc(doc)) for doc in docs],
            self.wconcern
        )

        return [doc["_id"] for doc in docs]


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

    def _decode_doc(self, doc):
        # Decode BSON types
        return BSON(doc[0]).decode(self._collection.coptions)

    def query(self, max_scan):
        docs = self._conn.read_all(self._col_path, max_scan)
        return [self._decode_doc(doc) for doc in docs]


SQLiteCollection.contractor_cls = SQLiteCursor
