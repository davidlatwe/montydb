
import os
import shutil
import sqlite3
import contextlib

import bson
from bson.py3compat import _unicode

from ..base import WriteConcern
from .base import (
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
  module: {}
  pragmas:
    database:
      journal_mode: WAL
    connection:
      # These will be picked up by wconcern
      synchronous: 1
      automatic_index: OFF
""".format(__name__)

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
    INSERT INTO [{}](k, v) VALUES (?, ?)
"""


class SQLiteKVEngine(object):

    def __init__(self, db_pragmas):
        self.db_pragmas = self._assemble_pragmas(db_pragmas)
        self.__conn = None

    def _connect(self, db_file, wconcern=None):
        self.__conn = sqlite3.connect(db_file)
        self.__conn.text_factory = str

        if wconcern:
            wcon_doc = wconcern.document
            # wtimeout (milliseconds) -> busy_timeout (milliseconds)
            timeout = wcon_doc.get("wtimeout")
            if timeout:
                del wcon_doc["wtimeout"]
                wcon_doc["busy_timeout"] = timeout

            conn_pragmas = self._assemble_pragmas(wcon_doc)
            self.__conn.executescript(conn_pragmas)

        return contextlib.closing(self.__conn)

    def _assemble_pragmas(self, pragma_dict):
        return ";".join(["PRAGMA {0}={1}".format(k, v)
                         for k, v in pragma_dict.items()])

    def _read(self, sql, param):
        """Ignore table's existence and get a cursor.
        """
        try:
            return self.__conn.execute(sql, param)
        except sqlite3.OperationalError as e:
            if e.args[0].startswith("no such table"):
                return self.__conn.cursor()
            else:
                raise

    def create_table(self, db_file):
        with self._connect(db_file) as conn:
            with conn:
                # Assemble PRAGMAS and init db
                conn.executescript(self.db_pragmas)
                conn.execute(CREATE_TABLE.format(SQLITE_RECORD_TABLE))

    def write_one(self, db_file, params, wconcern=None):
        with self._connect(db_file, wconcern) as conn:
            with conn:
                conn.execute(INSERT_RECORD.format(SQLITE_RECORD_TABLE), params)


class SQLiteWriteConcern(WriteConcern):
    """
    """
    def __init__(self,
                 wtimeout=None,
                 synchronous=None,
                 automatic_index=None):

        super(SQLiteWriteConcern, self).__init__(wtimeout)

        if synchronous is not None:
            self.__document["synchronous"] = synchronous
        if automatic_index is not None:
            self.__document["automatic_index"] = automatic_index

    def __repr__(self):
        return ("SQLiteWriteConcern({})".format(
            ", ".join("%s=%s" % kvt for kvt in self.document.items()),))


class SQLiteStorage(AbstractStorage):
    """
    """

    def __init__(self, repository, storage_config):
        super(SQLiteStorage, self).__init__(repository, storage_config)

        self._conn = SQLiteKVEngine(self._config.storage.pragmas.database)

        # initialization complete
        self.is_opened = True

    def _db_path(self, db_name):
        """
        Get Monty database dir path.
        """
        return os.path.join(self._repository, db_name)

    def wconcern_parser(self, client_kwargs):
        wtimeout = client_kwargs.get("wtimeout")
        # Default from config
        conn_pragmas = self._config.storage.pragmas.connection
        synchronous = client_kwargs.get(
            "synchronous",
            conn_pragmas.get("synchronous"))
        automatic_index = client_kwargs.get(
            "automatic_index",
            conn_pragmas.get("automatic_index"))

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

    def __init__(self, name, storage):
        super(SQLiteDatabase, self).__init__(name, storage)
        self._db_path = storage._db_path(self._name)

    def _col_path(self, col_name):
        """
        Get SQLite database file path, which is Monty collection.
        """
        return os.path.join(self._db_path, col_name) + SQLITE_DB_EXT

    @property
    def _conn(self, col_name):
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


SQLiteStorage.db_cls = SQLiteDatabase


class SQLiteCollection(AbstractCollection):

    def __init__(self, name, database, write_concern, codec_options):
        super(SQLiteCollection, self).__init__(name,
                                               database,
                                               write_concern,
                                               codec_options)

        self._col_path = self._database._col_path(self._name)

    def _ensure_table(func):
        def make_table(self, *args, **kwargs):
            if not self._database.collection_exists(self._name):
                self._database.collection_create(self._name)
            return func(self, *args, **kwargs)
        return make_table

    def _encode_doc(self, doc):
        return sqlite3.Binary(bson.BSON.encode(doc))

    @property
    def _conn(self):
        return self._database._conn

    @_ensure_table
    def insert_one(self, doc, bypass_doc_val):
        """
        bypass_doc_val not used
        """
        if "_id" not in doc:
            doc["_id"] = bson.ObjectId()

        self._conn.write_one(
            self._col_path,
            (str(doc["_id"]), self._encode_doc(doc),),
            self.wconcern
        )

        return doc["_id"]

    @_ensure_table
    def insert_many(self, docs, ordered, bypass_doc_val):
        """
        ordered, bypass_doc_val not used
        """
        for doc in docs:
            if "_id" not in doc:
                doc["_id"] = bson.ObjectId()

        self._conn.write_many(
            self._col_path,
            [(str(doc["_id"]), self._encode_doc(doc)) for doc in docs],
            self.wconcern
        )

        return [doc["_id"] for doc in docs]

    def replace_one(self):
        return NotImplemented

    def update_one(self):
        return NotImplemented


SQLiteDatabase.col_cls = SQLiteCollection


class SQLiteCursor(AbstractCursor):

    def _decode_doc(self, doc):
        return bson.BSON(doc)

    def query(self):
        pass


SQLiteCollection.cursor_cls = SQLiteCursor
