
import os
import shutil
import sqlite3
import contextlib

import bson
from bson.py3compat import _unicode

from .base import (
    AbstractStorage,
    AbstractDatabase,
    AbstractTable,
    AbstractCursor,
)


SQLITE_CONFIG = """
storage:
  engine: SQLiteStorage
  module: {}
  pragmas:
    - PRAGMA automatic_index=OFF
    - PRAGMA foreign_keys=ON
    - PRAGMA journal_mode=WAL
    - PRAGMA synchronous=1
""".format(__name__)


SQLITE_DB_EXT = ".collection"
SQLITE_DOC_TABLE = "documents"


class SQLiteConn(object):

    def __init__(self, config):
        self.config = config
        self.__conn = None

    def _connect(self, db_path):
        self.__conn = sqlite3.connect(db_path)
        # Assemble all PRAGMA and execute as script
        self.__conn.executescript(";".join(self.config.storage.pragmas))
        self.__conn.text_factory = str
        return contextlib.closing(self.__conn)

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


class SQLiteStorage(AbstractStorage):
    """
    """

    def __init__(self, repository, storage_config):
        super(SQLiteStorage, self).__init__(repository, storage_config)

        self._conn = SQLiteConn(self._config)

        # initialization complete
        self.is_opened = True

    def _db_path(self, db_name):
        return os.path.join(self._repository, db_name)

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
        return os.path.join(self._db_path, col_name) + SQLITE_DB_EXT

    def _conn(self, col_name):
        return self._storage._conn._connect(self._col_path(col_name))

    def database_exists(self):
        return os.path.isdir(self._db_path)

    def collection_exists(self, col_name):
        return os.path.isfile(self._col_path(col_name))

    def collection_create(self, col_name):
        if not self.database_exists():
            self._storage.database_create(self._name)

        INIT_COLLECTION = u"""
            CREATE TABLE [{}](
                _id text NOT NULL,
                doc blob NOT NULL,
                PRIMARY KEY(_id)
            );
        """.format(SQLITE_DOC_TABLE)

        with self._conn(col_name) as conn:
            with conn:
                conn.execute(INIT_COLLECTION)

    def collection_drop(self, col_name):
        if self.collection_exists(col_name):
            os.remove(self._col_path(col_name))

    def collection_list(self):
        if not self.database_exists():
            return []
        return [os.path.splitext(name)[0]
                for name in os.listdir(_unicode(self._db_path))]


SQLiteStorage.db_cls = SQLiteDatabase


class SQLiteTable(AbstractTable):

    def __init__(self, name, database):
        super(SQLiteTable, self).__init__(name, database)
        self._col_path = database._col_path(self._name)

    def _ensure_table(func):
        def make_table(self, *args, **kwargs):
            if not self._database.collection_exists(self._name):
                self._database.collection_create(self._name)
            return func(self, *args, **kwargs)
        return make_table

    def _bdoc(self, doc):
        return sqlite3.Binary(bson.BSON.encode(doc))

    def _ddoc(self, bdoc):
        return bson.BSON.decode(bson.BSON(bdoc))

    def write_with_concern(self):
        return NotImplemented


SQLiteDatabase.table_cls = SQLiteTable


class SQLiteCursor(AbstractCursor):
    pass


SQLiteTable.cursor_cls = SQLiteCursor
