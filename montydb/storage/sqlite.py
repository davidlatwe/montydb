
import os
import shutil

from . import AbstractStorage
from ..engine.helpers import FS_ENCODE


SQLITE_CONN_OPTIONS = frozenset([
    "OFF",
    "MEMORY",
    "WAL",
    "TRUNCATE",
    "PERSIST",
    "DELETE",
    "EXTRA"
])


SQLITE_CONFIG = """
storage:
  engine: SQLiteStorage
  module: {}
  pragma:
    - automatic_index=OFF
    - foreign_keys=ON
    - journal_mode=WAL
    - synchronous=1
""".format(__name__)


class SQLiteStorage(AbstractStorage):
    """
    """

    def __init__(self, repository=None, storage_config=None):
        super(SQLiteStorage, self).__init__(repository, storage_config)

    def __repr__(self):
        return ("Storage(engine='SQLite')")

    def _db_path(self, db_name):
        return os.path.join(self._repository, db_name)

    def _parse_config(self, config):
        return NotImplemented

    def create_database(self, db_name):
        if not os.path.isdir(self._db_path(db_name)):
            os.makedirs(self._db_path(db_name))

    def has_database(self, db_name):
        return os.path.isdir(self._db_path(db_name))

    def drop_database(self, db_name):
        db_path = self._db_path(db_name)
        if os.path.isdir(db_path):
            shutil.rmtree(db_path)

    def list_databases(self):
        return [
            db_name.decode(FS_ENCODE)
            for db_name in os.listdir(self._repository)
            if os.path.isdir(self._db_path(db_name))
        ]

    def write_with_concern(self):
        return NotImplemented
