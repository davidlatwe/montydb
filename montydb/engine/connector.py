import sqlite3
import contextlib


class Memory(object):
    pass


class SQLazyConn(object):
    """
    """
    def __init__(self):
        self.__conn = None

    def _read(self, sql, par):
        """Ignore table's existence and get a cursor.
        """
        try:
            return self.__conn.execute(sql, par)
        except sqlite3.OperationalError as e:
            if e.args[0].startswith("no such table"):
                return self.__conn.cursor()
            else:
                raise

    def _pragma_parser(self, wconcern):
        pragma = [
            "PRAGMA foreign_keys=ON",
            "PRAGMA automatic_index=OFF",
        ]
        if wconcern:
            wcon_doc = wconcern.document
            wtimeout = wcon_doc.get("wtimeout")
            sqlite_jmode = wcon_doc.get("sqlite_jmode")

            if wtimeout:
                pragma.append("PRAGMA busy_timeout={}".format(wtimeout))
            if sqlite_jmode:
                pragma.append("PRAGMA journal_mode={}".format(sqlite_jmode))
                if sqlite_jmode in ["OFF", "MEMORY"]:
                    pragma.append("PRAGMA synchronous=0")
                elif sqlite_jmode == "WAL":
                    pragma.append("PRAGMA synchronous=1")
                elif sqlite_jmode in ["TRUNCATE", "PERSIST", "DELETE"]:
                    pragma.append("PRAGMA synchronous=2")
                elif sqlite_jmode == "EXTRA":
                    pragma.append("PRAGMA synchronous=3")
                    pragma.append("PRAGMA journal_mode=DELETE")
        return ";".join(pragma)

    def _conn(self, db_path, wconcern):
        self.__conn = sqlite3.connect(db_path)
        # Assemble all PRAGMA and execute as script
        self.__conn.executescript(self._pragma_parser(wconcern))
        self.__conn.text_factory = str
        return contextlib.closing(self.__conn)

    def run_script(self, db_path, sql, wconcern=None):
        with self._conn(db_path, wconcern) as conn:
            with conn:
                conn.executescript(sql)

    def write(self, db_path, sql, par=(), wconcern=None):
        with self._conn(db_path, wconcern) as conn:
            with conn:
                conn.execute(sql, par)

    def write_many(self, db_path, sql, seq_par, wconcern=None):
        with self._conn(db_path, wconcern) as conn:
            with conn:
                conn.executemany(sql, seq_par)

    def read_one(self, db_path, sql, par=(), wconcern=None):
        with self._conn(db_path, wconcern) as conn:
            with conn:
                return self._read(sql, par).fetchone()

    def read_all(self, db_path, sql, par=(), wconcern=None):
        with self._conn(db_path, wconcern) as conn:
            with conn:
                return self._read(sql, par).fetchall()

    def read_many(self, db_path, sql, par=(), arraysize=1000, wconcern=None):
        with self._conn(db_path, wconcern) as conn:
            with conn:
                cursor = self._read(sql, par)
                for result in iter(lambda: cursor.fetchmany(arraysize), []):
                    yield result
