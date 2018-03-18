

class CursorEngine(object):
    def __init__(self, col_engine):
        self.col_engine = col_engine

    def query_by_scan(self, filter):
        SELECT_ALL = """
            SELECT doc FROM [{}]
        """.format(self.col_engine.db_engine.COL_TBL)
        self.col_engine.db_engine.repo_engine.conn.read_many(
            SELECT_ALL
        )
