from .engine import CursorEngine


class CursorType(object):
    EXHAUST = 64
    NON_TAILABLE = 0
    TAILABLE = 2
    TAILABLE_AWAIT = 34


class MontyCursor(object):
    def __init__(self,
                 collection,
                 filter=None,
                 projection=None,
                 skip=0,
                 limit=0,
                 no_cursor_timeout=False,
                 cursor_type=CursorType.NON_TAILABLE,
                 sort=None,
                 allow_partial_results=False,
                 oplog_replay=False,
                 modifiers=None,
                 batch_size=0,
                 manipulate=True,
                 collation=None,
                 hint=None,
                 max_scan=None,
                 max_time_ms=None,
                 max=None,
                 min=None,
                 return_key=False,
                 show_record_id=False,
                 snapshot=False,
                 comment=None):
        """
        """
        self._engine = CursorEngine(collection._engine)
        self._collection = collection

        self._filter = filter
        self._projection = projection
        self._skip = skip
        self._limit = limit

    def __getitem__(self, index):
        pass

    def _refresh(self):
        pass

    @property
    def address(self):
        pass

    @property
    def collection(self):
        pass

    @property
    def cursor_id(self):
        pass

    @property
    def retrieved(self):
        pass

    def next(self):
        pass

    __next__ = next

    def close(self):
        pass

    def clone(self):
        pass

    def collation(self, collation):
        pass

    def comment(self, comment):
        pass

    def count(self, with_limit_and_skip=False):
        pass

    def distinct(self, key):
        pass

    def explain(self):
        pass

    def hint(self, index):
        pass

    def limit(self, limit):
        pass

    def max(self, spec):
        pass

    def max_await_time_ms(self, max_await_time_ms):
        pass

    def max_scan(self, max_scan):
        pass

    def max_time_ms(self, max_time_ms):
        pass

    def min(self, spec):
        pass

    def remove_option(self, mask):
        pass

    def rewind(self):
        pass

    def skip(self, skip):
        pass

    def sort(self, key_or_list, direction=None):
        pass

    def where(self, code):
        pass
