
from collections import deque


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
        self._id = None
        self._killed = False

        spec = filter
        if spec is None:
            spec = {}

        self._address = collection.database.client.address
        self._collection = collection
        self._codec_options = collection.codec_options

        self._spec = spec
        self._projection = projection
        self._skip = skip
        self._limit = limit
        self._ordering = sort

        self._empty = False
        self._data = deque()
        self._retrieved = 0

    def __getitem__(self, index):
        """Get a single document or a slice of documents from this cursor.
        """
        pass

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __die(self):
        pass

    def __fetch(self):
        storage = self._collection.database.client._storage
        res = storage.query(self._collection.database.name,
                            self._collection.name,
                            self._skip,
                            self._spec,
                            self._projection,
                            self._limit)

        documents = self._decode(res)

        self._data = deque(documents)
        self._retrieved += len(documents)
        self._id = 0

        if self._id == 0:
            self._killed = True
            self.__die()

        if self._limit and self._id and self._limit <= self._retrieved:
            self.__die()

    def _refresh(self):
        """Refreshes the cursor with more data from Monty.
        """
        if len(self._data) or self._killed:
            return len(self._data)

        if self._id is None:
            self.__fetch()

    def next(self):
        """Advance the cursor."""
        if self._empty:
            raise StopIteration
        if len(self._data) or self._refresh():
            return self._data.popleft()
        else:
            raise StopIteration

    __next__ = next

    @property
    def address(self):
        return self._address

    @property
    def collection(self):
        return self._collection

    @property
    def cursor_id(self):
        return self._id

    @property
    def retrieved(self):
        return self._retrieved

    def close(self):
        self.__die()

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
        self._data = deque()
        self._id = None
        self._retrieved = 0
        self._killed = False

        return self

    def skip(self, skip):
        pass

    def sort(self, key_or_list, direction=None):
        pass

    def where(self, code):
        pass
