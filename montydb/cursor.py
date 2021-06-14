import warnings
import copy
from collections import deque

from .errors import InvalidOperation, OperationFailure
from .engine.queries import QueryFilter, ordering
from .engine.project import Projector
from .types import (
    bson,
    RE_PATTERN_TYPE,
    iteritems,
    integer_types,
)
from .base import (
    validate_boolean,
    validate_is_mapping,
    _fields_list_to_dict,
    _index_list,
    _index_document,
)


NotImplementeds = {
    "collation",
    "comment",
    "distinct",
    "explain",
    "hint",
    "max",
    "max_await_time_ms",
    "max_time_ms",
    "min",
    "remove_option",
    "where",
    "batch_size",
}


_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "slave_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16,
    "await_data": 32,
    "exhaust": 64,
    "partial": 128,
}


class CursorType(object):
    NON_TAILABLE = 0
    TAILABLE = _QUERY_OPTIONS["tailable_cursor"]
    TAILABLE_AWAIT = TAILABLE | _QUERY_OPTIONS["await_data"]
    EXHAUST = _QUERY_OPTIONS["exhaust"]


class MontyCursor(object):
    def __init__(
        self,
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
        modifiers=None,  # DEPRECATED
        batch_size=0,
        manipulate=True,  # DEPRECATED
        collation=None,
        hint=None,
        max_scan=None,
        max_time_ms=None,
        max=None,
        min=None,
        return_key=False,
        show_record_id=False,
        snapshot=False,
        comment=None,
        *args,
        **kwargs
    ):
        """ """
        self._id = None
        self._exhaust = False
        self._killed = False

        spec = filter
        if spec is None:
            spec = {}

        validate_is_mapping("filter", spec)

        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")

        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")

        if cursor_type not in (
            CursorType.NON_TAILABLE,
            CursorType.TAILABLE,
            CursorType.TAILABLE_AWAIT,
            CursorType.EXHAUST,
        ):
            raise ValueError("not a valid value for cursor_type")

        if projection is not None:
            if not projection:
                projection = {"_id": 1}
            projection = _fields_list_to_dict(projection, "projection")

        self._address = collection.database.client.address
        self._collection = collection
        self._components = (collection.database, collection, self)
        self._codec_options = collection.codec_options

        self._spec = spec
        self._projection = projection
        self._skip = skip
        self._limit = limit
        self._ordering = sort and _index_document(sort) or None
        self._max_scan = max_scan
        self._max = max
        self._min = min
        self._return_key = return_key

        self._empty = False
        self._data = deque()
        self._retrieved = 0

        self._query_flags = cursor_type

        self._doc_count = 0
        self._doc_count_with_skip_limit = 0

    def __getattr__(self, name):
        if name in NotImplementeds:
            raise NotImplementedError("'MontyCursor.%s' is NOT implemented !" % name)
        raise AttributeError("'MontyCursor' object has no attribute '%s'" % name)

    def __getitem__(self, index):
        """Get a single document or a slice of documents from this cursor."""
        self.__check_okay_to_chain()
        self._empty = False
        if isinstance(index, slice):
            if index.step is not None:
                raise IndexError("Cursor instances do not support slice steps")

            skip = 0
            if index.start is not None:
                if index.start < 0:
                    raise IndexError(
                        "Cursor instances do not support negative indices"
                    )
                skip = index.start

            if index.stop is not None:
                limit = index.stop - skip
                if limit < 0:
                    raise IndexError(
                        "stop index must be greater than start "
                        "index for slice %r" % index
                    )
                if limit == 0:
                    self._empty = True
            else:
                limit = 0

            self._skip = skip
            self._limit = limit

            return self

        if isinstance(index, integer_types):
            if index < 0:
                raise IndexError("Cursor instances do not support negative indices")
            clone = self.clone()
            clone.skip(index + self._skip)
            clone.limit(-1)  # use a hard limit
            clone._query_flags &= ~CursorType.TAILABLE_AWAIT  # PYTHON-1371
            for doc in clone:
                return doc

            raise IndexError("no such item for Cursor instance")
        raise TypeError("index %r cannot be applied to Cursor instances" % index)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __copy__(self):
        return self._clone(deepcopy=False)

    def __deepcopy__(self, memo):
        return self._clone(deepcopy=True)

    def _deepcopy(self, x, memo=None):
        if not hasattr(x, "items"):
            y, is_list, iterator = [], True, enumerate(x)
        else:
            y, is_list, iterator = {}, False, iteritems(x)

        if memo is None:
            memo = {}
        val_id = id(x)
        if val_id in memo:
            return memo.get(val_id)
        memo[val_id] = y

        for key, value in iterator:
            if isinstance(value, (dict, list)) and not isinstance(value, bson.SON):
                value = self._deepcopy(value, memo)
            elif not isinstance(value, RE_PATTERN_TYPE):
                value = copy.deepcopy(value, memo)

            if is_list:
                y.append(value)
            else:
                if not isinstance(key, RE_PATTERN_TYPE):
                    key = copy.deepcopy(key, memo)
                y[key] = value
        return y

    def _clone(self, deepcopy=True, base=None):
        if not base:
            base = self._clone_base()

        values_to_clone = (
            "spec",
            "projection",
            "skip",
            "limit",
            # "max_time_ms",
            # "max_await_time_ms",
            # "comment",
            "max",
            "min",
            "ordering",
            # "explain",
            # "hint",
            "batch_size",
            "max_scan",
            # "manipulate",
            "query_flags",
            # "modifiers",
            # "collation"
        )
        data = dict(
            (k, v)
            for k, v in iteritems(self.__dict__)
            if k.startswith("_") and k[1:] in values_to_clone
        )
        if deepcopy:
            data = self._deepcopy(data)
        base.__dict__.update(data)
        return base

    def _clone_base(self):
        return self.__class__(self._collection)

    def __check_okay_to_chain(self):
        if self._retrieved or self._id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def __die(self):
        pass

    def __query(self):
        """ """
        # Validate params before jump into storage

        if self._skip < 0:
            raise OperationFailure(
                "Skip value must be non-negative, but received: {}"
                "".format(self._skip)
            )

        max_scan = 0
        if self._max_scan:
            if isinstance(self._max_scan, bool) or not isinstance(
                self._max_scan, (int, float)
            ):
                raise OperationFailure("'maxScan' field must be numeric.")
            if int(self._max_scan) < 0:
                raise OperationFailure(
                    "MaxScan value must be non-negative, but received: {}"
                    "".format(self._max_scan)
                )
            max_scan = int(self._max_scan)

        queryfilter = QueryFilter(self._spec)
        projector = None
        if self._projection:
            projector = Projector(self._projection, queryfilter)

        # Fetch from storage
        # (NOTE) Documents return from storage should be decoded.
        storage = self._collection.database.client._storage
        documents = storage.query(self, max_scan)
        # Filtering
        fieldwalkers = []
        for doc in documents:
            if queryfilter(doc):
                fieldwalkers.append(queryfilter.fieldwalker)

        self._doc_count = len(fieldwalkers)

        # Sorting
        if self._ordering:
            fieldwalkers = ordering(fieldwalkers, self._ordering)

        # Limit and Skip
        if self._skip and not self._limit:
            fieldwalkers = fieldwalkers[self._skip:]
        elif self._limit:
            end = self._skip + abs(self._limit)
            fieldwalkers = fieldwalkers[self._skip: end]

        self._doc_count_with_skip_limit = len(fieldwalkers)

        # Projection
        if projector:
            for fw in fieldwalkers:
                projector(fw)

        self._data = deque(fw.doc for fw in fieldwalkers)
        self._retrieved += len(fieldwalkers)
        # (NOTE) cursor id should return from storage, but ignore for now.
        self._id = 0

        if self._id == 0:
            self._killed = True
            self.__die()

        if self._limit and self._id and self._limit <= self._retrieved:
            self.__die()

    def _refresh(self):
        """Refreshes the cursor with more data from Monty."""
        if len(self._data) or self._killed:
            return len(self._data)

        if self._id is None:
            # (NOTE) Query
            self.__query()
        elif self._id:
            # (NOTE) Get More
            pass

        return len(self._data)

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

    @property
    def alive(self):
        return bool(len(self._data) or (not self._killed))

    def close(self):
        self.__die()

    def clone(self):
        return self._clone(True)

    def count(self, with_limit_and_skip=False):
        warnings.warn(
            "count is deprecated. Use Collection.count_documents instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        validate_boolean("with_limit_and_skip", with_limit_and_skip)
        if self._id is None:
            # (NOTE) this might need improve
            self.__query()
        if with_limit_and_skip:
            return self._doc_count_with_skip_limit
        return self._doc_count

    def limit(self, limit):
        if not isinstance(limit, integer_types):
            raise TypeError("limit must be an integer")
        if self._exhaust:
            raise InvalidOperation("Can't use limit and exhaust together.")
        self.__check_okay_to_chain()

        self._empty = False
        self._limit = limit
        return self

    def max_scan(self, max_scan):
        self.__check_okay_to_chain()
        self._max_scan = max_scan
        return self

    def rewind(self):
        self._data = deque()
        self._id = None
        self._retrieved = 0
        self._killed = False

        return self

    def skip(self, skip):
        if not isinstance(skip, integer_types):
            raise TypeError("skip must be an integer")
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self.__check_okay_to_chain()

        self._skip = skip
        return self

    def sort(self, key_or_list, direction=None):
        """ """
        self.__check_okay_to_chain()
        keys = _index_list(key_or_list, direction)
        self._ordering = _index_document(keys)
        return self
