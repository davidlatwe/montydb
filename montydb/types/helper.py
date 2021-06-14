from __future__ import absolute_import
import re
import sys
import platform
from .compat import integer_types, zip_longest
from . import bson

RE_PATTERN_TYPE = type(re.compile(""))


def to_bytes(s):
    return s.encode()


def _compare_doc_in_strict_order(a, b):
    if len(a) == len(b):
        if all(ak == bk for ak, bk in zip_longest(a.keys(), b.keys())):
            for av, bv in zip_longest(a.values(), b.values()):
                if (
                    is_duckument_type(av)
                    and is_duckument_type(bv)
                    and not _compare_doc_in_strict_order(av, bv)
                ) or av != bv:
                    return False
            return True
    return False


DictKeyOrderMutable = (
    # CPython >= 3.5
    sys.version_info >= (3, 5)
    # all PyPy
    or platform.python_implementation() == "PyPy"
)


def compare_documents(this, that):
    """Document key order matters

    In PY 3.6, `dict` has order-preserving, so does PyPy and PyPy3, and in
    PY 3.5, `dict` is random iteration ordered.

    Which means, same key-value pairs could have different orders in runtime,
    and two dictionary with same values but different key ordered are still
    equal in comparison, but not equal in MongoDB.

    Therefore, we should compare them by length, keys, values separately.
    """
    if not DictKeyOrderMutable:
        # use simple `equal` if the order is constant
        return this == that
    else:
        return _compare_doc_in_strict_order(this, that)


def is_duckument_type(obj):
    """Internal mapping type checker

    Instead of using `isinstance(obj, MutableMapping)`, duck type checking
    is much cheaper and work on most common use cases.

    If an object has these attritubes, is a document:
        `__len__`, `keys`, `values`

    """
    doc_attrs = ("__len__", "keys", "values")
    return all(hasattr(obj, attr) for attr in doc_attrs)


def is_numeric_type(obj):
    if isinstance(obj, bool):
        return False
    return isinstance(obj, (integer_types, float, bson.Int64, bson.Decimal128))


def is_integer_type(obj):
    if isinstance(obj, bool):
        return False
    return isinstance(obj, (integer_types, bson.Int64))


def is_pattern_type(obj):
    return isinstance(obj, RE_PATTERN_TYPE)


def re_int_flag_to_str(int_flags):
    """Ripped from bson.json_util"""
    flags = ""
    if int_flags & re.IGNORECASE:
        flags += "i"
    if int_flags & re.LOCALE:
        flags += "l"
    if int_flags & re.MULTILINE:
        flags += "m"
    if int_flags & re.DOTALL:
        flags += "s"
    if int_flags & re.UNICODE:
        flags += "u"
    if int_flags & re.VERBOSE:
        flags += "x"
    return flags


def re_str_flags_to_int(str_flags):
    flags = 0
    if "i" in str_flags:
        flags |= re.IGNORECASE
    if "l" in str_flags:
        flags |= re.LOCALE
    if "m" in str_flags:
        flags |= re.MULTILINE
    if "s" in str_flags:
        flags |= re.DOTALL
    if "u" in str_flags:
        flags |= re.UNICODE
    if "x" in str_flags:
        flags |= re.VERBOSE

    return flags


def __add_attrib(deco):
    """Decorator helper"""

    def meta_decorator(value):
        def add_attrib(func):
            func._keep = lambda: value
            return func

        return add_attrib

    return meta_decorator


@__add_attrib
def keep(query):
    """A decorator that preserve operation query for operator"""

    def _(func):
        return query

    return _


class Counter(object):
    """An iterator that could trace the progress of iteration

    Args:
        iterable (Iterable): An iterable object.
        job_on_each (func): The function to process each item in the iterable
                            during the iteration. `function(item)`
                            The return value of the function will be stored in
                            `Counter.data` attribute.

    Attributes:
        count (int): An int that indicate the progress of iteration.
        data: The value returned from the `job_on_each` function.

    """

    def __init__(self, iterable, job_on_each=None):
        self._iterable = iterable
        self._job = job_on_each or (lambda i: None)
        self.count = 0
        self.data = None

    def __next__(self):
        item = next(self._iterable)
        self.data = self._job(item)
        self.count += 1
        return item

    next = __next__

    def __iter__(self):
        return self


def on_err_close(generator):
    """Decorator, to close `generator` passed into the function on error

    Ensure the `generator` which holds the storage database connection to
    close that connection right on error occurred and then raise the error.

    In some tests, one may leave the connection open and cause next test
    case fail on startup or teardown, e.g. drop collection that created by
    previous test. The reason why that previous connection remains open,
    could be the `generator` which holds the connection context was not
    being garbage collected (`__exit__` was not triggered) while entring
    the next test. So we need this at least for now, until better solution
    or better test cases came up.

    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                for i in func(*args, **kwargs):
                    yield i
            except Exception:
                generator.close()
                raise

        return wrapper

    return decorator
