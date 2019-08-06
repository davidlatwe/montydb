
import sys
import platform
import re

from bson.py3compat import integer_types
from bson.int64 import Int64
from bson.decimal128 import Decimal128

FS_ENCODE = sys.getfilesystemencoding()

PY3 = sys.version_info[0] == 3

if PY3:
    from itertools import zip_longest
else:
    from itertools import izip_longest as zip_longest


RE_PATTERN_TYPE = type(re.compile(""))


def _compare_doc_in_strict_order(a, b):
    if len(a) == len(b):
        if all(ak == bk for ak, bk in zip_longest(a.keys(), b.keys())):
            for av, bv in zip_longest(a.values(), b.values()):
                if (is_duckument_type(av) and is_duckument_type(bv) and
                        not _compare_doc_in_strict_order(av, bv)) or av != bv:
                        return False
            return True

    return False


DictKeyOrderMutable = (
    # CPython >= 3.5
    (sys.version_info[0] == 3 and sys.version_info[1] >= 5) or
    # all PyPy
    platform.python_implementation() == "PyPy"
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
    number_type = (integer_types, float, Int64, Decimal128)
    return isinstance(obj, number_type)


def is_integer_type(obj):
    if isinstance(obj, bool):
        return False
    integer_type = (integer_types, Int64)
    return isinstance(obj, integer_type)


def is_pattern_type(obj):
    return isinstance(obj, RE_PATTERN_TYPE)


def re_int_flag_to_str(int_flags):
    """Ripped from bson.json_util
    """
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


def decode_(s):
    if PY3:
        return s
    else:
        return s.decode(FS_ENCODE)


def encode_(s):
    if PY3:
        return s
    else:
        return s.encode("utf-8")


# Grab from six.py
def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""
    # This requires a bit of explanation: the basic idea is to make a dummy
    # metaclass for one level of class instantiation that replaces itself with
    # the actual metaclass.
    class metaclass(meta):

        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)
    return type.__new__(metaclass, 'temporary_class', (), {})


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
