
import sys
import re

from bson.py3compat import integer_types
from bson.int64 import Int64
from bson.decimal128 import Decimal128

FS_ENCODE = sys.getfilesystemencoding()

PY3 = sys.version_info[0] == 3
PY36 = sys.version_info[0] == 3 and sys.version_info[1] >= 6

if PY3:
    from itertools import zip_longest
else:
    from itertools import izip_longest as zip_longest


RE_PATTERN_TYPE = type(re.compile(""))


def _compare_documents_in_py36(a, b):
    if len(a) == len(b):
        if all(ak == bk for ak, bk in zip_longest(a.keys(), b.keys())):
            for av, bv in zip_longest(a.values(), b.values()):
                if (is_duckument_type(av) and is_duckument_type(bv) and
                        not _compare_documents_in_py36(av, bv)) or av != bv:
                        return False
            return True

    return False


def compare_documents(this, that):
    """Document key order matters

    Before PY 3.6, the `dict` key order is defined by Python itself, so as
    long as two dicts both have same keys, thier order will be the same.

    In PY 3.6, `dict` has order-preserving,
    https://docs.python.org/3.6/whatsnew/3.6.html#new-dict-implementation
    but two different key ordered dictionary are still equal.
    ```python 3.6
    >>> print({'a': 1.0, 'b': 1.0})
    {'a': 1.0, 'b': 1.0}
    >>> print({'b': 1.0, 'a': 1.0})
    {'b': 1.0, 'a': 1.0}
    >>> {'a': 1.0, 'b': 1.0} == {'b': 1.0, 'a': 1.0}
    True
    ```
    Therefore, we should compare them by length, keys, values separately.
    """
    if not PY36:
        return this == that
    else:
        return _compare_documents_in_py36(this, that)


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
