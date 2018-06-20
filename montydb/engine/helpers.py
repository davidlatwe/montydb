
import sys
import re
from collections import Mapping

from bson.py3compat import integer_types
from bson.int64 import Int64
from bson.decimal128 import Decimal128

FS_ENCODE = sys.getfilesystemencoding()

PY3 = sys.version_info[0] == 3
PY36 = sys.version_info[0] == 3 and sys.version_info[1] >= 6

RE_PATTERN_TYPE = type(re.compile(""))


def is_mapping_type(obj):
    return isinstance(obj, Mapping)


def is_array_type(obj):
    return isinstance(obj, (list, tuple))


def is_numeric_type(obj):
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
