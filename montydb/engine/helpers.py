
import sys

from bson.py3compat import abc, PY3


FS_ENCODE = sys.getfilesystemencoding()

PY36 = sys.version_info[0] == 3 and sys.version_info[1] >= 6


def is_mapping_type(obj):
    return isinstance(obj, abc.Mapping)


def is_array_type(obj):
    return isinstance(obj, (list, tuple))


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
