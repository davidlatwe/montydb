"""Managing all data types and mocks
"""
import sys
import platform
import re
from datetime import datetime
from collections import Mapping

PY3 = sys.version_info[0] == 3


# Python 3 compat

if PY3:
    import collections.abc as abc
    from itertools import zip_longest
    from configparser import ConfigParser

    string_types = str,
    integer_types = int,

    def iteritems(d, **kw):
        return iter(d.items(**kw))

    unicode_ = (lambda s: s)

    def encode_(s):
        return s

    def decode_(s):
        return s

else:
    import collections as abc
    from itertools import izip_longest as zip_longest
    from ConfigParser import ConfigParser

    string_types = basestring,
    integer_types = (int, long)

    def iteritems(d, **kw):
        return d.iteritems(**kw)

    unicode_ = unicode

    def encode_(s):
        return s.encode("utf-8")

    def decode_(s):
        return s.decode(_fs_encoding)
    _fs_encoding = sys.getfilesystemencoding()


# BSON types

try:

    from bson import SON, BSON
    from bson.timestamp import Timestamp
    from bson.objectid import ObjectId
    from bson.min_key import MinKey
    from bson.max_key import MaxKey
    from bson.int64 import Int64
    from bson.decimal128 import Decimal128
    from bson.binary import Binary
    from bson.regex import Regex
    from bson.code import Code
    from bson.raw_bson import RawBSONDocument

    from bson.codec_options import (
        CodecOptions,
        _parse_codec_options as parse_codec_options,
    )

except ImportError:
    _mock = type("mock", (object,), {})

    SON = _mock
    BSON = _mock
    Timestamp = _mock
    ObjectId = _mock
    MinKey = _mock
    MaxKey = _mock
    Int64 = _mock
    Decimal128 = _mock
    Binary = _mock
    Regex = _mock
    Code = _mock
    RawBSONDocument = _mock

    class CodecOptions(_mock):
        def __init__(self, document_class=dict):
            self.document_class = document_class

    def parse_codec_options(options):
        return CodecOptions(
            document_class=options.get("document_class", dict)
        )


RE_PATTERN_TYPE = type(re.compile(""))


_decimal128_NaN = Decimal128('NaN')
_decimal128_INF = Decimal128('Infinity')
_decimal128_NaN_ls = (_decimal128_NaN,
                      Decimal128('-NaN'),
                      Decimal128('sNaN'),
                      Decimal128('-sNaN'))


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
