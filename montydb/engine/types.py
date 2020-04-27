"""Managing all data types and mocks
"""
import sys
import platform
import re
from datetime import datetime
from collections import Mapping

from . import ENABLE_BSON

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

if ENABLE_BSON:

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

    from bson.json_util import (
        CANONICAL_JSON_OPTIONS,
        loads as _loads,
        dumps as _dumps,
    )

    from bson.codec_options import (
        DEFAULT_CODEC_OPTIONS,
        CodecOptions,
        _parse_codec_options as parse_codec_options,
    )

    from bson.errors import (
        BSONError,
        InvalidDocument,
    )

    def document_encode(doc,
                        check_keys=False,
                        codec_options=DEFAULT_CODEC_OPTIONS):
        return BSON.encode(doc,
                           check_keys=check_keys,
                           codec_options=codec_options)

    def document_decode(doc, codec_options=DEFAULT_CODEC_OPTIONS):
        return BSON(doc).decode(codec_options)

    def json_loads(serialized):
        return _loads(serialized, json_options=CANONICAL_JSON_OPTIONS)

    def json_dumps(doc):
        return _dumps(doc, json_options=CANONICAL_JSON_OPTIONS)

else:

    from json import (
        JSONEncoder,
        loads as _loads,
        dumps as _dumps,
    )

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

    class BSONError(Exception):
        """Base class for all BSON exceptions.
        """

    class InvalidDocument(BSONError):
        """Raised when trying to create a BSON object from an invalid document.
        """

    def document_encode(doc, check_keys=False, *args, **kwargs):
        if check_keys:
            serialized = ""
            encoder = JSONEncoder()
            item_sep = encoder.item_separator
            key_incoming = False
            for s in encoder.iterencode(doc):
                if key_incoming:
                    key_incoming = False
                    if "." in s:
                        msg = "key '%s' must not contain '.'" % s
                        raise InvalidDocument(msg)
                    if s.startswith("$"):
                        msg = "key '%s' must not start with '$'" % s
                        raise InvalidDocument(msg)

                elif s == "{" or s == item_sep:
                    key_incoming = True

                serialized += s

            return serialized
        else:
            return _dumps(doc)

    def document_decode(serialized, codec_options=None, *args, **kwargs):
        if codec_options:
            object_hook = codec_options.document_class
        else:
            object_hook = None
        return _loads(serialized, object_hook=object_hook)

    def json_loads(serialized):
        return _loads(serialized)

    def json_dumps(doc):
        return _dumps(doc)


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
