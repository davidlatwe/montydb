"""Managing all data types and mocks
"""
import sys
import os
import platform
import re
import calendar
import datetime
from collections import OrderedDict

MONTY_ENABLE_BSON = bool(os.getenv("MONTY_ENABLE_BSON", False))

PY3 = sys.version_info[0] == 3


# Python 3 compat

if PY3:
    import collections.abc as abc
    from itertools import zip_longest
    from configparser import ConfigParser

    string_types = str,
    integer_types = int,
    text_type = str

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
    text_type = unicode

    def iteritems(d, **kw):
        return d.iteritems(**kw)

    unicode_ = unicode

    def encode_(s):
        return s.encode("utf-8")

    def decode_(s):
        return s.decode(_fs_encoding)
    _fs_encoding = sys.getfilesystemencoding()


# BSON types

if MONTY_ENABLE_BSON:
    from bson.errors import (
        BSONError,
        InvalidDocument,
        InvalidId,
    )

else:
    class BSONError(Exception):
        """Base class for all BSON exceptions.
        """

    class InvalidDocument(BSONError):
        """Raised when trying to create a BSON object from an invalid document.
        """

    class InvalidId(BSONError):
        """Raised when trying to create an ObjectId from invalid data.
        """


if MONTY_ENABLE_BSON:

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
    from .objectid import ObjectId
    from .tz_util import utc

    class _mock(object):
        def __init__(self, *args, **kwargs):
            pass

    SON = _mock
    BSON = _mock
    RawBSONDocument = _mock

    Timestamp = _mock
    MinKey = _mock
    MaxKey = _mock
    Int64 = _mock
    Decimal128 = _mock
    Binary = _mock
    Regex = _mock
    Code = _mock

    class CodecOptions(object):
        def __init__(self, document_class=dict, tz_aware=False, tzinfo=None):
            self.document_class = document_class
            self.tz_aware = tz_aware
            self.tzinfo = tzinfo

    DEFAULT_CODEC_OPTIONS = CodecOptions()

    def parse_codec_options(options):
        return CodecOptions(
            document_class=options.get("document_class", dict),
            tz_aware=options.get("tz_aware", DEFAULT_CODEC_OPTIONS.tz_aware),
            tzinfo=options.get("tzinfo", DEFAULT_CODEC_OPTIONS.tzinfo),
        )

    EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
    EPOCH_NAIVE = datetime.datetime.utcfromtimestamp(0)

    def _datetime_to_millis(dtm):
        """Convert datetime to milliseconds since epoch UTC."""
        if dtm.utcoffset() is not None:
            dtm = dtm - dtm.utcoffset()
        return int(calendar.timegm(dtm.timetuple()) * 1000 +
                   dtm.microsecond // 1000)

    def _millis_to_datetime(millis, opts):
        """Convert milliseconds since epoch UTC to datetime."""
        diff = ((millis % 1000) + 1000) % 1000
        seconds = (millis - diff) // 1000
        micros = diff * 1000
        if opts.tz_aware:
            dt = EPOCH_AWARE + datetime.timedelta(seconds=seconds,
                                                  microseconds=micros)
            if opts.tzinfo:
                dt = dt.astimezone(opts.tzinfo)
            return dt
        else:
            return EPOCH_NAIVE + datetime.timedelta(seconds=seconds,
                                                    microseconds=micros)

    class BSONEncoder(JSONEncoder):
        _key_is_keyword = False

        def default(self, obj):
            self._key_is_keyword = True

            if isinstance(obj, ObjectId):
                return {"$oid": str(obj)}

            if isinstance(obj, datetime.datetime):
                millis = _datetime_to_millis(obj)
                return {"$date": millis}

            if isinstance(obj, RE_PATTERN_TYPE):
                flags = re_int_flag_to_str(obj.flags)
                if isinstance(obj.pattern, text_type):
                    pattern = obj.pattern
                else:
                    pattern = obj.pattern.decode("utf-8")
                return OrderedDict([("$regex", pattern), ("$options", flags)])

            if hasattr(obj, "to_json"):
                return obj.to_json()

            return JSONEncoder.default(self, obj)

    def _key_validate(key):
        if "." in key:
            raise InvalidDocument("key '%s' must not contain '.'" % key)
        if key.startswith("$"):
            raise InvalidDocument("key '%s' must not start with '$'" % key)

    def document_encode(doc, check_keys=False, *args, **kwargs):
        _encoder = BSONEncoder()
        _key_sep = _encoder.key_separator
        candidate = serialized = ""
        for s in _encoder.iterencode(doc):
            if s == _key_sep:
                if not _encoder._key_is_keyword:
                    key = eval(candidate)
                    if not isinstance(key, string_types):
                        raise InvalidDocument(
                            "documents must have only string keys, key was "
                            "%r" % key)
                    if check_keys:
                        _key_validate(key)
                _encoder._key_is_keyword = False
            candidate = s
            serialized += s
        return serialized.encode()

    def object_hook(obj, opts=DEFAULT_CODEC_OPTIONS):
        if "$oid" in obj:
            return ObjectId(obj["$oid"])
        if "$date" in obj:
            return _millis_to_datetime(int(obj["$date"]), opts)
        if "$regex" in obj:
            flags = re_str_flags_to_int(obj.get("$options", ""))
            return re.compile(obj["$regex"], flags)
        for key in custom_json_hooks:
            if key in obj:
                return custom_json_hooks[key](obj, opts)
        return obj

    def document_decode(serialized, codec_options=None, *args, **kwargs):
        opts = codec_options or DEFAULT_CODEC_OPTIONS
        cls = opts.document_class
        return _loads(
            serialized,
            object_pairs_hook=lambda pairs: object_hook(cls(pairs), opts)
        )

    def json_loads(serialized):
        return _loads(serialized, object_hook=object_hook)

    def json_dumps(doc):
        return _dumps(doc, default=BSONEncoder().default)


custom_json_hooks = {}


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
