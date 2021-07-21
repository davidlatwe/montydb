import sys
import types
import base64
import ast


def _mock(name):
    class Mock(object):
        def __init__(self, *args, **kwargs):
            self.name = name
            self.args = args
            self.kwargs = kwargs

        def __eq__(self, other):
            if isinstance(other, Mock):
                return (
                    self.name == other.name
                    and self.args == other.args
                    and self.kwargs == other.kwargs
                )
            return False

    return Mock


class NoBSON(types.ModuleType):
    import re
    import datetime
    import calendar
    from collections import OrderedDict
    from json import (
        JSONEncoder,
    )

    class BSONError(Exception):
        """Base class for all BSON exceptions."""

    class InvalidId(BSONError):
        """Raised when trying to create an ObjectId from invalid data."""

    class InvalidDocument(BSONError):
        """Raised when trying to create a BSON object from an invalid document."""

    SON = _mock("SON")
    BSON = _mock("BSON")
    Code = _mock("Code")
    Int64 = _mock("Int64")
    Regex = _mock("Regex")
    MinKey = _mock("MinKey")
    MaxKey = _mock("MaxKey")
    Binary = _mock("Binary")
    Timestamp = _mock("Timestamp")
    Decimal128 = _mock("Decimal128")
    RawBSONDocument = _mock("RawBSONDocument")

    decimal128_NaN = Decimal128("NaN")
    decimal128_INF = Decimal128("Infinity")
    decimal128_NaN_ls = (
        decimal128_NaN,
        Decimal128("-NaN"),
        Decimal128("sNaN"),
        Decimal128("-sNaN"),
    )

    from .objectid import ObjectId
    from .tz_util import utc

    class CodecOptions(object):
        def __init__(self, document_class=dict, tz_aware=False, tzinfo=None):
            self.document_class = document_class
            self.tz_aware = tz_aware
            self.tzinfo = tzinfo

    DEFAULT_CODEC_OPTIONS = CodecOptions()

    def __init__(self):
        self.bson_used = False
        super(NoBSON, self).__init__(__name__)

    @classmethod
    def parse_codec_options(cls, options):
        default_opt = cls.DEFAULT_CODEC_OPTIONS
        return cls.CodecOptions(
            document_class=options.get("document_class", dict),
            tz_aware=options.get("tz_aware", default_opt.tz_aware),
            tzinfo=options.get("tzinfo", default_opt.tzinfo),
        )

    EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
    EPOCH_NAIVE = datetime.datetime.utcfromtimestamp(0)

    @classmethod
    def _datetime_to_millis(cls, dtm):
        """Convert datetime to milliseconds since epoch UTC."""
        if dtm.utcoffset() is not None:
            dtm = dtm - dtm.utcoffset()
        return int(
            cls.calendar.timegm(dtm.timetuple()) * 1000 + dtm.microsecond // 1000
        )

    @classmethod
    def _millis_to_datetime(cls, millis, opts):
        """Convert milliseconds since epoch UTC to datetime."""
        diff = ((millis % 1000) + 1000) % 1000
        seconds = (millis - diff) // 1000
        micros = diff * 1000
        if opts.tz_aware:
            dt = cls.EPOCH_AWARE + cls.datetime.timedelta(
                seconds=seconds, microseconds=micros
            )
            if opts.tzinfo:
                dt = dt.astimezone(opts.tzinfo)
            return dt
        else:
            return cls.EPOCH_NAIVE + cls.datetime.timedelta(
                seconds=seconds, microseconds=micros
            )

    class BSONEncoder(JSONEncoder):
        key_is_keyword = False

        def default(self, obj):
            self.key_is_keyword = True

            if isinstance(obj, NoBSON.ObjectId):
                return {"$oid": str(obj)}

            if isinstance(obj, bytes):
                return {"$binary": {"base64": base64.b64encode(obj).decode(),
                                    "subType": "00"}}

            if isinstance(obj, NoBSON.datetime.datetime):
                millis = NoBSON._datetime_to_millis(obj)
                return {"$date": millis}

            if isinstance(obj, NoBSON._re_type):
                flags = NoBSON._re_int_flag_to_str(obj.flags)
                if isinstance(obj.pattern, NoBSON._text_type):
                    pattern = obj.pattern
                else:
                    pattern = obj.pattern.decode("utf-8")
                return NoBSON.OrderedDict([("$regex", pattern), ("$options", flags)])

            if hasattr(obj, "to_json"):
                return obj.to_json()

            return NoBSON.JSONEncoder.default(self, obj)

    @classmethod
    def _key_validate(cls, key):
        if "." in key:
            raise cls.InvalidDocument("key '%s' must not contain '.'" % key)
        if key.startswith("$"):
            raise cls.InvalidDocument("key '%s' must not start with '$'" % key)

    @classmethod
    def document_encode(cls, doc, check_keys=False, *args, **kwargs):
        _encoder = cls.BSONEncoder()
        _key_sep = _encoder.key_separator
        candidate = serialized = ""
        for s in _encoder.iterencode(doc):
            if s == _key_sep:
                if not _encoder.key_is_keyword:
                    key = ast.literal_eval(candidate)
                    if not isinstance(key, cls._string_types):
                        raise cls.InvalidDocument(
                            "documents must have only string keys, key was %r" % key
                        )
                    if check_keys:
                        cls._key_validate(key)
                _encoder.key_is_keyword = False
            candidate = s
            serialized += s
        return serialized.encode()

    @classmethod
    def object_hook(cls, obj, opts=DEFAULT_CODEC_OPTIONS):
        if "$oid" in obj:
            return cls.ObjectId(obj["$oid"])
        if "$binary" in obj:
            return base64.b64decode(obj["$binary"]["base64"])
        if "$date" in obj:
            return cls._millis_to_datetime(int(obj["$date"]), opts)
        if "$regex" in obj:
            flags = cls._re_str_flags_to_int(obj.get("$options", ""))
            return cls.re.compile(obj["$regex"], flags)
        for key in cls.custom_json_hooks:
            if key in obj:
                return cls.custom_json_hooks[key](obj, opts)
        return obj

    @classmethod
    def document_decode(cls, serialized, codec_options=None, *args, **kwargs):
        from json import loads as _loads

        opts = codec_options or cls.DEFAULT_CODEC_OPTIONS
        dcls = opts.document_class
        return _loads(
            serialized,
            object_pairs_hook=lambda pairs: cls.object_hook(dcls(pairs), opts),
        )

    @classmethod
    def json_loads(cls, serialized, *args, **kwarg):
        from json import loads as _loads

        return _loads(serialized, object_hook=cls.object_hook)

    @classmethod
    def json_dumps(cls, doc, *args, **kwarg):
        from json import dumps as _dumps

        return _dumps(doc, default=cls.BSONEncoder().default)

    @classmethod
    def id_encode(cls, id, *args, **kwargs):
        return cls.document_encode(id)

    custom_json_hooks = {}

    _re_type = type(re.compile(""))
    _re_flags = (
        ("i", re.IGNORECASE),
        ("l", re.LOCALE),
        ("m", re.MULTILINE),
        ("s", re.DOTALL),
        ("u", re.UNICODE),
        ("x", re.VERBOSE),
    )

    if sys.version_info[0] == 3:
        _string_types = (str,)
        _integer_types = (int,)
        _text_type = str
    else:
        _string_types = (basestring,)  # noqa
        _integer_types = (int, long)  # noqa
        _text_type = unicode  # noqa

    @classmethod
    def _re_int_flag_to_str(cls, int_flags):
        return "".join(str_f for str_f, re_F in cls._re_flags if int_flags & re_F)

    @classmethod
    def _re_str_flags_to_int(cls, str_flags):
        flags = 0
        for str_f, re_F in cls._re_flags:
            flags |= re_F if str_f in str_flags else 0
        return flags
