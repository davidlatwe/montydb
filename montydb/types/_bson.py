from __future__ import absolute_import
import types


class BSON_(types.ModuleType):

    from bson import (
        SON,
        BSON,
        Code,
        Int64,
        Regex,
        Binary,
        MinKey,
        MaxKey,
        ObjectId,
        Timestamp,
        Decimal128,
        _ENCODERS,
    )
    from bson.raw_bson import (
        RawBSONDocument,
    )
    from bson.json_util import (
        CANONICAL_JSON_OPTIONS,
    )
    from bson.codec_options import (
        CodecOptions,
        DEFAULT_CODEC_OPTIONS,
    )
    from bson.errors import (
        BSONError,
        InvalidId,
        InvalidDocument,
    )

    decimal128_NaN = Decimal128("NaN")
    decimal128_INF = Decimal128("Infinity")
    decimal128_NaN_ls = (
        decimal128_NaN,
        Decimal128("-NaN"),
        Decimal128("sNaN"),
        Decimal128("-sNaN"),
    )

    def __init__(self):
        self.bson_used = True
        super(BSON_, self).__init__(__name__)

    @classmethod
    def parse_codec_options(cls, options):
        from bson.codec_options import _parse_codec_options

        return _parse_codec_options(options)

    @classmethod
    def document_encode(
        cls, doc, check_keys=False, codec_options=DEFAULT_CODEC_OPTIONS
    ):
        return cls.BSON.encode(doc, check_keys=check_keys, codec_options=codec_options)

    @classmethod
    def document_decode(cls, doc, codec_options=DEFAULT_CODEC_OPTIONS):
        return cls.BSON(doc).decode(codec_options)

    @classmethod
    def json_loads(cls, serialized, json_options=None):
        from bson.json_util import loads as _loads

        json_options = json_options or cls.CANONICAL_JSON_OPTIONS
        return _loads(serialized, json_options=json_options)

    @classmethod
    def json_dumps(cls, doc, json_options=None):
        from bson.json_util import dumps as _dumps

        json_options = json_options or cls.CANONICAL_JSON_OPTIONS
        return _dumps(doc, json_options=json_options)

    @classmethod
    def id_encode(cls, id, codec_options=DEFAULT_CODEC_OPTIONS):
        # args: name, value, check_keys, opts
        return cls._ENCODERS[type(id)](b"", id, False, codec_options)
