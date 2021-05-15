
import sys

bson_used = None


SON = None
BSON = None
ObjectId = None
Timestamp = None
MinKey = None
MaxKey = None
Int64 = None
Decimal128 = None
Binary = None
Regex = None
Code = None
RawBSONDocument = None
CodecOptions = None

decimal128_NaN = None
decimal128_INF = None
decimal128_NaN_ls = None

BSONError = None
InvalidId = None
InvalidDocument = None

id_encode = None
document_encode = None
document_decode = None
json_loads = None
json_dumps = None
parse_codec_options = None


def init(use_bson=None):
    from . import _bson
    from .. import errors

    self = sys.modules[__name__]
    if self.bson_used is not None:
        return

    # Init

    if use_bson is None:
        try:
            import bson
        except ImportError:
            use_bson = False
        else:
            use_bson = True

    if use_bson:
        bson_ = _bson.BSON_()
    else:
        bson_ = _bson.NoBSON()

    self.bson_used = bson_.bson_used
    for name in __all__:
        setattr(self, name, getattr(bson_, name))

    errors.init_bson_err()


__all__ = [
    "SON",
    "BSON",
    "ObjectId",
    "Timestamp",
    "MinKey",
    "MaxKey",
    "Int64",
    "Decimal128",
    "Binary",
    "Regex",
    "Code",
    "RawBSONDocument",
    "CodecOptions",

    "decimal128_NaN",
    "decimal128_INF",
    "decimal128_NaN_ls",

    "BSONError",
    "InvalidId",
    "InvalidDocument",

    "id_encode",
    "document_encode",
    "document_decode",
    "json_loads",
    "json_dumps",
    "parse_codec_options",
]
