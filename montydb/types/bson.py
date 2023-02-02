from __future__ import absolute_import
import sys


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


bson_used = None


def __getattr__(name):
    if name in __all__:
        from .. import configure

        config = configure.session_config()
        init(config.get("use_bson"))
        return getattr(sys.modules[__name__], name)


def init(use_bson=None):
    from .. import errors

    self = sys.modules[__name__]
    if self.bson_used is not None:
        return

    # Init

    if use_bson is None:
        try:
            import bson  # noqa: F401
        except ImportError:
            use_bson = False
        else:
            use_bson = True

    if use_bson:
        from ._bson import BSON_
        bson_ = BSON_()
    else:
        from ._nobson import NoBSON
        bson_ = NoBSON()

    self.bson_used = bson_.bson_used
    for name in __all__:
        setattr(self, name, getattr(bson_, name))

    errors.init_bson_err()
