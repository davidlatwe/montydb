from __future__ import absolute_import
import sys


__all__ = [  # noqa: F822
    "SON",  # noqa: F822
    "BSON",  # noqa: F822
    "ObjectId",  # noqa: F822
    "Timestamp",  # noqa: F822
    "MinKey",  # noqa: F822
    "MaxKey",  # noqa: F822
    "Int64",  # noqa: F822
    "Decimal128",  # noqa: F822
    "Binary",  # noqa: F822
    "Regex",  # noqa: F822
    "Code",  # noqa: F822
    "RawBSONDocument",  # noqa: F822
    "CodecOptions",  # noqa: F822

    "decimal128_NaN",  # noqa: F822
    "decimal128_INF",  # noqa: F822
    "decimal128_NaN_ls",  # noqa: F822

    "BSONError",  # noqa: F822
    "InvalidId",  # noqa: F822
    "InvalidDocument",  # noqa: F822

    "id_encode",  # noqa: F822
    "document_encode",  # noqa: F822
    "document_decode",  # noqa: F822
    "json_loads",  # noqa: F822
    "json_dumps",  # noqa: F822
    "parse_codec_options",  # noqa: F822
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
