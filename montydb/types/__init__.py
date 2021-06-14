"""Managing all data types and mocks
"""

from .compat import (
    PY3,
    ConfigParser,
    abc,
    zip_longest,
    integer_types,
    string_types,
    text_type,
    iteritems,
    unicode_,
    encode_,
    decode_,
)
from .helper import (
    RE_PATTERN_TYPE,
    to_bytes,
    compare_documents,
    is_duckument_type,
    is_integer_type,
    is_numeric_type,
    is_pattern_type,
    re_int_flag_to_str,
    re_str_flags_to_int,

    Counter,
    keep,
    on_err_close,
)
from .bson import init as init_bson


__all__ = [
    "PY3",
    "ConfigParser",
    "abc",
    "zip_longest",
    "integer_types",
    "string_types",
    "text_type",
    "iteritems",
    "unicode_",
    "encode_",
    "decode_",

    "RE_PATTERN_TYPE",
    "to_bytes",
    "compare_documents",
    "is_duckument_type",
    "is_integer_type",
    "is_numeric_type",
    "is_pattern_type",
    "re_int_flag_to_str",
    "re_str_flags_to_int",

    "Counter",
    "keep",
    "on_err_close",

    "init_bson",
]
