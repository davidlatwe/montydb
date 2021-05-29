
import sys

PY3 = sys.version_info[0] == 3


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

    string_types = basestring,  # noqa
    integer_types = (int, long)  # noqa
    text_type = unicode  # noqa

    def iteritems(d, **kw):
        return d.iteritems(**kw)

    unicode_ = unicode  # noqa

    def encode_(s):
        return s.encode("utf-8")

    def decode_(s):
        return s.decode(_fs_encoding)
    _fs_encoding = sys.getfilesystemencoding()


__all__ = [
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
]
