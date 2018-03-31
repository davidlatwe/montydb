# Copyright 2011-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Modifications copyright (C) 2018 davidlatwe
#
# Assembling crucial classes and functions form pymongo module,
# some of them may modified by needs.


import collections

from bson.py3compat import integer_types, string_type, abc
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from bson.codec_options import _parse_codec_options
from .errors import ConfigurationError


def is_mapping_type(obj):
    return isinstance(obj, abc.Mapping)


def is_array_type(obj):
    return isinstance(obj, (list, tuple))


def validate_is_document_type(option, value):
    """Validate the type of method arguments that expect a MongoDB document."""
    if not isinstance(value, (collections.MutableMapping, RawBSONDocument)):
        raise TypeError("%s must be an instance of dict, bson.son.SON, "
                        "bson.raw_bson.RawBSONDocument, or "
                        "a type that inherits from "
                        "collections.MutableMapping" % (option,))


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


BSON_TYPE_ALIAS_ID = {

    "double": 1,
    "string": 2,
    "object": 3,
    "array": 4,
    "binData": 5,
    # undefined (Deprecated)
    "objectId": 7,
    "bool": 8,
    "date": 9,
    "null": 10,
    "regex": 11,
    # dbPointer (Deprecated)
    "javascript": 13,
    # symbol (Deprecated)
    "javascriptWithScope": 15,
    "int": 16,
    "timestamp": 17,
    "long": 18,
    "decimal": 19,
    "minKey": -1,
    "maxKey": 127
}


SQLITE_CONN_OPTIONS = frozenset([
    "OFF",
    "MEMORY",
    "WAL",
    "TRUNCATE",
    "PERSIST",
    "DELETE",
    "EXTRA"
])


class WriteConcern(object):
    """WriteConcern
    """

    __slots__ = ("__document")

    def __init__(self, wtimeout=None, sqlite_jmode=None):
        self.__document = {}

        if wtimeout is not None:
            if not isinstance(wtimeout, integer_types):
                raise TypeError("wtimeout must be an integer")
            self.__document["wtimeout"] = wtimeout

        if sqlite_jmode is not None:
            if not isinstance(sqlite_jmode, string_type):
                raise TypeError("sqlite_jmode must be string")
            if sqlite_jmode not in SQLITE_CONN_OPTIONS:
                raise ConfigurationError(
                    "sqlite_jmode must be one of these options: {}".format(
                        ", ".join(SQLITE_CONN_OPTIONS)))
            self.__document["sqlite_jmode"] = sqlite_jmode

    @property
    def document(self):
        """The document representation of this write concern.
        """
        return self.__document.copy()

    def __repr__(self):
        return ("WriteConcern({})".format(
            ", ".join("%s=%s" % kvt for kvt in self.document.items()),))

    def __eq__(self, other):
        return self.document == other.document

    def __ne__(self, other):
        return self.document != other.document

    def __bool__(self):
        return bool(self.document)


def _parse_write_concern(options):
    """Parse write concern options."""
    wtimeout = options.get('wtimeout')
    sqlite_jmode = options.get('sqlite_jmode')
    return WriteConcern(wtimeout, sqlite_jmode)


class ClientOptions(object):
    """ClientOptions"""

    def __init__(self, options):
        self.__options = options
        self.__codec_options = _parse_codec_options(options)
        self.__write_concern = _parse_write_concern(options)

    @property
    def _options(self):
        """The original options used to create this ClientOptions."""
        return self.__options

    @property
    def codec_options(self):
        """A :class:`~bson.codec_options.CodecOptions` instance."""
        return self.__codec_options

    @property
    def write_concern(self):
        """A :class:`~montydb.base.WriteConcern` instance."""
        return self.__write_concern


class BaseObject(object):
    """A base class that provides attributes and methods common
    to multiple montydb classes.

    SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO MONTYDB.
    """

    def __init__(self, codec_options, write_concern):

        if not isinstance(codec_options, CodecOptions):
            raise TypeError("codec_options must be an instance of "
                            "bson.codec_options.CodecOptions")
        self.__codec_options = codec_options

        if not isinstance(write_concern, WriteConcern):
            raise TypeError("write_concern must be an instance of "
                            "montydb.base.WriteConcern")
        self.__write_concern = write_concern

    @property
    def codec_options(self):
        """Read only access to the :class:`~bson.codec_options.CodecOptions`
        of this instance.
        """
        return self.__codec_options

    @property
    def write_concern(self):
        """Read only access to the :class:`~montydb.base.WriteConcern`
        of this instance.
        """
        return self.__write_concern
