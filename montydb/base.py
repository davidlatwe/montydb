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


from collections import (
    OrderedDict
)
from collections.abc import MutableMapping
from .types import (
    abc,
    iteritems,
    integer_types,
    string_types,
    bson,
)

ASCENDING = 1
DESCENDING = -1


def validate_is_document_type(option, value):
    """Validate the type of method arguments that expect a MongoDB document."""
    if not isinstance(value, (MutableMapping, bson.RawBSONDocument)):
        raise TypeError("%s must be an instance of dict, bson.son.SON, "
                        "bson.raw_bson.RawBSONDocument, or "
                        "a type that inherits from "
                        "collections.MutableMapping" % (option,))


def validate_boolean(option, value):
    """Validates that 'value' is True or False."""
    if isinstance(value, bool):
        return value
    raise TypeError("%s must be True or False" % (option,))


def validate_list(option, value):
    """Validates that 'value' is a list."""
    if not isinstance(value, list):
        raise TypeError("%s must be a list" % (option,))
    return value


def validate_list_or_none(option, value):
    """Validates that 'value' is a list or None."""
    if value is None:
        return value
    return validate_list(option, value)


def validate_is_mapping(option, value):
    """Validate the type of method arguments that expect a document."""
    if not isinstance(value, abc.Mapping):
        raise TypeError("%s must be an instance of dict, bson.son.SON, or "
                        "other type that inherits from "
                        "collections.Mapping" % (option,))


def validate_ok_for_update(update):
    """Validate an update document."""
    validate_is_mapping("update", update)
    # Update can not be {}
    if not update:
        raise ValueError('update only works with $ operators')
    first = next(iter(update))
    if not first.startswith('$'):
        raise ValueError('update only works with $ operators')


def validate_ok_for_replace(replacement):
    """Validate a replacement document."""
    validate_is_mapping("replacement", replacement)
    # Replacement can be {}
    if replacement and not isinstance(replacement, bson.RawBSONDocument):
        first = next(iter(replacement))
        if first.startswith('$'):
            raise ValueError('replacement can not include $ operators')


def _fields_list_to_dict(fields, option_name):
    """Takes a sequence of field names and returns a matching dictionary.
    ["a", "b"] becomes {"a": 1, "b": 1}
    and
    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    if isinstance(fields, abc.Mapping):
        return fields

    if isinstance(fields, (abc.Sequence, abc.Set)):
        if not all(isinstance(field, string_types) for field in fields):
            raise TypeError("%s must be a list of key names, each an "
                            "instance of %s" % (option_name,
                                                string_types.__name__))
        return dict.fromkeys(fields, 1)

    raise TypeError("%s must be a mapping or "
                    "list of key names" % (option_name,))


def _index_list(key_or_list, direction=None):
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, string_types):
            return [(key_or_list, ASCENDING)]
        elif not isinstance(key_or_list, (list, tuple)):
            raise TypeError("if no direction is specified, "
                            "key_or_list must be an instance of list")
        return key_or_list


def _index_document(index_list):
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if isinstance(index_list, abc.Mapping):
        raise TypeError("passing a dict to sort/create_index/hint is not "
                        "allowed - use a list of tuples instead. did you "
                        "mean %r?" % list(iteritems(index_list)))
    elif not isinstance(index_list, (list, tuple)):
        raise TypeError("must use a list of (key, direction) pairs, "
                        "not: " + repr(index_list))
    if not len(index_list):
        raise ValueError("key_or_list must not be the empty list")

    index = OrderedDict()
    for (key, value) in index_list:
        if not isinstance(key, string_types):
            raise TypeError("first item in each key pair must be a string")
        if not isinstance(value, (string_types, int, abc.Mapping)):
            raise TypeError("second item in each key pair must be 1, -1, "
                            "'2d', 'geoHaystack', or another valid MongoDB "
                            "index specifier.")
        index[key] = value
    return index


class WriteConcern(object):
    """MontyWriteConcern
    """

    __slots__ = ("_document")

    def __init__(self, wtimeout=None):
        self._document = {}

        if wtimeout is not None:
            if not isinstance(wtimeout, integer_types):
                raise TypeError("wtimeout must be an integer")
            self._document["wtimeout"] = wtimeout

    @property
    def document(self):
        """The document representation of this write concern.
        """
        return self._document.copy()

    def __repr__(self):
        return ("MontyWriteConcern({})".format(
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
    return WriteConcern(wtimeout)


class ClientOptions(object):
    """ClientOptions"""

    def __init__(self, options, storage_wconcern=None):
        self.__options = options
        self.__codec_options = bson.parse_codec_options(options)

        if storage_wconcern is not None:
            self.__write_concern = storage_wconcern
        else:
            self.__write_concern = _parse_write_concern(options)

    @property
    def _options(self):
        """The original options used to create this ClientOptions."""
        return self.__options  # pragma: no cover

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

        if not isinstance(codec_options, bson.CodecOptions):
            raise TypeError("codec_options must be an "  # pragma: no cover
                            "instance of bson.codec_options.CodecOptions")
        self.__codec_options = codec_options

        if not isinstance(write_concern, WriteConcern):
            raise TypeError(f"write_concern must be an "  # pragma: no cover
                            f"instance of montydb.base.WriteConcern. "
                            f"Got {type(write_concern)}")
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
