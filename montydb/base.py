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

from bson.py3compat import integer_types, abc
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from bson.codec_options import _parse_codec_options


def validate_is_document_type(option, value):
    """Validate the type of method arguments that expect a MongoDB document."""
    if not isinstance(value, (collections.MutableMapping, RawBSONDocument)):
        raise TypeError("%s must be an instance of dict, bson.son.SON, "
                        "bson.raw_bson.RawBSONDocument, or "
                        "a type that inherits from "
                        "collections.MutableMapping" % (option,))


class WriteConcern(object):
    """WriteConcern
    """

    __slots__ = ("__document")

    def __init__(self, wtimeout=None, montywc=None):
        self.__document = {}

        if wtimeout is not None:
            if not isinstance(wtimeout, integer_types):
                raise TypeError("wtimeout must be an integer")
            self.__document["wtimeout"] = wtimeout

        if montywc is not None:
            if not isinstance(montywc, abc.Mapping):
                raise TypeError("montywc must be a dict")
            self.__document["montywc"] = montywc

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
    montywc = options.get('montywc')
    return WriteConcern(wtimeout, montywc)


class ClientOptions(object):
    """ClientOptions"""

    # (NOTE): Maybe this could hand over to MontyConfigure and pass
    #         options to Storage.

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
