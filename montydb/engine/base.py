

from datetime import datetime
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.regex import Regex

from bson.py3compat import string_type

from .helpers import is_array_type, is_mapping_type


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


class Weighted(tuple):
    """
    """

    def __new__(cls, value, reverse=1):
        return super(Weighted, cls).__new__(cls,
                                            gravity(value, reverse))


def gravity(value, reverse):
    """
    """

    def _dict_parser(dict_doc):
        for key, value in dict_doc.items():
            wgt, value = gravity(value, None)
            yield (wgt, key, value)

    def _list_parser(list_doc):
        return (gravity(member, None) for member in list_doc)

    # a short cut for getting weight number,
    # to get rid of lots `if` stetments.
    # may not covering all types
    TYPE_WEIGHT = {
        MinKey: -1,
        # less then None: 0,
        type(None): 1,
        int: 2,
        float: 2,
        Int64: 2,
        Decimal128: 2,
        string_type: 3,
        dict: 4,
        list: 5,
        tuple: 5,
        Binary: 6,
        ObjectId: 7,
        bool: 8,
        datetime: 9,
        Timestamp: 10,
        Regex: 11,
        MaxKey: 127
    }

    # get value type weight
    try:
        wgt = TYPE_WEIGHT[type(value)]
    except KeyError:
        if is_mapping_type(value):
            wgt = 4
        elif isinstance(value, string_type):
            wgt = 3
        else:
            wgt = 1
            value = None

    # gain weight
    if wgt == 4:
        weighted = (wgt, tuple(_dict_parser(value)))

    elif wgt == 5:
        if not len(value):
            # [] less then None
            wgt = 0
            weighted = [(wgt, [])]
        else:
            weighted = (wgt, tuple(_list_parser(value)))

        if reverse is not None:
            # list will firstly compare with other doc by it's smallest
            # or largest member
            weighted = max(weighted[1]) if reverse else min(weighted[1])
    else:
        weighted = (wgt, value)

    return weighted


class FieldWalker(object):
    """Document traversal context manager

    A helper for document field-level operators working inside a field-themed
    `LogicBox` object.

    Before each field-themed `LogicBox` processing any operators within, it
    will first *call* this helper instance and send a document field path to
    find and cache document field's value for operators' later use, after all
    operators been processed, the context will reset.

    Args:
        doc (dict): Document passed from `QueryFilter` instance.

    """

    def __init__(self, doc):
        self.doc = doc
        self.reset()

    def __call__(self, path):
        """For `LogicBox` calling on `with` statement and cache field value"""
        _doc = self.doc
        for field in path.split("."):
            in_array = False
            array_index_pos = False
            array_has_doc = False

            if is_array_type(_doc):
                in_array = True

                if len(_doc) == 0:
                    self.__exists = False
                    break

                if any(is_mapping_type(ele) for ele in _doc):
                    array_has_doc = True

                if field.isdigit():
                    # Currently inside an array type value
                    # with given index path.
                    array_index_pos = True
                else:
                    # Possible quering from an array of documents.
                    _doc = self.__from_array(_doc, field)

            self.__index_posed = array_index_pos

            if array_index_pos and array_has_doc:
                index_as_field_doc = self.__from_array(_doc, field)
                if index_as_field_doc is not None:
                    if len(_doc) > int(field):
                        index_as_field_doc[field] += [_doc[int(field)]]

                    _doc = index_as_field_doc
                    array_index_pos = False

            try:
                _doc = _doc[int(field) if array_index_pos else field]
                self.__exists = True
            except (KeyError, IndexError, TypeError) as e:
                ecls = e.__class__

                self.__array_elem_short = ecls is IndexError

                if ecls is TypeError and in_array:
                    self.__array_no_docs = True

                _doc = None
                self.reset(partial=True)

                break

        # Collect values
        if not array_index_pos and is_array_type(_doc):
            self.__value += _doc
        self.__value.append(_doc)

        return self

    def __from_array(self, _doc, field):
        """
        """
        nest = []
        is_array = []
        for emb_doc in _doc:
            if not is_mapping_type(emb_doc):
                continue
            emb_field = FieldWalker(emb_doc)(field)
            if emb_field.exists:
                nest += emb_field.value
                is_array.append(is_array_type(emb_field.value[-1]))
            else:
                self.__array_field_missing = True

        if nest:
            self.__nested = True
            return {field: nest}
        else:
            return None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.reset()

    @property
    def value(self):
        """
        """
        return self.__value

    @value.setter
    def value(self, val):
        self.__value = val

    @property
    def exists(self):
        """
        """
        return self.__exists

    @property
    def nested(self):
        """
        """
        return self.__nested

    @property
    def index_posed(self):
        """
        """
        return self.__index_posed

    @property
    def array_field_missing(self):
        """
        """
        return self.__array_field_missing

    @property
    def array_elem_short(self):
        """
        """
        return self.__array_elem_short

    @property
    def array_no_docs(self):
        """
        """
        return self.__array_no_docs

    def reset(self, partial=None):
        self.__value = []
        self.__exists = False
        self.__nested = False
        self.__index_posed = False

        if not partial:
            self.__array_field_missing = False
            self.__array_elem_short = False
            self.__array_no_docs = False
