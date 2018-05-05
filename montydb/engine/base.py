

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


def is_array_type_(doc):
    return is_array_type(doc) or isinstance(doc, FieldValues)


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
        self.reset()

        for field in path.split("."):

            array_index_pos = False
            array_has_doc = False
            if is_array_type_(_doc):
                self.__been_in_array = True

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

            # Is the path ends with index position ?
            self.__index_posed = array_index_pos

            # If the `_doc` is an array and containing documents, those
            # documents possible has digit key, for example:
            # [{"1": <value>}, ...]
            if array_index_pos and array_has_doc:
                # Treat index opsition path as a field of document
                _if_doc = self.__from_array(_doc, field)
                # Append index opsition result to the document field result
                if _if_doc is not None:
                    if len(_doc) > int(field):
                        _if_doc[field].append(_doc[int(field)])
                    _doc = _if_doc
                    array_index_pos = False

            try:
                # Try getting value with key(field).
                _doc = _doc[int(field) if array_index_pos else field]
                self.__exists = True

            except (KeyError, IndexError, TypeError) as err:
                ecls = err.__class__
                # Raising some flags if conditions match.

                # FLAGS_FOR_NONE_QUERYING:
                #   possible index position out of length of array
                self.__array_elem_short = ecls is IndexError
                # FLAGS_FOR_NONE_QUERYING:
                #   possible not field missing, but the array has no document
                if ecls is TypeError and self.__been_in_array:
                    self.__array_no_docs = not self.__array_field_missing

                # Reset partialy and stop field walking.
                _doc = None
                self.reset(partial=True)
                break

        # Collecting values
        if not array_index_pos and is_array_type_(_doc):
            # Extend `fieldValues.elements` with an array field value from
            # a single document or from multiple documents inside an array.
            self.__value.extend(_doc)
        # Append to `fieldValues.arrays`, but if `_doc` is not array type,
        # will be append to `fieldValues.elements`.
        self.__value.append(_doc)

        # FLAGS_FOR_NONE_QUERYING:
        #   correcting flag after value been collected.
        #   possible all documents inside the array have no such field,
        #   instead of missing field in some of the documents.
        if None not in self.__value.elements and not self.__array_field_short:
            self.__array_field_missing = False

        return self

    def __from_array(self, _doc, field):
        """Querying embedded documents from array.
        """
        field_values = FieldValues()
        emb_doc_count = 0
        for emb_doc in _doc:
            if not is_mapping_type(emb_doc):
                continue
            emb_doc_count += 1
            emb_field = FieldWalker(emb_doc)(field)
            if emb_field.exists:
                field_values += emb_field.value
            else:
                # FLAGS_FOR_NONE_QUERYING:
                #   possible missing field in some documents,
                #   (might be redundant)
                self.__array_field_missing = True
                #   or the field not existing in all documents.
                self.__array_field_short = True

        if not len(field_values.arrays) == emb_doc_count:
            # FLAGS_FOR_NONE_QUERYING:
            #   possible missing field in some documents.
            self.__array_field_missing = True

        if field_values:
            self.__embedded_in_array = True
            return {field: field_values}
        else:
            return None

    def reset(self, partial=None):
        """Rest all, or keeping some flags for internal use.
        """
        self.__value = FieldValues()
        self.__exists = False
        self.__embedded_in_array = False
        self.__index_posed = False

        if not partial:
            self.__been_in_array = False
            self.__array_field_missing = False
            self.__array_field_short = False
            self.__array_elem_short = False
            self.__array_no_docs = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.reset()

    @property
    def value(self):
        """An instance of `FieldValues`, hold the result of the query."""
        return self.__value

    @property
    def exists(self):
        """Is the path of this query exists ?"""
        return self.__exists

    @property
    def embedded_in_array(self):
        """Is the results from documents embedded in array ?"""
        return self.__embedded_in_array

    @property
    def index_posed(self):
        """Is the path of this query ends with index position ?"""
        return self.__index_posed

    @property
    def array_field_missing(self):
        """
        """
        return self.__array_field_missing

    @property
    def array_status_normal(self):
        """
        """
        return self.__array_elem_short or self.__array_no_docs


class FieldValues(object):

    def __init__(self, elements=None, arrays=None):
        self.__elements = elements or []
        self.__arrays = arrays or []

    @property
    def __merged(self):
        return self.__elements + self.__arrays

    def __repr__(self):
        return str(self.__merged)

    def __iter__(self):
        return iter(self.__merged)

    def __len__(self):
        return len(self.__merged)

    def __bool__(self):
        return bool(self.__elements or self.__arrays)

    def __getitem__(self, index):
        return self.__elements[index]

    def __iadd__(self, val):
        if not isinstance(val, FieldValues):
            raise TypeError("Should be a FieldValues type.")
        self.__elements += val.elements
        self.__arrays += val.arrays
        return self

    def extend(self, val):
        if isinstance(val, FieldValues):
            self.__elements += val.elements
        else:
            self.__elements += val

    def append(self, val):
        if isinstance(val, FieldValues):
            self.__arrays += val.arrays
        else:
            if is_array_type(val):
                self.__arrays.append(val)
            else:
                self.__elements.append(val)

    @property
    def elements(self):
        return self.__elements

    @elements.setter
    def elements(self, val):
        if not is_array_type(val):
            raise TypeError("Should be a list, got {}".format(val))
        self.__elements = val

    @property
    def arrays(self):
        return self.__arrays

    @arrays.setter
    def arrays(self, val):
        if not is_array_type(val):
            raise TypeError("Should be a list, got {}".format(val))
        self.__arrays = val
