

from datetime import datetime

from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.regex import Regex
from bson.code import Code

from bson.py3compat import string_type

from .helpers import (
    is_array_type,
    is_mapping_type,
    RE_PATTERN_TYPE,
    re_int_flag_to_str,
)


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


def obj_to_bson_type_id(obj):
    if isinstance(obj, float):
        return BSON_TYPE_ALIAS_ID["double"]
    if isinstance(obj, string_type):
        return BSON_TYPE_ALIAS_ID["string"]
    if is_mapping_type(obj):
        return BSON_TYPE_ALIAS_ID["object"]
    if is_array_type(obj):
        return BSON_TYPE_ALIAS_ID["array"]
    if isinstance(obj, Binary):
        return BSON_TYPE_ALIAS_ID["binData"]
    if isinstance(obj, ObjectId):
        return BSON_TYPE_ALIAS_ID["objectId"]
    if isinstance(obj, bool):
        return BSON_TYPE_ALIAS_ID["bool"]
    if isinstance(obj, datetime):
        return BSON_TYPE_ALIAS_ID["date"]
    if obj is None:
        return BSON_TYPE_ALIAS_ID["null"]
    if isinstance(obj, (RE_PATTERN_TYPE, Regex)):
        return BSON_TYPE_ALIAS_ID["regex"]
    if isinstance(obj, Code) and obj.scope is None:
        return BSON_TYPE_ALIAS_ID["javascript"]
    if isinstance(obj, Code):
        return BSON_TYPE_ALIAS_ID["javascriptWithScope"]
    if isinstance(obj, int):
        return BSON_TYPE_ALIAS_ID["int"]
    if isinstance(obj, Timestamp):
        return BSON_TYPE_ALIAS_ID["timestamp"]
    if isinstance(obj, Int64):
        return BSON_TYPE_ALIAS_ID["long"]
    if isinstance(obj, Decimal128):
        return BSON_TYPE_ALIAS_ID["decimal"]
    if isinstance(obj, MinKey):
        return BSON_TYPE_ALIAS_ID["minKey"]
    if isinstance(obj, MaxKey):
        return BSON_TYPE_ALIAS_ID["maxKey"]

    raise Exception("Unknown data type, this should not happend.")


_decimal128_NaN = Decimal128('NaN')
_decimal128_INF = Decimal128('Infinity')
_decimal128_NaN_ls = (_decimal128_NaN,
                      Decimal128('-NaN'),
                      Decimal128('sNaN'),
                      Decimal128('-sNaN'))


class _cmp_decimal(object):

    __slots__ = ["_dec"]

    def __init__(self, dec):
        self._dec = dec

    def _is_numeric(self, other):
        return isinstance(other, (int, Int64, float, Decimal128, _cmp_decimal))

    def _to_decimal(self, other):
        if isinstance(other, _cmp_decimal):
            other = other._dec
        if not isinstance(other, Decimal128):
            other = Decimal128(str(float(other)))
        return other

    def __repr__(self):
        return "Decimal128({!r})".format(str(self._dec))

    def __eq__(self, other):
        if self._is_numeric(other):
            other = self._to_decimal(other)
            return self._dec.bid == other.bid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        if self._is_numeric(other):
            other = self._to_decimal(other)
            if other == _decimal128_INF or self._dec == _decimal128_NaN:
                return False
            if other == _decimal128_NaN or self._dec == _decimal128_INF:
                return True
            return self._dec.bid > other.bid
        else:
            raise TypeError("'>' not supported between instances of "
                            "'Decimal128' and {}".format(type(other)))

    def __ge__(self, other):
        if self._is_numeric(other):
            if self._dec == _decimal128_INF:
                return True
            if not self.__lt__(other):
                return True
            else:
                return False
        else:
            raise TypeError("'>=' not supported between instances of "
                            "'Decimal128' and {}".format(type(other)))

    def __lt__(self, other):
        if self._is_numeric(other):
            other = self._to_decimal(other)
            if other == _decimal128_INF or self._dec == _decimal128_NaN:
                return True
            if other == _decimal128_NaN or self._dec == _decimal128_INF:
                return False
            return self._dec.bid < other.bid
        else:
            raise TypeError("'<' not supported between instances of "
                            "'Decimal128' and {}".format(type(other)))

    def __le__(self, other):
        if self._is_numeric(other):
            if self._dec == _decimal128_INF:
                return True
            if not self.__gt__(other):
                return True
            else:
                return False
        else:
            raise TypeError("'<=' not supported between instances of "
                            "'Decimal128' and {}".format(type(other)))


class Weighted(tuple):
    """
    """

    def __new__(cls, value):
        return super(Weighted, cls).__new__(cls, gravity(value))


def gravity(value):
    """
    """

    def _dict_parser(dict_doc):
        for key, value in dict_doc.items():
            wgt, value = gravity(value)
            yield (wgt, key, value)

    def _list_parser(list_doc):
        return (gravity(member) for member in list_doc)

    # a short cut for getting weight number,
    # to get rid of lots `if` stetments.
    TYPE_WEIGHT = {
        MinKey: -1,
        # less than None: 0, this scenario handles in
        # ordering phase, not during weighting
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
        bytes: 6,
        ObjectId: 7,
        bool: 8,
        datetime: 9,
        Timestamp: 10,
        # Regex: 11
        # Code without scope: 12
        # Code with scope: 13
        MaxKey: 127
    }

    # get value type weight
    try:
        wgt = TYPE_WEIGHT[type(value)]
    except KeyError:
        if is_mapping_type(value):
            wgt = 4
        elif isinstance(value, Code):  # also an instance of string_type
            wgt = 12 if value.scope is None else 13
        elif isinstance(value, string_type):
            wgt = 3
        elif isinstance(value, (RE_PATTERN_TYPE, Regex)):
            wgt = 11
        else:
            raise TypeError("Not weightable type: {!r}".format(type(value)))

    # gain weight
    if wgt == 4:
        weighted = (wgt, tuple(_dict_parser(value)))

    elif wgt == 5:
        weighted = (wgt, tuple(_list_parser(value)))

    elif wgt == 2 and isinstance(value, Decimal128):
        if value in _decimal128_NaN_ls:
            # MongoDB does not sort them
            value = Decimal128('NaN')
        weighted = (wgt, _cmp_decimal(value))

    elif wgt == 11:
        weighted = (wgt, value.pattern, re_int_flag_to_str(value.flags))

    elif wgt == 13:
        weighted = (wgt, str(value), tuple(_dict_parser(value.scope)))

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

    __slots__ = [
        "doc",
        "__value",
        "__exists",
        "__embedded_in_array",
        "__index_posed",
        "__been_in_array",
        "__array_field_missing",
        "__array_field_short",
        "__array_elem_short",
        "__array_no_docs",
    ]

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

                    if self.__index_posed and self.__embedded_in_array:
                        if not any(is_array_type(ele) for ele in _doc):
                            array_index_pos = False
                else:
                    # Possible quering from an array of documents.
                    _doc = self.__from_array(_doc, field)

            # Is the path ends with index position ?
            self.__index_posed = array_index_pos

            # If the `_doc` is an array (or `FieldValues` type) and containing
            # documents, those documents possible has digit key, for example:
            # [{"1": <value>}, ...]
            if array_index_pos and array_has_doc:
                # Treat index position path as a field of document
                _idpos_as_field_doc = self.__from_array(_doc, field)
                # Append index position result to the document field result
                if _idpos_as_field_doc is not None:
                    if len(_doc) > int(field):
                        if isinstance(_doc, FieldValues):
                            _doc.positional(int(field))
                            _idpos_as_field_doc[field] += _doc
                        else:
                            _idpos_as_field_doc[field].append(_doc[int(field)])
                    _doc = _idpos_as_field_doc
                    array_index_pos = False

            if array_index_pos and self.__embedded_in_array:
                # the `_doc` in here must be `FieldValues` type
                _doc.positional(int(field))
                _doc = {field: _doc}
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

    __slots__ = [
        "__elements",
        "__arrays",
    ]

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

    def __eq__(self, other):
        return self.__merged == other

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

    def positional(self, index):
        self.__elements = [val[index] for val in self.__arrays
                           if len(val) > index]
        self.__arrays = []

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
