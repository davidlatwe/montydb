
from bson.py3compat import abc, string_type

from datetime import datetime
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.regex import Regex


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
        if isinstance(value, abc.Mapping):
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

            if isinstance(_doc, (list, tuple)):
                if field.isdigit():
                    # Currently inside an array type value
                    # with given index path.
                    in_array = True
                    field = int(field)
                else:
                    # Possible quering from an array of documents.
                    nest = []
                    for emb_doc in _doc:
                        if not isinstance(emb_doc, abc.Mapping):
                            continue
                        emb_field = FieldWalker(emb_doc)(field)
                        if emb_field.exists:
                            nest += emb_field.value
                    if nest:
                        self.nested = True
                        _doc = {field: nest}
                    else:
                        _doc = None
            try:
                _doc = _doc[field]
                self.exists = True
            except (KeyError, IndexError, TypeError):
                _doc = None
                self.reset()
                break

        if not in_array and isinstance(_doc, (list, tuple)):
            self.value += _doc
        self.value.append(_doc)

        return self

    def reset(self):
        self.value = []
        self.exists = False
        self.nested = False


class Stack(object):

    def __init__(self, specifier):
        self.spec = specifier
        self.drop()

    def push(self, doc):
        for path, revr in self.spec:
            is_reverse = bool(1 - revr)
            value = Weighted(FieldWalker(doc)(path).value, is_reverse)
            self.heaps.setdefault(path, []).append(value)
        self.total += 1

    def drop(self):
        self.total = 0
        self.heaps = {}

    def sort(self):
        pre_sect_stack = []

        for path, revr in self.spec:
            is_reverse = bool(1 - revr)
            value_stack = []

            for indx, value in enumerate(self.heaps[path]):
                # read previous section
                pre_sect = pre_sect_stack[indx] if pre_sect_stack else 0
                # inverse if in reverse mode
                pre_sect = (self.total - pre_sect) if is_reverse else pre_sect
                indx = (self.total - indx) if is_reverse else indx

                value_stack.append((pre_sect, value, indx))

            # sort docs
            value_stack.sort(reverse=is_reverse)

            ordereddoc = []
            sect_stack = []
            sect_id = -1
            last_doc = None
            for _, value, indx in value_stack:
                # restore if in reverse mode
                indx = (self.total - indx) if is_reverse else indx
                ordereddoc.append(self.heaps[path][indx])

                # define section
                # maintain the sorting result in next level sorting
                if not value == last_doc:
                    sect_id += 1
                sect_stack.append(sect_id)
                last_doc = value

            # save result for next level sorting
            _documents = ordereddoc
            pre_sect_stack = sect_stack

        documents = _documents

        return documents  # should return index ?


def ordering(documents, specifier):
    """
    """
    _documents = documents
    total = len(_documents)
    pre_sect_stack = []

    for path, revr in specifier:
        is_reverse = bool(1 - revr)
        value_stack = []

        for indx, doc in enumerate(_documents):
            # get field value
            value = Weighted(FieldWalker(doc)(path).value, is_reverse)
            # read previous section
            pre_sect = pre_sect_stack[indx] if pre_sect_stack else 0
            # inverse if in reverse mode
            pre_sect = (total - pre_sect) if is_reverse else pre_sect
            indx = (total - indx) if is_reverse else indx

            value_stack.append((pre_sect, value, indx))

        # sort docs
        value_stack.sort(reverse=is_reverse)

        ordereddoc = []
        sect_stack = []
        sect_id = -1
        last_doc = None
        for _, value, indx in value_stack:
            # restore if in reverse mode
            indx = (total - indx) if is_reverse else indx
            ordereddoc.append(_documents[indx])

            # define section
            # maintain the sorting result in next level sorting
            if not value == last_doc:
                sect_id += 1
            sect_stack.append(sect_id)
            last_doc = value

        # save result for next level sorting
        _documents = ordereddoc
        pre_sect_stack = sect_stack

    documents = _documents

    return documents
