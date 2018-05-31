
from collections import deque, OrderedDict

from ..helpers import (
    is_array_type,
    is_mapping_type,
)


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
        "matched_indexes",
        "__value",
        "__exists",
        "__embedded_in_array",
        "__index_posed",
        "__been_in_array",
        "__NQF_docs_field_missing_in_array",
        "__array_field_short",
        "__NQF_out_of_array_index",
        "__NQF_no_docs_in_array",
        "__elem_iter_map",
        "__query_path"
    ]

    def __init__(self, doc):
        self.doc = doc
        self.matched_indexes = {}
        self.reset()

    def __call__(self, path):
        """For `LogicBox` calling on `with` statement and cache field value"""
        _doc = self.doc
        self.reset()

        self.__query_path = path
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
                self.__NQF_out_of_array_index = ecls is IndexError
                # FLAGS_FOR_NONE_QUERYING:
                #   possible not field missing, but the array has no document
                if ecls is TypeError and self.__been_in_array:
                    self.__NQF_no_docs_in_array = (
                        not self.__NQF_docs_field_missing_in_array)

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
            self.__NQF_docs_field_missing_in_array = False

        return self

    def __from_array(self, _doc, field):
        """Querying embedded documents from array.
        """
        field_values = FieldValues()
        emb_doc_count = 0
        self.__elem_iter_map[field] = OrderedDict()
        for i, emb_doc in enumerate(_doc):
            if not is_mapping_type(emb_doc):
                continue
            emb_doc_count += 1
            emb_field = FieldWalker(emb_doc)(field)
            if emb_field.exists:
                self.__elem_iter_map[field][i] = len(emb_field.value.elements)
                field_values += emb_field.value
            else:
                # FLAGS_FOR_NONE_QUERYING:
                #   field not exists in all documents.
                self.__array_field_short = True

        if not len(field_values.arrays) == emb_doc_count:
            # FLAGS_FOR_NONE_QUERYING:
            #   possible missing field in some documents.
            self.__NQF_docs_field_missing_in_array = True

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
        self.__elem_iter_map = OrderedDict()
        self.__query_path = ""

        if not partial:
            self.__been_in_array = False
            self.__NQF_docs_field_missing_in_array = False
            self.__array_field_short = False
            self.__NQF_out_of_array_index = False
            self.__NQF_no_docs_in_array = False

    def __get_matched_index(self):
        times = self.__value.iter_times
        if len(self.__elem_iter_map) == 0:
            if not len(self.__value.elements) == 0:
                return times - 1
        else:
            while len(self.__elem_iter_map):
                # (NOTE) OrderedDict popitem from right, SON pop forom left.
                for ind, len_ in self.__elem_iter_map.popitem()[1].items():
                    if times > len_:
                        times -= len_
                    else:
                        times = ind + 1
                        break
            return times - 1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        root = self.__query_path.split(".", 1)[0]
        self.matched_indexes[root] = self.__get_matched_index()
        self.reset(partial=True)

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
        """Documents in array has missing field

        Possible some of document embedded in array has field missing,
        or all document in array has no such field existed.
        """
        return self.__NQF_docs_field_missing_in_array

    @property
    def array_status_normal(self):
        """No missing field but the path not exists

        Query path not existed in document array due to array out of index
        or has no document object in array.
        In this case, the field was not missing because it's possible that
        field did not meant to be there (because no embedded document) or
        array out of index if the path is index position, and since the field
        was not missing, the document won't pop when querying `None`.
        """
        return self.__NQF_out_of_array_index or self.__NQF_no_docs_in_array

    def matched_index(self, path):
        root = path.split(".", 1)[0]
        return self.matched_indexes.get(root)


class FieldValues(object):

    __slots__ = [
        "__elements",
        "__arrays",
        "__iter_queue",
        "__iter_times",
    ]

    def __init__(self, elements=None, arrays=None):
        self.__elements = elements or []
        self.__arrays = arrays or []
        self.__iter_queue = deque()
        self.__iter_times = 1

    @property
    def __merged(self):
        return self.__elements + self.__arrays

    def __repr__(self):
        return str(self.__merged)

    def __next__(self):
        if len(self.__iter_queue):
            self.__iter_times += 1
            return self.__iter_queue.popleft()
        else:
            raise StopIteration

    next = __next__

    def __iter__(self):
        self.__iter_times = 0
        self.__iter_queue = deque(self.__merged)
        return self

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

    @property
    def iter_times(self):
        return self.__iter_times
