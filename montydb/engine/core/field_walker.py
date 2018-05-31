
from collections import deque, OrderedDict

from ..helpers import (
    is_array_type,
    is_mapping_type,
)


def is_array_type_(doc):
    return is_array_type(doc) or isinstance(doc, _FieldValues)


class FieldWalker(object):
    """Document traversal context manager
    """

    __slots__ = [
        "doc",
        "matched_indexes",

        "__value",
        "__exists",
        "__embedded_in_array",
        "__index_posed",
        "__been_in_array",

        # FLAGS_FOR_NONE_QUERYING
        "__NQF_docs_field_missing_in_array",
        "__NQF_array_field_not_exists_in_all_elements",
        "__NQF_out_of_array_index",
        "__NQF_no_docs_in_array",

        "__elem_iter_map",
        "__query_path",
    ]

    def __init__(self, doc):
        """
        Args:
            doc (dict): Document object
        """
        self.doc = doc
        self.matched_indexes = {}
        self.reset()

    def __call__(self, path):
        """Walk through document and acquire value with given key-path

        Args:
            path (string): Document field path
        """
        doc_ = self.doc
        self.reset()

        self.__query_path = path
        for field in path.split("."):

            array_index_pos = False
            array_has_doc = False
            if is_array_type_(doc_):
                if len(doc_) == 0:
                    self.__exists = False
                    break

                self.__been_in_array = True
                array_has_doc = any(is_mapping_type(e_) for e_ in doc_)
                array_index_pos = field.isdigit()

                if array_index_pos:
                    if self.__index_posed and self.__embedded_in_array:
                        array_index_pos = any(is_array_type(e_) for e_ in doc_)
                else:
                    doc_ = self.__walk_array(doc_, field)

            # Is the path ends with index position ?
            self.__index_posed = array_index_pos

            # If the `doc_` is an array (or `_FieldValues` type) and containing
            # documents, those documents possible has numeric string key,
            # for example: [{"1": <value>}, ...]
            if array_index_pos and array_has_doc:
                # Index position path As a Field of `doc_`
                iaf_doc_ = self.__walk_array(doc_, field)
                # Append index position result to the document field result
                if iaf_doc_ is not None:
                    if len(doc_) > int(field):  # Make sure index in range
                        if isinstance(doc_, _FieldValues):
                            iaf_doc_[field] += doc_.positional(int(field))
                        else:
                            iaf_doc_[field].append(doc_[int(field)])

                    doc_ = iaf_doc_
                    array_index_pos = False

            if array_index_pos and self.__embedded_in_array:
                # the `doc_` in here must be `_FieldValues` type
                doc_ = {field: doc_.positional(int(field))}
                array_index_pos = False

            try:
                # Try getting value with key(field) or index.
                doc_ = doc_[int(field) if array_index_pos else field]
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
                doc_ = None
                self.reset(partial=True)
                break

        """End of walk"""

        # Collecting values
        if not array_index_pos and is_array_type_(doc_):
            # Extend `fieldValues.elements` with an array field value from
            # a single document or from multiple documents inside an array.
            self.__value.extend(doc_)
        # Append to `fieldValues.arrays`, but if `doc_` is not array type,
        # will be append to `fieldValues.elements`.
        self.__value.append(doc_)

        # FLAGS_FOR_NONE_QUERYING:
        #   Correcting flag after value been collected.
        #       Confirm all documents inside the array have no such field,
        #       instead of missing field in some of the documents.
        if (None not in self.__value.elements and
                not self.__NQF_array_field_not_exists_in_all_elements):
            self.__NQF_docs_field_missing_in_array = False

        return self

    def __walk_array(self, doc_, field):
        """Walk in to array for embedded documents.
        """
        field_values = _FieldValues()
        num_of_emb_doc = 0
        self.__elem_iter_map[field] = OrderedDict()

        for i, emb_doc in enumerate(doc_):
            if not is_mapping_type(emb_doc):
                continue
            num_of_emb_doc += 1

            emb_field = FieldWalker(emb_doc)(field)
            if emb_field.exists:
                self.__elem_iter_map[field][i] = len(emb_field.value.elements)
                field_values += emb_field.value
            else:
                # FLAGS_FOR_NONE_QUERYING:
                #   field not exists in all elements.
                self.__NQF_array_field_not_exists_in_all_elements = True

        if len(field_values.arrays) != num_of_emb_doc:
            # FLAGS_FOR_NONE_QUERYING:
            #   Possible missing field in some documents.
            #       Using `field_values.arrays` length to compare is not
            #       accurate, but will correcting the result after all value
            #       been collected.
            self.__NQF_docs_field_missing_in_array = True

        if field_values:
            self.__embedded_in_array = True
            return {field: field_values}
        else:
            return None

    def reset(self, partial=None):
        """Rest all, or keeping some flags for internal use.
        """
        self.__value = _FieldValues()
        self.__exists = False
        self.__embedded_in_array = False
        self.__index_posed = False
        self.__elem_iter_map = OrderedDict()
        self.__query_path = ""

        if not partial:
            self.__been_in_array = False
            self.__NQF_docs_field_missing_in_array = False
            self.__NQF_array_field_not_exists_in_all_elements = False
            self.__NQF_out_of_array_index = False
            self.__NQF_no_docs_in_array = False

    def __get_matched_index(self):
        times = self.__value.iter_times
        if len(self.__elem_iter_map) == 0:
            return None if len(self.__value.elements) == 0 else (times - 1)
        else:
            while len(self.__elem_iter_map):
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
        """An instance of `_FieldValues`, hold the result of the query."""
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
        return self.matched_indexes.get(path.split(".", 1)[0])


class _FieldValues(object):

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

    __nonzero__ = __bool__

    def __getitem__(self, index):
        return self.__elements[index]

    def __iadd__(self, val):
        self.__elements += val.elements
        self.__arrays += val.arrays
        return self

    def extend(self, val):
        if isinstance(val, _FieldValues):
            self.__elements += val.elements
        else:
            self.__elements += val

    def append(self, val):
        if isinstance(val, _FieldValues):
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

        return self

    @property
    def elements(self):
        return self.__elements

    @elements.setter
    def elements(self, val):
        """Args: val (list)"""
        self.__elements = val

    @property
    def arrays(self):
        return self.__arrays

    @arrays.setter
    def arrays(self, val):
        """Args: val (list)"""
        self.__arrays = val

    @property
    def iter_times(self):
        return self.__iter_times
