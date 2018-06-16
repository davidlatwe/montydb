
from collections import deque


def _is_array_type(obj):
    return isinstance(obj, (list, _FieldValues))


class _FieldLogger(object):
    """
    """

    __slots__ = (
        "embedded_in_array",
        "elem_iter_map",
        "query_path",
        "end_field",
        "matched_indexes",
        "index_posed",
        "field_as_index",
        "array_has_doc",

        "F_BEEN_IN_ARRAY",
        "F_MISSING_IN_ARRAY",
        "F_FIELD_NOT_EXISTS",
        "F_INDEX_ERROR",
        "F_ARRAY_NO_DOC",
    )

    def __init__(self):
        self.matched_indexes = {}

    def reset(self, deep):
        self.embedded_in_array = False
        self.index_posed = False
        self.elem_iter_map = []
        self.end_field = None
        if deep:
            self.query_path = ""
            self.F_BEEN_IN_ARRAY = False
            self.F_MISSING_IN_ARRAY = False
            self.F_FIELD_NOT_EXISTS = False
            self.F_INDEX_ERROR = False
            self.F_ARRAY_NO_DOC = False

    def possible_index_as_field(self):
        return self.field_as_index and self.array_has_doc

    def been_in_array(self):
        self.F_BEEN_IN_ARRAY = True

    def field_not_exists(self):
        self.F_FIELD_NOT_EXISTS = True

    def missing_in_array(self):
        self.F_MISSING_IN_ARRAY = True

    def parse_index_error(self):
        self.F_INDEX_ERROR = True

    def parse_type_error(self):
        if self.F_BEEN_IN_ARRAY and not self.F_MISSING_IN_ARRAY:
            self.F_ARRAY_NO_DOC = True

    def confirm_missing(self):
        if not self.F_FIELD_NOT_EXISTS:
            self.F_MISSING_IN_ARRAY = False

    def array_field_missing(self):
        return self.F_MISSING_IN_ARRAY

    def array_status_normal(self):
        return (self.F_INDEX_ERROR or self.F_ARRAY_NO_DOC)


class FieldWalker(object):
    """Document traversal context manager
    """

    __slots__ = (
        "doc",
        "doc_type",
        "value",
        "exists",
        "logger",
    )

    def __init__(self, doc):
        """
        """
        self.doc = doc
        self.doc_type = type(doc)
        self.logger = _FieldLogger()

    def _reset(self, deep=None):
        """Rest all, or keeping some flags for internal use.
        """
        self.value = _FieldValues()
        self.exists = False
        self.logger.reset(deep)

    def _is_doc_type(self, obj):
        return isinstance(obj, self.doc_type)

    def __call__(self, path):
        """Walk through document and acquire value with given key-path
        """
        doc_ = self.doc
        log_ = self.logger
        ref_ = end_field_ = None

        self._reset(deep=True)
        log_.query_path = path

        for field in path.split("."):
            log_.field_as_index = False
            log_.array_has_doc = False

            if _is_array_type(doc_):
                if len(doc_) == 0:
                    self.exists = False
                    break

                log_.been_in_array()
                log_.array_has_doc = any(self._is_doc_type(e_) for e_ in doc_)
                log_.field_as_index = field.isdigit()

                if log_.field_as_index:
                    if log_.index_posed and log_.embedded_in_array:
                        if not any(isinstance(e_, list) for e_ in doc_):
                            log_.field_not_exists()
                else:
                    doc_ = self._walk_array(doc_, field)

            log_.index_posed = log_.field_as_index

            if log_.field_as_index and log_.array_has_doc:
                iaf_doc_ = self._walk_array(doc_, field)
                if iaf_doc_ is not None:
                    if len(doc_) > int(field):  # Make sure index in range
                        if isinstance(doc_, _FieldValues):
                            iaf_doc_[field] += doc_._positional(int(field))
                        else:
                            iaf_doc_[field]._extend_values(doc_[int(field)])

                    doc_ = iaf_doc_
                    log_.field_as_index = False

            if log_.field_as_index and log_.embedded_in_array:
                field_values = doc_._positional(int(field))
                doc_ = {field: field_values} if field_values else None
                log_.field_as_index = False

            ref_ = self._get_ref(doc_, field)
            end_field_ = field
            key = int(field) if log_.field_as_index else field
            try:
                doc_ = doc_[key]
                self.exists = True
            except (KeyError, IndexError, TypeError) as err:
                err_cls = err.__class__

                if err_cls is IndexError:
                    log_.parse_index_error()
                if err_cls is TypeError:
                    log_.parse_type_error()

                doc_ = None
                ref_ = end_field_ = None
                self._reset()
                break

        """End of walk"""

        if not log_.field_as_index and _is_array_type(doc_):
            self.value._extend_elements(doc_)
        self.value._extend_values(doc_)

        self.value._ref = ref_
        log_.end_field = end_field_

        if None not in self.value.elements:
            log_.confirm_missing()

        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        root = self.logger.query_path.split(".", 1)[0]
        self.logger.matched_indexes[root] = self._get_matched_index()
        self._reset()

    def _walk_array(self, doc_, field):
        """Walk in to array for embedded documents.
        """
        field_values = _FieldValues()
        ref_ = []
        num_of_emb_doc = 0
        elem_iter_map_field = []

        for i, emb_doc in enumerate(doc_):
            if not self._is_doc_type(emb_doc):
                continue
            num_of_emb_doc += 1

            emb_field = FieldWalker(emb_doc)(field)
            if emb_field.exists:
                elem_iter_map_field.append((i, len(emb_field.value.elements)))
                field_values += emb_field.value
                ref_.append(emb_field.value._ref)
            else:
                self.logger.field_not_exists()

        if len(field_values.arrays) != num_of_emb_doc:
            self.logger.missing_in_array()

        self.logger.elem_iter_map.append(elem_iter_map_field)

        if field_values:
            self.logger.embedded_in_array = True
            field_values._ref = ref_
            return {field: field_values}
        else:
            return None

    def _get_ref(self, doc_, field):
        if doc_ is not None and self.logger.embedded_in_array:
            if isinstance(doc_, _FieldValues):
                return None
            else:
                return doc_[field]._ref
        else:
            return doc_

    def _get_matched_index(self):
        times = self.value._iter_times
        elem_iter_map = self.logger.elem_iter_map
        if len(elem_iter_map) == 0:
            return None if len(self.value.elements) == 0 else (times - 1)
        else:
            while len(elem_iter_map):
                for ind, len_ in elem_iter_map.pop():
                    if times > len_:
                        times -= len_
                    else:
                        times = ind + 1
                        break
            return times - 1

    def matched_index(self, path):
        """
        """
        return self.logger.matched_indexes.get(path.split(".", 1)[0])

    def missing(self):
        if self.logger.array_field_missing():
            return True
        if self.logger.array_status_normal():
            return False
        return None

    def setval(self, value):
        if self.value._ref is None:
            ref_ = self.doc
            fields = self.logger.query_path.split(".")
            end = fields.pop()
            pre_field = ""
            for field in fields:
                if isinstance(ref_, list) and field.isdigit():
                    ref_ = ref_[int(field)]
                elif self._is_doc_type(ref_):
                    ref_ = ref_.setdefault(field, {})
                else:
                    return (field, pre_field, ref_)
                pre_field = field
            ref_[end] = value
        else:
            ref_ = self.value._ref
            if not self.logger.embedded_in_array:
                ref_ = [ref_]

            for r_ in ref_:
                if isinstance(r_, list):
                    if len(r_) > int(self.logger.end_field):
                        r_[int(self.logger.end_field)] = value
                    else:
                        fill = int(self.logger.end_field) - len(r_)
                        r_ += [None for i in range(fill)] + [value]
                elif self._is_doc_type(r_):
                    r_[self.logger.end_field] = value


class _FieldValues(object):

    __slots__ = (
        "elements",
        "arrays",
        "_iter_queue",
        "_iter_times",
        "_ref",
    )

    def __init__(self):
        self.elements = []
        self.arrays = []
        self._iter_queue = None
        self._iter_times = 1
        self._ref = None

    def _merged(self):
        return self.elements + self.arrays

    def __repr__(self):
        return "_FieldValues(elements: {}, arrays: {})".format(self.elements,
                                                               self.arrays)

    def __next__(self):
        if len(self._iter_queue):
            self._iter_times += 1
            return self._iter_queue.popleft()
        else:
            raise StopIteration

    next = __next__

    def __iter__(self):
        self._iter_times = 0
        self._iter_queue = deque(self._merged())
        return self

    def __len__(self):
        return len(self._merged())

    def __eq__(self, other):
        return self._merged() == other

    def __bool__(self):
        return bool(self.elements or self.arrays)

    __nonzero__ = __bool__

    def __getitem__(self, index):
        return self.elements[index]

    def __iadd__(self, val):
        self.elements += val.elements
        self.arrays += val.arrays
        return self

    def _extend_elements(self, val):
        if isinstance(val, _FieldValues):
            self.elements += val.elements
        else:
            self.elements += val

    def _extend_values(self, val):
        if isinstance(val, _FieldValues):
            self.arrays += val.arrays
        else:
            if isinstance(val, list):
                self.arrays.append(val)
            else:
                self.elements.append(val)

    def _positional(self, index):
        self.elements = [m_[index] for m_ in self.arrays if len(m_) > index]
        self.arrays = []

        self._ref = [next(iter(r.values())) for r in self._ref]

        return self
