
import re
from collections import deque


def _is_array_type(obj):
    return isinstance(obj, (list, FieldValues))


class FieldSetValueError(TypeError):

    def __init__(self, error, code=None, details=None):
        self.__code = code
        self.__details = details
        super(FieldSetValueError, self).__init__(error)

    @property
    def code(self):
        return self.__code

    @property
    def details(self):
        return self.__details


class LoggerMixin(object):
    __slots__ = (
        "field_levels",
        "matched_indexes",
    )

    def matched_index(self, path):
        """Internal method"""
        return self.matched_indexes.get(path.split(".", 1)[0])


class GetterLogger(LoggerMixin):
    __slots__ = (
        "exists",
        "null_or_missing",
        "elem_iter_map",
    )

    def __init__(self, fieldwalker):
        self.field_levels = fieldwalker.log.field_levels
        self.matched_indexes = fieldwalker.log.matched_indexes
        self.exists = None
        self.null_or_missing = None
        self.elem_iter_map = []


class SetterLogger(LoggerMixin):
    __slots__ = (
    )

    def __init__(self, fieldwalker):
        log_ = fieldwalker.log
        for attr in LoggerMixin.__slots__:
            setattr(self, attr, getattr(log_, attr))


class ValueGetter(object):
    """Internal class"""

    def __init__(self):
        self.F_EMBEDDED_IN_ARRAY = False
        self.F_BEEN_IN_ARRAY = False
        self.F_MISSING_IN_ARRAY = False
        self.F_FIELD_NOT_EXISTS = False
        self.F_INDEX_ERROR = False
        self.F_ARRAY_NO_DOC = False

    def get(self, fieldwalker):
        logger = fieldwalker.log
        field_levels = logger.field_levels
        doc = fieldwalker.doc
        doc_type = fieldwalker.doc_type

        fieldwalker.value = FieldValues()
        self.elem_iter_map = logger.elem_iter_map
        # Begin the walk
        for field in field_levels:
            self.field_is_digit = False
            self.array_has_doc = False

            if _is_array_type(doc):
                if len(doc) == 0:
                    logger.exists = False
                    break

                self.field_is_digit = field.isdigit()
                self.preview_array(doc, doc_type)
                if self.field_is_digit:
                    doc = self.walk_array_complex(doc, field)
                else:
                    doc = self.walk_array(field)

            key = int(field) if self.field_is_digit else field
            try:
                doc = doc[key]
            except (KeyError, IndexError, TypeError) as err:
                doc = None
                logger.exists = False
                fieldwalker.value = FieldValues()
                self.error_handler(err.__class__)
                break
            else:
                logger.exists = True
        # End of walk
        self.commit(fieldwalker, doc)

    def error_handler(self, err_cls):
        """Internal method"""
        if err_cls is IndexError:
            self.F_INDEX_ERROR = True
        if err_cls is TypeError:
            if self.F_BEEN_IN_ARRAY and not self.F_MISSING_IN_ARRAY:
                self.F_ARRAY_NO_DOC = True

    def commit(self, fieldwalker, doc):
        """Internal method"""
        value = fieldwalker.value
        if not self.field_is_digit and _is_array_type(doc):
            value._extend_elements(doc)
        value._extend_values(doc)

        if None not in value.elements and not self.F_FIELD_NOT_EXISTS:
            self.F_MISSING_IN_ARRAY = False

        if self.F_MISSING_IN_ARRAY:
            fieldwalker.log.null_or_missing = True
        elif self.F_INDEX_ERROR or self.F_ARRAY_NO_DOC:
            fieldwalker.log.null_or_missing = False
        elif None in value:
            fieldwalker.log.null_or_missing = True

    def preview_array(self, doc, doc_type):
        """Internal method"""
        self.F_BEEN_IN_ARRAY = True
        self.element_walkers = []
        self.num_of_emb_doc = 0

        no_list = True
        for i, elem in enumerate(doc):
            if isinstance(elem, doc_type):
                self.num_of_emb_doc += 1
                self.element_walkers.append((i, FieldWalker(elem, doc_type)))
            if no_list and isinstance(elem, list):
                no_list = False

        self.array_has_doc = bool(self.num_of_emb_doc)
        if self.field_is_digit and self.F_EMBEDDED_IN_ARRAY and no_list:
            self.F_FIELD_NOT_EXISTS = True

    def walk_array(self, field):
        """Internal method
        Walk in to array for embedded documents.
        """
        field_values = FieldValues()
        elem_iter_map_field = []

        for i, emb_fw in self.element_walkers:
            emb_fw.go(field).get()
            if emb_fw.log.exists:
                elem_iter_map_field.append((i, len(emb_fw.value.elements)))
                field_values += emb_fw.value
            else:
                self.F_FIELD_NOT_EXISTS = True

        if len(field_values.arrays) != self.num_of_emb_doc:
            self.F_MISSING_IN_ARRAY = True

        self.elem_iter_map.append(elem_iter_map_field)

        if field_values:
            self.F_EMBEDDED_IN_ARRAY = True
            return {field: field_values}
        else:
            return None

    def walk_array_complex(self, doc, field):
        """Internal method"""
        if self.array_has_doc:
            doc_ = self.walk_array(field)
            if doc_ is not None:
                doc_ = self.add_element_with_digit_field(field, doc, doc_)
                self.field_is_digit = False
                return doc_

        if self.F_EMBEDDED_IN_ARRAY:
            field_values = doc._positional(int(field))
            self.field_is_digit = False
            return {field: field_values} if field_values else None

        return doc

    def add_element_with_digit_field(self, field, doc, doc_):
        """Internal method"""
        index = int(field)
        if len(doc) > index:
            if isinstance(doc, FieldValues):
                doc_[field] += doc._positional(index)
            else:
                doc_[field]._extend_values(doc[index])
        return doc_


def get_matched_index(fieldwalker):
    """Internal method"""
    value = fieldwalker.value
    times = value._iter_times
    elem_iter_map = fieldwalker.log.elem_iter_map
    if len(elem_iter_map) == 0:
        return None if len(value.elements) == 0 else (times - 1)
    else:
        while elem_iter_map:
            for ind, len_ in elem_iter_map.pop():
                if times > len_:
                    times -= len_
                else:
                    times = ind + 1
                    break
        return times - 1


class ValueSetter(object):

    def set(self, fieldwalker, value, operator=None, array_filter=None):
        logger = fieldwalker.log
        doc = fieldwalker.doc
        doc_type = fieldwalker.doc_type
        fields = deque(logger.field_levels[:])
        func = operator or (lambda _, val: val)
        position_type = None
        positioner = None
        identifier = None
        pre_field = None

        while fields:
            field = fields.popleft()
            is_list = isinstance(doc, list)
            if field[:1] == "$":
                if not is_list:
                    raise FieldSetValueError
                if field == "$":
                    position_type = 0
                else:
                    identifier = re.split(r"([\[\]])", field)[2]
                    position_type = 2 if identifier else 1
                positioner = POSITIONERS[position_type]

            if is_list and (field.isdigit() or positioner):
                if field.isdigit():
                    index, length = int(field), len(doc)
                    if index >= length:
                        fill_none = [None for _ in range(index - length)]
                        doc += fill_none + [doc_type()]
                    if len(fields):
                        doc = doc[index]
                    else:
                        doc[index] = func(doc[index], value)
                elif positioner:
                    path = ".".join(fields)
                    positioner(fieldwalker, doc, pre_field, field, path, value,
                               operator, array_filter, identifier)
                    fields.clear()

            elif isinstance(doc, doc_type):
                if len(fields):
                    if field not in doc:
                        doc[field] = doc_type()
                    doc = doc[field]
                else:
                    doc[field] = func(doc.get(field), value)

            else:
                element = {field: doc}
                raise FieldSetValueError("ERROR",
                                         details=(field, element))

            pre_field = field


def positional_operator(fw, doc, pre_field, field, path, value,
                        operator, array_filter, identifier):
    root = fw.log.field_levels[0]
    index = fw.log.matched_index(root)
    if path:
        fieldwalker = FieldWalker(doc[index], fw.doc_type)
        fieldwalker.go(path).set(value, operator, array_filter)
    else:
        func = operator or (lambda _, val: val)
        doc[index] = func(doc[index], value)


def all_positional_operator(fw, doc, pre_field, field, path, value,
                            operator, array_filter, identifier):
    if path:
        for d in doc:
            fieldwalker = FieldWalker(d, fw.doc_type)
            fieldwalker.go(path).set(value, operator, array_filter)
    else:
        func = operator or (lambda _, val: val)
        for i, d in enumerate(doc):
            doc[i] = func(d, value)


def filtered_positional_operator(fw, doc, pre_field, field, path, value,
                                 operator, array_filter, identifier):
    picker = array_filter[identifier](pre_field)
    for i, d in enumerate(doc):
        if picker({pre_field: d}):
            if path:
                fieldwalker = FieldWalker(d, fw.doc_type)
                fieldwalker.go(path).set(value, operator, array_filter)
            else:
                func = operator or (lambda _, val: val)
                doc[i] = func(d, value)


POSITIONERS = (
    positional_operator,
    all_positional_operator,
    filtered_positional_operator,
)


class FieldWalker(object):
    """Document traversal context manager
    """
    __slots__ = (
        "doc",
        "doc_type",
        "value",
        "log",
    )

    def __init__(self, doc, doc_type=None):
        """
        """
        self.doc = doc
        self.doc_type = doc_type or type(doc)
        self.log = LoggerMixin()
        self.value = FieldValues()
        self.log.matched_indexes = {}

    def go(self, path):
        self.log.field_levels = tuple(path.split("."))
        return self

    def get(self):
        """Walk through document and acquire value with given key-path
        """
        self.log = GetterLogger(self)
        ValueGetter().get(self)
        return self

    def set(self, value, by_func=None, pick_with=None):
        self.log = SetterLogger(self)
        ValueSetter().set(self, value, by_func, pick_with)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        logger = self.log
        if isinstance(logger, GetterLogger):
            root = logger.field_levels[0]
            logger.matched_indexes[root] = get_matched_index(self)


class FieldValues(object):
    __slots__ = (
        "elements",
        "arrays",
        "_iter_queue",
        "_iter_times",
    )

    def __init__(self):
        self.elements = []
        self.arrays = []
        self._iter_queue = None
        self._iter_times = 1

    def _merged(self):
        return self.elements + self.arrays

    def __repr__(self):
        return "FieldValues(elements: {}, arrays: {})".format(self.elements,
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
        if isinstance(val, FieldValues):
            self.elements += val.elements
        else:
            self.elements += val

    def _extend_values(self, val):
        if isinstance(val, FieldValues):
            self.arrays += val.arrays
        else:
            if isinstance(val, list):
                self.arrays.append(val)
            else:
                self.elements.append(val)

    def _positional(self, index):
        self.elements = [m_[index] for m_ in self.arrays if len(m_) > index]
        self.arrays = []
        return self
