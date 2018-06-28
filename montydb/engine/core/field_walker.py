
from collections import deque


def _is_array_type(obj):
    return isinstance(obj, (list, FieldValues))


class FieldWriteError(TypeError):

    def __init__(self, error, code=None):
        self.__code = code
        super(FieldWriteError, self).__init__(error)

    @property
    def code(self):
        return self.__code


class BaseLogger(object):
    __slots__ = (
        "field_levels",
        "matched_indexes",
        "path_root",
        "transaction_queue",
    )

    def __init__(self):
        self.matched_indexes = {}

    def matched_index(self, path):
        """Internal method"""
        return self.matched_indexes.get(path.split(".", 1)[0])


class GetterLogger(BaseLogger):
    __slots__ = (
        "elem_iter_map",
    )

    def __init__(self, fieldwalker):
        log_ = fieldwalker.log
        self.field_levels = log_.field_levels
        self.matched_indexes = log_.matched_indexes
        self.transaction_queue = getattr(log_, "transaction_queue", [])
        self.path_root = self.field_levels[0]
        self.elem_iter_map = []


class SetterLogger(BaseLogger):
    __slots__ = (
        "itered_path",
        "modified_path",
    )

    def __init__(self, fieldwalker=None):
        if fieldwalker is None:
            return
        log_ = fieldwalker.log
        for attr in BaseLogger.__slots__:
            setattr(self, attr, getattr(log_, attr, None))
        self.modified_path = getattr(log_, "modified_path", [])
        self.itered_path = getattr(log_, "itered_path", [])
        self.transaction_queue = getattr(log_, "transaction_queue", [])

    def copy(self):
        logger = SetterLogger()
        logger.matched_indexes = self.matched_indexes
        logger.path_root = self.field_levels[0]
        logger.modified_path = self.modified_path
        logger.itered_path = self.itered_path
        logger.transaction_queue = self.transaction_queue
        return logger


class DropperLogger(SetterLogger):
    __slots__ = ()


class FieldGetter(object):
    """Internal class"""

    def __init__(self):
        self.F_EMBEDDED_IN_ARRAY = False
        self.F_BEEN_IN_ARRAY = False
        self.F_MISSING_IN_ARRAY = False
        self.F_FIELD_NOT_EXISTS = False
        self.F_INDEX_ERROR = False
        self.F_ARRAY_NO_DOC = False

    def run(self, fieldwalker):
        self.logger = fieldwalker.log
        field_levels = self.logger.field_levels
        doc = fieldwalker.doc
        doc_type = fieldwalker.doc_type

        fieldwalker.value = FieldValues()
        # Begin the walk
        for field in field_levels:
            self.field_is_digit = False
            self.array_has_doc = False

            if _is_array_type(doc):
                if len(doc) == 0:
                    fieldwalker.value.exists = False
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
                fieldwalker.value.exists = False
                fieldwalker.value = FieldValues()
                self.error_handler(err.__class__)
                break
            else:
                fieldwalker.value.exists = True
        # End of walk
        self.report(fieldwalker, doc)

    def error_handler(self, err_cls):
        """Internal method"""
        if err_cls is IndexError:
            self.F_INDEX_ERROR = True
        if err_cls is TypeError:
            if self.F_BEEN_IN_ARRAY and not self.F_MISSING_IN_ARRAY:
                self.F_ARRAY_NO_DOC = True

    def report(self, fieldwalker, doc):
        """Internal method"""
        value = fieldwalker.value
        if not self.field_is_digit and _is_array_type(doc):
            value._extend_elements(doc)
        value._extend_values(doc)

        if None not in value.elements and not self.F_FIELD_NOT_EXISTS:
            self.F_MISSING_IN_ARRAY = False

        if self.F_MISSING_IN_ARRAY:
            value.null_or_missing = True
        elif self.F_INDEX_ERROR or self.F_ARRAY_NO_DOC:
            value.null_or_missing = False
        elif None in value:
            value.null_or_missing = True

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

        for i, emb_fw in self.element_walkers:
            emb_fw.go(field).get()
            if emb_fw.value.exists:
                len_ = len(emb_fw.value.elements)
                is_list = bool(emb_fw.value.arrays)
                self.logger.elem_iter_map.append((i, len_, is_list))
                field_values += emb_fw.value
            else:
                self.F_FIELD_NOT_EXISTS = True

        if len(field_values.arrays) != self.num_of_emb_doc:
            self.F_MISSING_IN_ARRAY = True

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
            index = int(field)
            field_values = doc._positional(index)

            positioned_map = []
            for ind, len_, is_list in self.logger.elem_iter_map:
                if is_list and len_ > index:
                    positioned_map.append((ind, 1, is_list))

            self.logger.elem_iter_map = positioned_map
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
        for ind, len_, _ in elem_iter_map:
            if times > len_:
                times -= len_
            else:
                times = ind + 1
                break
        return times - 1


class FieldSetter(object):

    def __init__(self):
        pass

    def run(self, fieldwalker, value, operator=None, array_filter=None):
        self.value = value
        self.modifier = operator or (lambda o, val, f: val)
        self.array_filter = array_filter
        self.log = fieldwalker.log
        self.doc_type = fieldwalker.doc_type

        doc = fieldwalker.doc
        fields = deque(self.log.field_levels[:])
        pre_field = None

        while fields:
            field = fields.popleft()
            field_is_digit, is_list, transaction_positioner, identifier = (
                self.eval_status(doc, field, pre_field))

            if is_list and (field_is_digit or transaction_positioner):
                if field_is_digit:
                    index = int(field)
                    if len(fields):
                        doc = self.extend_array(
                            doc, index, self.doc_type())[index]
                    else:
                        doc = self.extend_array(doc, index, None)
                        self.transact(doc, index)
                        return

                elif transaction_positioner:
                    path = ".".join(fields)
                    transaction_positioner(doc, path, pre_field, identifier)
                    return

            elif isinstance(doc, self.doc_type):
                if field == "$":
                    field = str(self.find_matched_index())
                self.check_dollar_prefixed_field(field)
                if len(fields):
                    if field not in doc:
                        doc[field] = self.doc_type()
                    doc = doc[field]
                else:
                    self.transact(doc, field)
                    return

            else:
                self.transaction_failed(doc, field, pre_field)

            pre_field = field
            self.log.itered_path.append(field)

    def transact(self, doc, key):
        self.check_transaction_conflict(key)

        def transaction(k=key, v=self.value):
            try:
                """`doc` could be either list or dict(or other mapping type).
                list `doc` should have content enough element, but dict `doc`
                cannot be sure that the key exists."""
                old = doc[k]
            except KeyError:
                old = None
            new = self.modifier(old, v, str(k))
            if old != new:
                doc[k] = new
                return True
            return False
        self.log.transaction_queue.append(transaction)

    def transaction_failed(self, doc, field, pre_field):
        msg = ("Cannot create field {0!r} in element "
               "{1}".format(field, {pre_field: doc}))
        raise FieldWriteError(msg, code=28)

    def eval_status(self, doc, field, pre_field):
        transaction_positioner = identifier = None
        is_list = isinstance(doc, list)
        if field == "$":
            transaction_positioner = self.walk_aray_positional
        elif field[:2] == "$[":
            identifier = field[2:-1]  # $[identifier]
            self.check_filter_exists(identifier)
            transaction_positioner = self.walk_array_filtered
        else:
            field_is_digit = field.isdigit()

        if transaction_positioner is not None:
            field_is_digit = False
            self.check_array_status(field, is_list, pre_field, doc)

        return field_is_digit, is_list, transaction_positioner, identifier

    def check_transaction_conflict(self, key):
        """Internal method"""
        if key == "_id":
            msg = ("Performing an update on the path '_id' would modify "
                   "the immutable field '_id'")
            raise FieldWriteError(msg, code=66)

        logger = self.log
        path = ".".join(logger.itered_path + [str(key)])

        if path in self.log.modified_path:
            msg = "Update created a conflict at {0!r}".format(path)
            raise FieldWriteError(msg, code=40)

        logger.itered_path = []
        logger.modified_path.append(path)

    def check_dollar_prefixed_field(self, field):
        """Internal method"""
        if field[:1] == "$":
            full_path = ".".join(self.log.field_levels)
            msg = ("The dollar ($) prefixed field {0!r} in {1!r} is not "
                   "valid for storage.".format(field, full_path))
            raise FieldWriteError(msg, code=52)

    def extend_array(self, doc, index, end_element):
        """Internal method"""
        length = len(doc)
        if index >= length:
            fill_none = [None for _ in range(index - length)]
            doc += fill_none + [end_element]
        return doc

    def check_filter_exists(self, identifier):
        """Internal method"""
        if identifier and identifier not in self.array_filter:
            full_path = ".".join(self.log.field_levels)
            msg = ("No array filter found for identifier {0!r} "
                   "in path {1!r}".format(identifier, full_path))
            raise FieldWriteError(msg, code=2)

    def check_array_status(self, field, is_list, pre_field, doc):
        """Internal method"""
        if field != "$" and not is_list:
            if doc:
                msg = ("Cannot apply array updates to non-array "
                       "element {0}: {1}".format(pre_field or 0, doc))
                raise FieldWriteError(msg, code=2)
            else:
                path = ".".join(self.log.itered_path)
                msg = ("The path {0!r} must exist in the document in "
                       "order to apply array updates.".format(path))
                raise FieldWriteError(msg, code=2)

    def make_array_fieldwalker(self, doc, index, path):
        fieldwalker = FieldWalker(doc, self.doc_type)
        self.log.itered_path.append(str(index))
        fieldwalker.log = self.log.copy()
        self.log.itered_path = []
        return fieldwalker.go(path)

    def array_setter(self, fieldwalker):
        """Internal method"""
        fieldwalker.set(self.value, self.modifier, self.array_filter)

    def walk_aray_positional(self, doc, path, *args):
        """Internal method"""
        index = self.find_matched_index()
        if path:
            fieldwalker = self.make_array_fieldwalker(doc[index], index, path)
            self.array_setter(fieldwalker)
        else:
            doc = self.extend_array(doc, index, None)
            self.transact(doc, index)

    def walk_array_filtered(self, doc, path, pre_field, identifier):
        """Internal method"""
        if identifier:
            queryfilter = self.array_filter[identifier](pre_field)
        else:
            queryfilter = (lambda _: True)

        for i, d in enumerate(doc):
            if queryfilter({pre_field: d}):
                if path:
                    fieldwalker = self.make_array_fieldwalker(d, i, path)
                    self.array_setter(fieldwalker)
                else:
                    self.transact(doc, i)

    def find_matched_index(self):
        """Internal method"""
        root = self.log.path_root
        if root is None:
            msg = ("The positional operator did not find the match needed "
                   "from the query.")
            raise FieldWriteError(msg, code=2)
        return self.log.matched_index(root)


class FieldDropper(FieldSetter):

    def run(self, fieldwalker, array_filter=None):
        super(FieldDropper, self).run(
            fieldwalker, None, None, array_filter)

    def transact(self, doc, key):
        self.check_transaction_conflict(key)

        if isinstance(doc, self.doc_type):
            cond = (lambda d, k: k in d)
            drop = (lambda d, k: d.pop(k))
        else:
            cond = (lambda d, k: len(d) > k)
            drop = (lambda d, k: d.__setitem__(k, None))

        def transaction(k=key, cond=cond, drop=drop):
            if cond(doc, k):
                drop(doc, k)
                return True
            return False
        self.log.transaction_queue.append(transaction)

    def transaction_failed(self, *args):
        """Internal method"""
        return False

    def extend_array(self, doc, *args):
        """Internal method"""
        return doc

    def array_setter(self, fieldwalker):
        """Internal method"""
        fieldwalker.drop(self.array_filter)


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
        self.log = BaseLogger()
        self.value = FieldValues()

    def go(self, path):
        self.log.field_levels = tuple(path.split("."))
        return self

    def get(self):
        """Walk through document and acquire value with given key-path
        """
        self.log = GetterLogger(self)
        FieldGetter().run(self)
        return self

    def set(self, value, by_func=None, pick_with=None):
        self.log = SetterLogger(self)
        FieldSetter().run(self, value, by_func, pick_with)

    def drop(self, pick_with=None):
        self.log = DropperLogger(self)
        FieldDropper().run(self, pick_with)

    def commit(self):
        result = []
        for transaction in self.log.transaction_queue:
            result.append(transaction())
        return any(result)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        logger = self.log
        if isinstance(logger, GetterLogger):
            root = logger.field_levels[0]
            logger.matched_indexes[root] = get_matched_index(self)
        else:
            self.commit()


class FieldValues(object):
    __slots__ = (
        "elements",
        "arrays",
        "exists",
        "null_or_missing",
        "_iter_queue",
        "_iter_times",
    )

    def __init__(self):
        self.elements = []
        self.arrays = []
        self._iter_queue = None
        self._iter_times = 1
        self.exists = False
        self.null_or_missing = False

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
