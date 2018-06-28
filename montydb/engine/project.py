
from bson.py3compat import string_type

from ..errors import OperationFailure
from .queries import QueryFilter
from .helpers import (
    is_mapping_type,
    is_array_type,
)


def _is_include(val):
    """
    [] and "" will be `True` as well
    """
    return bool(is_array_type(val) or isinstance(val, string_type) or val)


def _is_positional_match(conditions, match_field):
    """
    @conditions `.queries.LogicBox`
    """
    theme = conditions.theme
    if theme.startswith("$"):
        for con in conditions:
            if _is_positional_match(con, match_field):
                return True
        return False
    else:
        if not theme:
            return False
        return match_field.split(".", 1)[0] == theme.split(".", 1)[0]


def _perr_doc(val):
    """
    For pretty error msg, same as Mongo
    """
    v_lis = []
    for _k, _v in val.items():
        if isinstance(_v, string_type):
            v_lis.append("{0}: \"{1}\"".format(_k, _v))
        else:
            if is_mapping_type(_v):
                _v = _perr_doc(_v)
            if is_array_type(_v):
                _ = []
                for v in _v:
                    _.append(_perr_doc(v))
                _v = "[ " + ", ".join(_) + " ]"
            v_lis.append("{0}: {1}".format(_k, _v))
    return "{ " + ", ".join(v_lis) + " }"


class Projector(object):
    """
    """

    ARRAY_OP_NORMAL = 0
    ARRAY_OP_POSITIONAL = 1
    ARRAY_OP_ELEM_MATCH = 2

    def __init__(self, spec, qfilter):
        self.proj_with_id = True
        self.include_flag = None
        self.regular_field = []
        self.array_field = {}

        self.parser(spec, qfilter)

    def __call__(self, fieldwalker):
        """
        """
        if not self.proj_with_id:
            del fieldwalker.doc["_id"]

        for field_path in self.array_field:
            self.array_field[field_path](fieldwalker)

        if self.include_flag:
            self.regular_field += list(self.array_field.keys())
            self.inclusion(fieldwalker, self.regular_field)
        else:
            self.exclusion(fieldwalker, self.regular_field)

    def parser(self, spec, qfilter):
        """
        """
        self.array_op_type = self.ARRAY_OP_NORMAL

        for key, val in spec.items():
            # Parsing options
            if is_mapping_type(val):
                if not len(val) == 1:
                    _v = _perr_doc(val)
                    raise OperationFailure(">1 field in obj: {}".format(_v))

                # Array field options
                sub_k, sub_v = next(iter(val.items()))
                if sub_k == "$slice":
                    if isinstance(sub_v, int):
                        if sub_v >= 0:
                            slicing = slice(sub_v)
                        else:
                            slicing = slice(sub_v, None)
                    elif is_array_type(sub_v):
                        if not len(sub_v) == 2:
                            raise OperationFailure("$slice array wrong size")
                        if sub_v[1] <= 0:
                            raise OperationFailure(
                                "$slice limit must be positive")
                        slicing = slice(sub_v[0], sub_v[0] + sub_v[1])
                    else:
                        raise OperationFailure(
                            "$slice only supports numbers and [skip, limit] "
                            "arrays")

                    self.array_field[key] = self.parse_slice(key, slicing)

                elif sub_k == "$elemMatch":
                    if not is_mapping_type(sub_v):
                        raise OperationFailure("elemMatch: Invalid argument, "
                                               "object required.")
                    if self.array_op_type == self.ARRAY_OP_POSITIONAL:
                        raise OperationFailure("Cannot specify positional "
                                               "operator and $elemMatch.")
                    if "." in key:
                        raise OperationFailure(
                            "Cannot use $elemMatch projection on a nested "
                            "field.")

                    self.array_op_type = self.ARRAY_OP_ELEM_MATCH

                    self.array_field[key] = self.parse_elemMatch(key, sub_v)

                elif sub_k == "$meta":
                    # Currently Not supported.
                    raise NotImplementedError("Monty currently not support "
                                              "$meta in projection.")

                else:
                    _v = _perr_doc(val)
                    raise OperationFailure(
                        "Unsupported projection option: "
                        "{0}: {1}".format(key, _v))

            elif key == "_id" and not _is_include(val):
                self.proj_with_id = False

            else:
                # Normal field options, include or exclude.
                flag = _is_include(val)
                if self.include_flag is None:
                    self.include_flag = flag
                else:
                    if not self.include_flag == flag:
                        raise OperationFailure(
                            "Projection cannot have a mix of inclusion and "
                            "exclusion.")

                self.regular_field.append(key)

            # Is positional ?
            bad_ops = [".$ref", ".$id", ".$db"]
            if ".$" in key and not any(ops in key for ops in bad_ops):
                # Validate the positional op.
                if not _is_include(val):
                    raise OperationFailure(
                        "Cannot exclude array elements with the positional "
                        "operator.")
                if self.array_op_type == self.ARRAY_OP_POSITIONAL:
                    raise OperationFailure(
                        "Cannot specify more than one positional proj. "
                        "per query.")
                if self.array_op_type == self.ARRAY_OP_ELEM_MATCH:
                    raise OperationFailure(
                        "Cannot specify positional operator and $elemMatch.")
                if ".$" in key.split(".$", 1)[-1]:
                    raise OperationFailure(
                        "Positional projection '{}' contains the positional "
                        "operator more than once.".format(key))

                path = key.split(".$", 1)[0]
                conditions = qfilter.conditions
                if not _is_positional_match(conditions, path):
                    raise OperationFailure(
                        "Positional projection '{}' does not match the query "
                        "document.".format(key))

                self.array_op_type = self.ARRAY_OP_POSITIONAL

                self.array_field[path] = self.parse_positional(path)

        if self.include_flag is None:
            self.include_flag = False

    def parse_slice(self, field_path, slicing):
        def _slice(fieldwalker):
            if "$" in field_path:
                return

            if "." in field_path:
                fore_path, key = field_path.rsplit(".", 1)
                if fieldwalker.go(fore_path).get().value.exists:
                    for emb_doc in fieldwalker.value:
                        if key not in emb_doc:
                            continue
                        if is_array_type(emb_doc[key]):
                            emb_doc[key] = emb_doc[key][slicing]
            else:
                doc = fieldwalker.doc
                if field_path in doc:
                    if is_array_type(doc[field_path]):
                        doc[field_path] = doc[field_path][slicing]

        return _slice

    def parse_elemMatch(self, field_path, sub_v):
        qfilter_ = QueryFilter(sub_v)

        def _elemMatch(fieldwalker):
            doc = fieldwalker.doc
            has_match = False
            if field_path in doc and is_array_type(doc[field_path]):
                for emb_doc in doc[field_path]:
                    if qfilter_(emb_doc):
                        doc[field_path] = [emb_doc]
                        has_match = True
                        break
            if not has_match:
                del fieldwalker.doc[field_path]

            if not self.include_flag:
                self.inclusion(fieldwalker, [field_path])

        return _elemMatch

    def parse_positional(self, field_path):
        def _positional(fieldwalker):
            matched_index = fieldwalker.log.matched_index(field_path)
            value = {}
            fieldwalker.go(field_path).get()
            elements = fieldwalker.value.elements
            match = True
            paths = field_path.split(".")
            paths.reverse()
            doc = fieldwalker.doc
            path = ""
            while paths:
                path = paths.pop()
                if is_array_type(doc[path]):
                    if matched_index is None:
                        match = False
                    elif matched_index >= len(doc[path]):
                        match = False
                    else:
                        if len(elements) == 0:
                            raise OperationFailure(
                                "Executor error during find command: "
                                "BadValue: positional operator ({}.$) "
                                "requires corresponding field in query "
                                "specifier".format(field_path))
                        value = [doc[path][matched_index]]
                    break
                if not len(paths):
                    value = {}
                    break
                doc = doc[path]
            if match:
                doc[path] = value
            else:
                del doc[path]

        return _positional

    def drop_doc(self, fieldwalker, key):
        if fieldwalker.value.exists:
            for emb_doc in fieldwalker.value:
                if key in emb_doc:
                    del emb_doc[key]

    def inclusion(self, fieldwalker, include_field, fore_path=""):
        if fore_path:
            key_list = []
            for val in fieldwalker.value:
                if is_mapping_type(val):
                    key_list += list(val.keys())
            key_list = list(set(key_list))
        else:
            key_list = list(fieldwalker.doc.keys())

        if "_id" in key_list:
            key_list.remove("_id")

        for key in key_list:
            current_path = fore_path + key

            if current_path in include_field:
                # skip included field
                continue

            drop = True
            for field_path in include_field:
                if field_path.startswith(current_path):
                    drop = False
                    break

            if drop:
                if fore_path:
                    fieldwalker.go(fore_path[:-1]).get()
                    self.drop_doc(fieldwalker, key)
                else:
                    if key in fieldwalker.doc:
                        del fieldwalker.doc[key]
            else:
                fore_path = current_path + "."
                fieldwalker.go(current_path).get()
                self.inclusion(fieldwalker, include_field, fore_path)

    def exclusion(self, fieldwalker, exclude_field):
        for field_path in exclude_field:
            if "." in field_path:
                fore_path, key = field_path.rsplit(".", 1)
                fieldwalker.go(fore_path).get()
                self.drop_doc(fieldwalker, key)
            else:
                if field_path in fieldwalker.doc:
                    del fieldwalker.doc[field_path]
