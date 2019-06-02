
from bson.py3compat import string_type

from ..errors import OperationFailure
from .queries import QueryFilter
from .core import inclusion, exclusion
from .helpers import (
    is_duckument_type,
)


def _is_include(val):
    """
    [] and "" will be `True` as well
    """
    return bool(isinstance(val, list) or
                isinstance(val, string_type) or
                val)


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
            if is_duckument_type(_v):
                _v = _perr_doc(_v)
            if isinstance(_v, list):
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
        self.matched_index = None
        self.regular_field = []
        self.array_field = {}

        self.parser(spec, qfilter)

    def __call__(self, fieldwalker):
        """
        """
        if fieldwalker.value is not None:
            top_matched = fieldwalker.value.first_matched()
            if top_matched is not None:
                self.matched_index = top_matched.split(".")[0]

        with fieldwalker:

            for path in self.array_field:
                operation = self.array_field[path]
                operation(fieldwalker)

            for path in self.regular_field:
                fieldwalker.go(path).get()

            if self.include_flag:
                if self.proj_with_id:
                    fieldwalker.go("_id").get()
                located = self.array_op_type != self.ARRAY_OP_NORMAL
                projected = inclusion(fieldwalker, located)
            else:
                if not self.proj_with_id:
                    fieldwalker.go("_id").get()
                projected = exclusion(fieldwalker)

            fieldwalker.doc = projected

    def parser(self, spec, qfilter):
        """
        """
        self.array_op_type = self.ARRAY_OP_NORMAL

        for key, val in spec.items():
            # Parsing options
            if is_duckument_type(val):
                if not len(val) == 1:
                    _v = _perr_doc(val)
                    raise OperationFailure(">1 field in obj: {}".format(_v),
                                           code=2)

                # Array field options
                sub_k, sub_v = next(iter(val.items()))
                if sub_k == "$slice":
                    if isinstance(sub_v, int):
                        if sub_v >= 0:
                            slicing = slice(sub_v)
                        else:
                            slicing = slice(sub_v, None)
                    elif isinstance(sub_v, list):
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
                    if not is_duckument_type(sub_v):
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

                if ".$" not in key:
                    self.regular_field.append(key)

            # Is positional ?
            bad_ops = [".$ref", ".$id", ".$db"]
            if ".$" in key and not any(ops in key for ops in bad_ops):
                # Validate the positional op.
                if not _is_include(val):
                    raise OperationFailure(
                        "Cannot exclude array elements with the positional "
                        "operator.", code=2)
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
                        "document.".format(key), code=2)

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
                        if isinstance(emb_doc[key], list):
                            emb_doc[key] = emb_doc[key][slicing]
            else:
                doc = fieldwalker.doc
                if field_path in doc:
                    if isinstance(doc[field_path], list):
                        doc[field_path] = doc[field_path][slicing]

        return _slice

    def parse_elemMatch(self, field_path, sub_v):
        qfilter_ = QueryFilter(sub_v)

        def _elemMatch(fieldwalker):
            doc = fieldwalker.doc
            has_match = False
            if field_path in doc and isinstance(doc[field_path], list):
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
            # Project first array doc's element
            fieldwalker.restart()
            for field in field_path.split("."):
                fieldwalker.step(field).get()
                fieldvalue = fieldwalker.value

                in_array = isinstance(fieldvalue.nodes[0].value, list)
                if in_array:
                    # Reach array field
                    elem_count = len(fieldvalue.nodes[0].value)
                    if int(self.matched_index) >= elem_count:
                        raise OperationFailure(
                            "Executor error during find command: BadValue: "
                            "positional operator element mismatch",
                            code=96)

                    fieldwalker.step(self.matched_index).get()
                    break

        return _positional
