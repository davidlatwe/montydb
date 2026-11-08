from ..errors import OperationFailure
from .queries import QueryFilter
from .field_walker import _no_val
from ..types import (
    string_types,
    is_duckument_type,
)


def _is_include(val):
    """
    [] and "" will be `True` as well
    """
    return bool(isinstance(val, (list, string_types)) or val)


def _is_positional_match(conditions, match_field):
    """
    @conditions `.queries.LogicBox`
    """
    theme = conditions.theme
    if theme.startswith("$"):
        for con in conditions:
            matched = _is_positional_match(con, match_field)
            if matched is not None:
                return matched
        return None
    else:
        if not theme:
            return None

        matched = match_field.split(".", 1)[0]
        if matched == theme.split(".", 1)[0]:
            return matched
        return None


def _has_path_collision(path, parsed_paths):
    path = path.split(".$")[0]
    for parsed in parsed_paths:
        if (
            path == parsed
            or path.startswith(parsed + ".")
            or parsed.startswith(path + ".")
        ):
            return parsed
    return None


def _perr_doc(val):
    """
    For pretty error msg, same as Mongo
    """
    v_lis = []
    for _k, _v in val.items():
        if isinstance(_v, string_types):
            v_lis.append(f'{_k}: "{_v}"')
        else:
            if is_duckument_type(_v):
                _v = _perr_doc(_v)
            if isinstance(_v, list):
                _ = []
                for v in _v:
                    if is_duckument_type(v):
                        _.append(_perr_doc(v))
                    else:
                        _.append(str(v))
                _v = "[ " + ", ".join(_) + " ]"
            v_lis.append(f"{_k}: {_v}")
    return "{ " + ", ".join(v_lis) + " }"


# MongoDB 4.4 rejects some projection keys, see `configure._mongo_compat`
_check_positional_key_ = False
_check_positional_key_v44 = True
_check_positional_key = _check_positional_key_v44

# MongoDB 4.4 rejects projection paths that collide, for example "a" and "a.b"
_check_path_collision_ = False
_check_path_collision_v44 = True
_check_path_collision = _check_path_collision_v44


class Projector:
    """ """

    ARRAY_OP_NORMAL = 0
    ARRAY_OP_POSITIONAL = 1
    ARRAY_OP_ELEM_MATCH = 2

    def __init__(self, spec, qfilter):
        self.proj_with_id = True
        self.include_flag = None
        self.regular_field = []
        self.array_field = {}
        self.slice_field = []
        self.matched = None
        self.position_path = None
        self.positional_rest = ()

        self.parser(spec, qfilter)

        # A projection with only $slice keeps the other fields, but one with
        # $elemMatch or the positional operator is an inclusion.
        only_slice = set(self.array_field) == set(self.slice_field)
        if self.array_field and not self.regular_field and not only_slice:
            self.include_flag = True

    def __call__(self, fieldwalker):
        """ """
        positioned = self.array_op_type == self.ARRAY_OP_POSITIONAL

        if positioned:
            top_matched = fieldwalker.get_matched(self.position_path)
            if top_matched is not None:
                self.matched = top_matched

        with fieldwalker:

            for path in self.array_field:
                operation = self.array_field[path]
                operation(fieldwalker)

            if self.proj_with_id:
                fieldwalker.go("_id").get()
            else:
                fieldwalker.go("_id").drop()

            init_doc = fieldwalker.touched()

            for path in self.regular_field:
                fieldwalker.go(path).get()

            if self.include_flag:
                projected = inclusion(
                    fieldwalker,
                    positioned,
                    self.matched,
                    init_doc,
                    self.positional_rest,
                )
            else:
                projected = exclusion(fieldwalker, init_doc)

            fieldwalker.doc = projected

    def parser(self, spec, qfilter):
        """ """
        self.array_op_type = self.ARRAY_OP_NORMAL

        for key, val in spec.items():
            # Check path collision (mongo-4.4+)
            if _check_path_collision:
                collision = (
                    _has_path_collision(key, self.regular_field)
                    or _has_path_collision(key, self.array_field.keys())
                )
                if collision:
                    remaining = key[len(collision + "."):]
                    if remaining:
                        raise OperationFailure(
                            f"Path collision at {collision} "
                            f"remaining portion {remaining}"
                        )
                    raise OperationFailure(f"Path collision at {key}")

            # Parsing options
            if is_duckument_type(val):
                if len(val) != 1:
                    _v = _perr_doc(val)
                    raise OperationFailure(f">1 field in obj: {_v}", code=2)

                # Array field options
                sub_k, sub_v = next(iter(val.items()))
                if sub_k == "$slice":
                    if isinstance(sub_v, int):
                        slicing = slice(sub_v) if sub_v >= 0 else slice(sub_v, None)
                    elif isinstance(sub_v, list):
                        if not len(sub_v) == 2:
                            raise OperationFailure("$slice array wrong size")
                        if sub_v[1] <= 0:
                            raise OperationFailure("$slice limit must be positive")
                        slicing = slice(sub_v[0], sub_v[0] + sub_v[1])
                    else:
                        raise OperationFailure(
                            "$slice only supports numbers and [skip, limit] arrays"
                        )

                    self.array_field[key] = self.parse_slice(key, slicing)
                    self.slice_field.append(key)

                elif sub_k == "$elemMatch":
                    if not is_duckument_type(sub_v):
                        raise OperationFailure(
                            "elemMatch: Invalid argument, object required."
                        )
                    if self.array_op_type == self.ARRAY_OP_POSITIONAL:
                        raise OperationFailure(
                            "Cannot specify positional operator and $elemMatch."
                        )
                    if "." in key:
                        raise OperationFailure(
                            "Cannot use $elemMatch projection on a nested field.",
                            code=2,
                        )

                    self.array_op_type = self.ARRAY_OP_ELEM_MATCH
                    self.array_field[key] = self.parse_elemMatch(key, sub_v)

                elif sub_k == "$meta":
                    # Currently Not supported.
                    raise NotImplementedError(
                        "Monty currently not support $meta in projection."
                    )

                else:
                    _v = _perr_doc(val)
                    raise OperationFailure(
                        f"Unsupported projection option: {key}: {_v}",
                        code=2,
                    )

            elif key == "_id" and not _is_include(val):
                self.proj_with_id = False

            elif _check_positional_key and key.startswith("$."):
                raise OperationFailure("FieldPath field names may not start with '$'.")

            elif _check_positional_key and key.endswith("."):
                raise OperationFailure("FieldPath must not end with a '.'.")

            else:
                # Normal field options, include or exclude.
                flag = _is_include(val)
                if self.include_flag is None:
                    self.include_flag = flag
                else:
                    if not self.include_flag == flag:
                        raise OperationFailure(
                            "Projection cannot have a mix of inclusion and "
                            "exclusion."
                        )

                if ".$" not in key:
                    self.regular_field.append(key)

            # Is positional ?
            bad_ops = [".$ref", ".$id", ".$db"]
            if ".$" in key and not any(ops in key for ops in bad_ops):
                # Validate the positional op.
                if not _is_include(val):
                    raise OperationFailure(
                        "Cannot exclude array elements with the positional "
                        "operator.",
                        code=2,
                    )
                if self.array_op_type == self.ARRAY_OP_POSITIONAL:
                    raise OperationFailure(
                        "Cannot specify more than one positional proj. per query."
                    )
                if self.array_op_type == self.ARRAY_OP_ELEM_MATCH:
                    raise OperationFailure(
                        "Cannot specify positional operator and $elemMatch."
                    )
                if ".$" in key.split(".$", 1)[-1]:
                    raise OperationFailure(
                        f"Positional projection '{key}' contains the positional "
                        "operator more than once."
                    )

                if _check_positional_key and ".$." in key:
                    raise OperationFailure(
                        "As of 4.4, it's illegal to specify positional "
                        "operator in the middle of a path.Positional "
                        "projection may only be used at the end, for example: "
                        "a.b.$. If the query previously used a form like "
                        "a.b.$.d, remove the parts following the '$' and the "
                        "results will be equivalent.",
                        code=31394,
                    )

                path = key.split(".$", 1)[0]
                conditions = qfilter.conditions
                match_query = _is_positional_match(conditions, path)
                if match_query is None:
                    raise OperationFailure(
                        f"Positional projection '{key}' does not match the query "
                        "document.",
                        code=2,
                    )

                self.position_path = match_query
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
                if fieldwalker.go(fore_path).get().value.is_exists():
                    for emb_doc in fieldwalker.value:
                        if key not in emb_doc:
                            continue
                        if isinstance(emb_doc[key], list):
                            fieldwalker.step(key).set(emb_doc[key][slicing])
            else:
                doc = fieldwalker.doc
                if field_path in doc:
                    if isinstance(doc[field_path], list):
                        sliced = doc[field_path][slicing]
                        fieldwalker.go(field_path).set(sliced)

        return _slice

    def parse_elemMatch(self, field_path, sub_v):
        wrapped_field_op = False
        if next(iter(sub_v)).startswith("$"):
            wrapped_field_op = True
            sub_v = {field_path: sub_v}

        qfilter_ = QueryFilter(sub_v)

        def _elemMatch(fieldwalker):
            doc = fieldwalker.doc
            if field_path in doc and isinstance(doc[field_path], list):
                for index, emb_doc in enumerate(doc[field_path]):
                    query_doc = {field_path: emb_doc} if wrapped_field_op else emb_doc

                    if qfilter_(query_doc):
                        fieldwalker.go(field_path).set([emb_doc])
                        break

        return _elemMatch

    def parse_positional(self, field_path):
        def _positional(fieldwalker):
            # Project first array doc's element
            fieldwalker.restart()
            fields = field_path.split(".")
            for depth, field in enumerate(fields):
                fieldwalker.step(field).get()
                fieldvalue = fieldwalker.value
                node = fieldvalue.nodes[0]

                in_array = isinstance(node.value, list)
                if in_array:
                    # Reach array field
                    elem_count = len(node.value)
                    matched_index = self.matched.split(".")[0]

                    if not self.matched.full_path.count(".") > 1:
                        raise OperationFailure(
                            "Executor error during find command "
                            ":: caused by :: errmsg: "
                            f'"positional operator ({field}.$) requires '
                            'corresponding field in query specifier"',
                            code=2,
                        )

                    if _positional_located_match_error and self.matched.located:
                        raise OperationFailure(
                            "Executor error during find command "
                            ":: caused by :: positional operator '.$' couldn't "
                            "find a matching element in the array",
                            code=51246,
                        )

                    if _positional_mismatch(
                        int(matched_index),
                        elem_count,
                        self.matched.full_path,
                        node.full_path,
                    ):
                        raise OperationFailure(
                            "Executor error during find command "
                            ":: caused by :: errmsg: "
                            '"positional operator element mismatch"',
                            code=2,
                        )

                    fieldwalker.step(matched_index).get()
                    # The path after the array, to project in the element
                    self.positional_rest = fields[depth + 1:]
                    break

        return _positional


def _positional_mismatch_(matched, elem_count, matched_path, node_path):
    return matched >= elem_count and matched_path.startswith(node_path)


def _positional_mismatch_v44(matched, elem_count, matched_path, node_path):
    return matched >= elem_count


_positional_mismatch = _positional_mismatch_v44


# Before 4.4, a document at the end of a positional projection path that has
# no array gives an empty document. From 4.4, it gives the full document.
_positional_empty_leaf_doc_ = True
_positional_empty_leaf_doc_v44 = False
_positional_empty_leaf_doc = _positional_empty_leaf_doc_v44

# From 4.4, a positional projection raises an error if the query uses an
# array index (for example "a.b.1") and not a match in the array.
_positional_located_match_error_ = False
_positional_located_match_error_v44 = True
_positional_located_match_error = _positional_located_match_error_v44


def _project_path(value, fields, doc_type):
    """Project `fields` (a path as list) on `value`, as a MongoDB inclusion

    A number in the path is a field name, not an array index.
    """
    if not fields:
        return value

    if is_duckument_type(value):
        head, rest = fields[0], fields[1:]
        projected = doc_type()
        if head in value:
            sub = _project_path(value[head], rest, doc_type)
            if sub is not _no_val:
                projected[head] = sub
        return projected

    if isinstance(value, list):
        return [
            _project_path(elem, fields, doc_type)
            for elem in value
            if is_duckument_type(elem) or isinstance(elem, list)
        ]

    return _no_val


def _positional_project_rest_(element, rest_path, doc_type):
    return element


def _positional_project_rest_v44(element, rest_path, doc_type):
    return _project_path(element, rest_path, doc_type)


_positional_project_rest = _positional_project_rest_v44


def inclusion(fieldwalker, positioned, matched, init_doc, rest_path=()):
    _doc_type = fieldwalker.doc_type
    located_match = False if matched is None else matched.located

    def _inclusion(node, init_doc=None):
        doc = node.value

        if not node.children:
            if positioned and _positional_empty_leaf_doc and isinstance(doc, _doc_type):
                return _doc_type()
            return doc

        if isinstance(doc, _doc_type):
            new_doc = init_doc or _doc_type()

            for field in doc:
                if field in node.children:
                    child = node[field]
                    value = _inclusion(child)
                    if value is not _no_val:
                        new_doc[field] = value

            return new_doc

        elif isinstance(doc, list):
            new_doc = list()

            if positioned:
                for child in node.children:
                    if not (child.exists and child.located):
                        continue

                    value = _positional_project_rest(child.value, rest_path, _doc_type)

                    if located_match:
                        if isinstance(child.value, _doc_type):
                            new_doc.append(value)
                    else:
                        new_doc.append(value)

                return new_doc or _no_val

            for index, elem in enumerate(doc):
                if isinstance(elem, list):
                    emb_doc = list()
                    new_doc.append(emb_doc)
                    continue

                if not isinstance(elem, _doc_type):
                    continue
                emb_doc = _doc_type()

                for field in elem:
                    embed_field = str(index) + "." + field
                    if embed_field in node.children:
                        child = node[embed_field]
                        if not any(str(gch) for gch in child.children):
                            value = elem[field]
                            emb_doc[field] = value
                        else:
                            value = _inclusion(child)
                            if value is not _no_val:
                                emb_doc[field] = value

                new_doc.append(emb_doc)

            return new_doc

        else:
            if not any(c.exists for c in node.children):
                return _no_val
            return doc

    return _inclusion(fieldwalker.tree.root, init_doc)


def exclusion(fieldwalker, init_doc):
    _doc_type = fieldwalker.doc_type

    def _exclusion(node, init_doc=None):
        doc = node.value

        if isinstance(doc, _doc_type):
            new_doc = init_doc or _doc_type()

            for field in doc:
                if field in node.children:
                    child = node[field]
                    if child.children:
                        value = _exclusion(child)
                        if value is not _no_val:
                            new_doc[field] = value
                else:
                    new_doc[field] = doc[field]

            return new_doc

        elif isinstance(doc, list):
            new_doc = list()

            for index, elem in enumerate(doc):
                if not isinstance(elem, _doc_type):
                    new_doc.append(elem)
                    continue
                emb_doc = _doc_type()

                for field in elem:
                    embed_field = str(index) + "." + field
                    if embed_field in node.children:
                        child = node[embed_field]
                        if child.children and any(str(gch) for gch in child.children):
                            value = _exclusion(child)
                            if value is not _no_val:
                                emb_doc[field] = value
                    else:
                        emb_doc[field] = elem[field]

                new_doc.append(emb_doc)

            return new_doc

        else:
            return doc

    return _exclusion(fieldwalker.tree.root, init_doc)
