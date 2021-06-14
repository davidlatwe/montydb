import re
from copy import deepcopy
from datetime import datetime
from collections import Mapping

from ..errors import OperationFailure

from .field_walker import FieldWalker
from .weighted import (
    Weighted,
    gravity,
    _cmp_decimal,
)
from ..types import (
    bson,
    RE_PATTERN_TYPE,
    integer_types,
    string_types,
    is_duckument_type,
    is_integer_type,
    is_pattern_type,
    keep,
    compare_documents,
    re_str_flags_to_int,
)


def validate_sort_specifier(sort):
    if not (is_integer_type(sort) and sort in (1, -1)):
        raise OperationFailure("bad sort specification", code=2)


def ordering(fieldwalkers, order, doc_type=None):
    """ """
    total = len(fieldwalkers)
    pre_sect_stack = []

    for path, revr in order.items():
        validate_sort_specifier(revr)

        is_reverse = bool(1 - revr)
        value_stack = []

        for indx, fieldwalker in enumerate(fieldwalkers):
            # get field value
            fieldwalker = FieldWalker(fieldwalker.doc, doc_type).go(path).get()
            values = list(fieldwalker.value.iter_flat())
            if values:
                value = tuple([Weighted(val) for val in values])

                if len(value):
                    # list will firstly compare with other doc by it's smallest
                    # or largest member
                    value = max(value) if is_reverse else min(value)

            elif not fieldwalker.value.is_exists():
                value = Weighted(None)

            else:
                # [] less than None
                value = (0, ())

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
            ordereddoc.append(fieldwalkers[indx])

            # define section
            # maintain the sorting result in next level sorting
            if not value == last_doc:
                sect_id += 1
            sect_stack.append(sect_id)
            last_doc = value

        # save result for next level sorting
        fieldwalkers = ordereddoc
        pre_sect_stack = sect_stack

    return fieldwalkers


class LogicBox(list):
    """A callable operator/logic array for document filtering

    By defining a `theme`, the instance's bool processing logic will behave
    differently when being called, and return different operation result.

    Only operator functions or `LogicBox` type instance are valid member of
    the `LogicBox`.

    Args:
        theme (str): A document field path or logic name ($and/$or/$nor/$not).

    """

    def __init__(self, theme, implicitly=False):
        self.theme = theme
        self.implicitly = implicitly
        self._logic = {
            "$and": self._call_and,
            "$or": self._call_or,
            "$nor": self._call_nor,
            "$not": self._call_not,
            "$elemMatch": self._call_elemMatch,
        }

    @property
    def __name__(self):
        return self.__repr__()

    def __repr__(self):
        """Display `theme` and `LogicBox` or operators content within"""
        content = []
        name = "[{}]"
        if not self.implicitly:
            content = ["LogicBox({})".format(self.theme)]
            name = "{}"

        for i in self[:]:
            if callable(i):
                if hasattr(i, "_keep"):
                    # query ops
                    content.append("{}({})".format(i.__name__, i._keep()))
                else:
                    # LogicBox
                    content.append(i.__name__)
            else:
                content.append(i)

        return name.format(content)[1:-1].replace("'", "")

    def __call__(self, fieldwalker):
        """Recursively calling `LogicBox` or operators content within

        A short-circuit logic sub-structure, passing `FieldWalker` instance.

        Args:
            fieldwalker (FieldWalker): Received from `QueryFilter` instance.

        """
        try:
            return self._logic[self.theme](fieldwalker)
        except KeyError:
            return self._call_field(fieldwalker)

    def _gen(self, fieldwalker):
        return (cond(fieldwalker) for cond in self[:])

    def _call_field(self, fieldwalker):
        """Entering document field context before process"""
        with fieldwalker.go(self.theme).get():
            return all(self._gen(fieldwalker))

    def _call_elemMatch(self, fieldwalker):
        """ """
        with fieldwalker.value as field_value:
            for elem in field_value.iter_elements():
                field_value.change_iter(lambda: iter([elem]))
                if all(self._gen(fieldwalker)):
                    return True

    def _call_and(self, fieldwalker):
        return all(self._gen(fieldwalker))

    def _call_or(self, fieldwalker):
        return any(self._gen(fieldwalker))

    def _call_nor(self, fieldwalker):
        return not any(self._gen(fieldwalker))

    def _call_not(self, fieldwalker):
        return not all(self._gen(fieldwalker))


class QueryFilter(object):
    """Document query filter

    Parsing MongoDB document query language, generate a callable instance for
    documents query filtering.

    Args:
        spec (dict): MongoDB document query language object.

    """

    def __init__(self, spec):

        # Top-level operators, work on top of fields.
        self.pathless_ops = {
            # Logical
            "$and": self.parse_logic("$and"),
            "$nor": self.parse_logic("$nor"),
            "$or": self.parse_logic("$or"),
            # Evaluation
            "$expr": None,
        }

        # Field-level operators, need to work inside a field context.
        self.field_ops = {
            # Logical
            "$not": self._parse_not,
            # Comparison
            "$eq": parse_eq,
            "$gt": parse_gt,
            "$gte": parse_gte,
            "$in": parse_in,
            "$lt": parse_lt,
            "$lte": parse_lte,
            "$ne": parse_ne,
            "$nin": parse_nin,
            # Element
            "$exists": parse_exists,
            "$type": parse_type,
            # Array
            "$all": parse_all,
            "$elemMatch": self._parse_elemMatch,
            "$size": parse_size,
            # Evaluation
            "$jsonSchema": parse_jsonSchema,
            "$mod": parse_mod,
            "$regex": parse_regex,
        }

        # Start parsing query object
        self.conditions = self.parser(spec)
        self.__fieldwalker = None

        # ready to be called.

    def __repr__(self):
        return "QueryFilter({})".format(str(self.conditions))

    def __call__(self, doc, doc_type=None):
        """Recursively calling `LogicBox` or operators content within

        A short-circuit logic structure to determine the document can pass the
        filter or not.

        Args:
            doc (dict): Document received from database.

        """
        self.__fieldwalker = FieldWalker(doc, doc_type)
        return all(cond(self.__fieldwalker) for cond in self.conditions)

    @property
    def fieldwalker(self):
        return self.__fieldwalker

    def parser(self, spec):
        """Top-level parser"""

        # Implementation of implicitly $and operation, fundamental query
        # container.
        logic_box = LogicBox("$and", implicitly=True)

        for path, sub_spec in spec.items():
            if path.startswith("$"):
                try:
                    logic_box.append(self.pathless_ops[path](sub_spec))
                except KeyError:
                    raise OperationFailure(
                        "unknown top level operator: {}".format(path)
                    )
            else:
                logic_box.append(self.subparser(path, sub_spec))

        return logic_box

    def subparser(self, path, sub_spec):
        """Field-level parser"""

        # Implementation of field-level operation container.
        logic_box = LogicBox(path)

        # There are two processing direction in field-level, one is filtering
        # with operators, the other is implicitly value $eq operation.
        # The direction was first defined by the expression value type, if is
        # <dict>, then by *first* key is starts with "$" or not.
        #
        # Example:
        #
        #   {"field.name": {"$ne": 5, "$exists": True}} -> by operators
        #   {"field.name": {"data": 5, "id": 2}}        -> value matching ($eq)
        #
        #   But if something like this, mixing operator and non-operator key.
        #   {"field.name": {"$eq": 5, "id": 2}}
        #
        #   Depend on which key get iter *first*, then this query might:
        #   1) return no document and without any error, or
        #   2) raise an "OperationFailure: unknown operator" error.
        #
        if isinstance(sub_spec, bson.Regex):
            sub_spec = {"$regex": sub_spec}

        if _is_expression_obj(sub_spec):
            # Modify `sub_spec` for $regex and $options
            # before parse to `logic_box`
            if "$regex" in sub_spec:
                sub_spec = _modify_regex_optins(sub_spec)
            elif "$options" in sub_spec:
                raise OperationFailure("$options needs a $regex")

            for op, value in sub_spec.items():
                # Regex can't do $ne directly
                if op == "$ne" and isinstance(value, (RE_PATTERN_TYPE, bson.Regex)):
                    raise OperationFailure("Can't have RegEx as arg to $ne.")
                # is predictable ?
                if op in ("$gt", "$gte", "$lt", "$lte"):
                    if isinstance(value, (RE_PATTERN_TYPE, bson.Regex)):
                        raise OperationFailure(
                            "Can't have RegEx as arg to predicate over "
                            "field {!r}.".format(path)
                        )

                try:
                    logic_box.append(self.field_ops[op](value))
                except KeyError:
                    raise OperationFailure("unknown operator: {}".format(op))
        else:
            logic_box.append(parse_eq(sub_spec))

        return logic_box

    def parse_logic(self, theme):
        """Logical operator parser (un-themed)"""

        def _parse_logic(sub_spec):
            """Themed logical operator"""
            if not isinstance(sub_spec, list):
                raise OperationFailure("{} must be an array".format(theme))

            logic_box = LogicBox(theme)

            for cond in sub_spec:
                if not is_duckument_type(cond):
                    raise OperationFailure(
                        "$or/$and/$nor entries need to be full objects"
                    )

                logic_box.append(self.parser(cond))
            return logic_box

        return _parse_logic

    def _parse_not(self, sub_spec):
        # $not logic only available in field-level
        if isinstance(sub_spec, (RE_PATTERN_TYPE, bson.Regex)):
            return self.subparser("$not", {"$regex": sub_spec})

        elif is_duckument_type(sub_spec):
            for op in sub_spec:
                if op not in self.field_ops:
                    raise OperationFailure("unknown operator: {}".format(op))
                if op == "$regex":
                    raise OperationFailure("$not cannot have a regex")

            return self.subparser("$not", sub_spec)

        else:
            raise OperationFailure("$not needs a regex or a document")

    def _parse_elemMatch(self, sub_spec):
        # $elemMatch only available in field-level
        if not is_duckument_type(sub_spec):
            raise OperationFailure("$elemMatch needs an Object")

        for op in sub_spec:
            if op in self.field_ops:
                return self.subparser("$elemMatch", sub_spec)
            elif not op.startswith("$") or op in self.pathless_ops:
                return parse_elemMatch(sub_spec)
            else:
                raise OperationFailure("unknown operator: {}".format(op))


def _is_expression_obj(sub_spec):
    return is_duckument_type(sub_spec) and next(iter(sub_spec)).startswith("$")


# Only for preserving `int` type flags to bypass
# internal "flags must be string" type check
class _FALG(object):
    def __init__(self, int_flags):
        self.retrieve = int_flags

    __slots__ = ("retrieve",)


def _regex_options_(regex_flag, opt_flag):
    pass


def _regex_options_v42(regex_flag, opt_flag):
    if regex_flag and opt_flag:
        raise OperationFailure("options set in both $regex and $options")


_regex_options_check = _regex_options_v42


def _modify_regex_optins(sub_spec):
    """Merging $regex and $options values in query document

    Besides string type value, field $regex accept `bson.Regex` and
    `re._pattern_type` in pymongo. For re.flags and $options, based
    on the key order of dict, seconded will override the first, if
    they both exists in the query document.
    """
    new_sub_spec = None
    _re = None
    regex_flags = ""
    opt_flags = ""
    flags = ""

    for key, val in sub_spec.items():
        if key == "$options":
            opt_flags = val
            flags = opt_flags
        if key == "$regex" and isinstance(val, (RE_PATTERN_TYPE, bson.Regex)):
            regex_flags = _FALG(val.flags)
            flags = regex_flags
            # We will deepcopy `sub_spec` later for merging "$regex" and
            # "$options" to query parser, but we can't deepcopy regex
            # object, so move it to somewhere else and retrieve it later.
            _re = sub_spec["$regex"]
            sub_spec["$regex"] = None

    _regex_options_check(regex_flags, opt_flags)

    new_sub_spec = deepcopy(sub_spec)
    new_sub_spec["$regex"] = {
        "pattern": _re.pattern if _re else sub_spec["$regex"],
        "flags": flags,
    }

    # (monument): This is edge case, and only MongoDB 4.0 don't fail the
    #   operation.
    #
    # if (MONTY_MONGO_COMPAT_36
    #         and "#" in new_sub_spec["$regex"]["pattern"].rsplit("\n")[-1]):
    #     # (NOTE) davidlatwe:
    #     #   if pound(#) char exists in $regex string value and not ends with
    #     #   newline(\n), Mongo raise error. (but the message seems incomplete)
    #     raise OperationFailure("Regular expression is invalid: missing )")

    if _re:
        # Put `re._pattern_type` or `Regex` object back.
        sub_spec["$regex"] = _re

    if "$options" in new_sub_spec:
        # Remove $options, Monty can't digest it
        del new_sub_spec["$options"]

    return new_sub_spec


"""
Field-level Query Operators
- Comparison
"""


def _is_comparable_ver4(val, qry):
    return gravity(val, weight_only=True) == gravity(
        qry, weight_only=True
    ) or isinstance(qry, (bson.MinKey, bson.MaxKey))


def _is_comparable_ver3(val, qry):
    return gravity(val, weight_only=True) == gravity(qry, weight_only=True)


_is_comparable = _is_comparable_ver4


def _eq_match(fieldwalker, query):
    """ """
    if is_duckument_type(query):
        for val in fieldwalker.value:
            if is_duckument_type(val):
                if compare_documents(query, val):
                    return True

    else:
        if query is None:
            return fieldwalker.value.null_or_missing()

        if isinstance(query, bson.Decimal128):
            query = _cmp_decimal(query)

        for val in fieldwalker.value:
            if isinstance(val, bson.Decimal128):
                val = _cmp_decimal(val)

            if val == query and _is_comparable(val, query):
                return True


def parse_eq(query):
    @keep(query)
    def _eq(fieldwalker):
        return _eq_match(fieldwalker, query)

    return _eq


def parse_ne(query):
    @keep(query)
    def _ne(fieldwalker):
        return not _eq_match(fieldwalker, query)

    return _ne


def parse_gt(query):
    @keep(query)
    def _gt(fieldwalker):
        for value in fieldwalker.value:
            if _is_comparable(value, query):
                if query in bson.decimal128_NaN_ls:
                    return False
                if Weighted(value) > Weighted(query):
                    return True
            elif isinstance(query, (bson.MinKey, bson.MaxKey)):
                return True

    return _gt


def parse_gte(query):
    @keep(query)
    def _gte(fieldwalker):
        for value in fieldwalker.value:
            if _is_comparable(value, query):
                if query in bson.decimal128_NaN_ls:
                    return True if value in bson.decimal128_NaN_ls else False
                if query == bson.decimal128_INF and not value == bson.decimal128_INF:
                    return False
                if Weighted(value) >= Weighted(query):
                    return True
            elif isinstance(query, (bson.MinKey, bson.MaxKey)):
                return True

    return _gte


def parse_lt(query):
    @keep(query)
    def _lt(fieldwalker):
        for value in fieldwalker.value:
            if _is_comparable(value, query):
                if value in bson.decimal128_NaN_ls:
                    return False
                if Weighted(value) < Weighted(query):
                    return True
            elif isinstance(query, (bson.MinKey, bson.MaxKey)):
                return True

    return _lt


def parse_lte(query):
    _dec_NaN_INF_ls = list(bson.decimal128_NaN_ls) + [bson.decimal128_INF]

    @keep(query)
    def _lte(fieldwalker):
        for value in fieldwalker.value:
            if _is_comparable(value, query):
                if query in bson.decimal128_NaN_ls:
                    return True if value in bson.decimal128_NaN_ls else False
                if query == bson.decimal128_INF and value in bson.decimal128_NaN_ls:
                    return False
                if query not in _dec_NaN_INF_ls and value in _dec_NaN_INF_ls:
                    return False
                if Weighted(value) <= Weighted(query):
                    return True
            elif isinstance(query, (bson.MinKey, bson.MaxKey)):
                return True

    return _lte


def _in_match(fieldwalker, query):
    """Helper function for $in and $nin"""
    q_regex = []
    q_value = []
    for q in query:
        if is_pattern_type(q):
            q_regex.append(q)
        elif isinstance(q, bson.Regex):
            try:
                q_regex.append(q.try_compile())
            except re.error as e:
                raise OperationFailure("Regular expression is invalid: {}".format(e))
        else:
            q_value.append(q)

    for q in q_value:
        if _eq_match(fieldwalker, q):
            return True

    for q in q_regex:
        for value in fieldwalker.value:
            if isinstance(value, string_types) and q.search(value):
                return True


def parse_in(query):
    if not isinstance(query, list):
        raise OperationFailure("$in needs an array")

    if any(_is_expression_obj(q) for q in query):
        raise OperationFailure("cannot nest $ under $in")

    @keep(query)
    def _in(fieldwalker):
        return _in_match(fieldwalker, query)

    return _in


def parse_nin(query):
    if not isinstance(query, list):
        raise OperationFailure("$nin needs an array")

    if any(_is_expression_obj(q) for q in query):
        raise OperationFailure("cannot nest $ under $nin")

    @keep(query)
    def _nin(fieldwalker):
        return not _in_match(fieldwalker, query)

    return _nin


"""
Field-level Query Operators
- Array
"""


def parse_all(query):

    field_op_ls = set(QueryFilter({}).field_ops.keys())
    field_op_ls.remove("$eq")
    field_op_ls.remove("$not")

    if not isinstance(query, list):
        raise OperationFailure("$all needs an array")

    if is_duckument_type(query[0]) and next(iter(query[0])) == "$elemMatch":
        go_match = True
        for q in query:
            if not (is_duckument_type(q) and next(iter(q)) == "$elemMatch"):
                raise OperationFailure("$all/$elemMatch has to be consistent")
    else:
        go_match = False
        for q in query:
            if is_duckument_type(q) and next(iter(q)) in field_op_ls:
                raise OperationFailure("no $ expressions in $all")

    @keep(query)
    def _all(fieldwalker):
        if go_match:
            for q in query:
                queryfilter = QueryFilter(q["$elemMatch"])
                doc_type = fieldwalker.doc_type
                for value in fieldwalker.value.iter_arrays():
                    if not any(queryfilter(v, doc_type) for v in value):
                        return False
            return True
        else:
            for q in query:
                if q not in fieldwalker.value:
                    return False
            return True

    return _all


def parse_elemMatch(query):
    # (NOTE) $elemMatch in MontyDB may require document input to proceed
    #        further filter error.OperationFailure check, here we put one
    #        fake doc {}
    QueryFilter(query)({})

    @keep(query)
    def _elemMatch(fieldwalker):
        queryfilter = QueryFilter(query)
        doc_type = fieldwalker.doc_type
        for value in fieldwalker.value.iter_arrays():
            for v in value:
                if queryfilter(v, doc_type):
                    return True

    return _elemMatch


def parse_size(query):
    if isinstance(query, float):
        raise OperationFailure("$size must be a whole number")
    if not isinstance(query, int):
        raise OperationFailure("$size needs a number")

    @keep(query)
    def _size(fieldwalker):
        for value in fieldwalker.value.iter_arrays():
            if len(value) == query:
                return True

    return _size


"""
Field-level Query Operators
- Element
"""


def parse_exists(query):
    @keep(query)
    def _exists(fieldwalker):
        return fieldwalker.value.is_exists() == bool(query)

    return _exists


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
    "maxKey": 127,
}


_BSON_TYPE_ID = tuple(BSON_TYPE_ALIAS_ID.values())


def obj_to_bson_type_id(obj):

    BSON_TYPE_ID = {
        float: 1,
        # string: 2,
        bson.SON: 3,
        dict: 3,
        list: 4,
        tuple: 4,
        bson.Binary: 5,
        # bytes: 5,
        # undefined (Deprecated)
        bson.ObjectId: 7,
        bool: 8,
        datetime: 9,
        type(None): 10,
        bson.Regex: 11,
        RE_PATTERN_TYPE: 11,
        # dbPointer (Deprecated)
        # javascript: 13,
        # symbol (Deprecated)
        # javascriptWithScope: 15,
        int: 16,
        bson.Timestamp: 17,
        bson.Int64: 18,
        bson.Decimal128: 19,
        bson.MinKey: -1,
        bson.MaxKey: 127,
    }

    try:
        type_id = BSON_TYPE_ID[type(obj)]
    except KeyError:
        if isinstance(obj, bson.Code):  # also an instance of string_types
            type_id = 13 if obj.scope is None else 15
        elif isinstance(obj, string_types):
            type_id = 2
        elif isinstance(obj, bytes):
            type_id = 5
        elif isinstance(obj, Mapping):
            type_id = 3
        else:
            type_id = None
    finally:
        if type_id is None:
            raise TypeError("Unknown data type: {!r}".format(type(obj)))

        return type_id


def parse_type(query):
    def get_bson_type_id_set(values):
        return set([obj_to_bson_type_id(v) for v in values])

    def str_type_to_int(query):
        if len(query) == 0:
            raise OperationFailure("$type must match at least one type")

        int_types = []
        for q in query:
            if isinstance(q, string_types):
                try:
                    int_types.append(BSON_TYPE_ALIAS_ID[q])
                except KeyError:
                    raise OperationFailure("Unknown type name alias: {}".format(q))
            elif isinstance(q, int):
                if q not in _BSON_TYPE_ID:
                    raise OperationFailure("Invalid numerical type code: {}".format(q))
                int_types.append(q)
            else:
                raise OperationFailure(
                    "type must be represented as a number or a string"
                )
        return int_types

    if not isinstance(query, list):
        query = set(str_type_to_int([query]))
    query = set(str_type_to_int(query))

    @keep(query)
    def _type(fieldwalker):
        if fieldwalker.value.is_exists():
            bids = get_bson_type_id_set(fieldwalker.value)
            return bids.intersection(query)

    return _type


"""
Field-level Query Operators
- Evaluation
"""


def parse_regex(query):
    if isinstance(query, bson.Regex):
        q = query.try_compile()
    else:
        if not isinstance(query["pattern"], string_types):
            raise OperationFailure("$regex has to be a string")
        if not isinstance(query["flags"], (string_types, _FALG)):
            raise OperationFailure("$options has to be a string")

        if isinstance(query["flags"], _FALG):
            flags = query["flags"].retrieve
        else:
            flags = re_str_flags_to_int(query["flags"])

        q = re.compile(query["pattern"], flags)

    @keep(query)
    def _regex(fieldwalker):
        for value in fieldwalker.value:
            if isinstance(value, (string_types, bytes)) and q.search(value):
                return True

    return _regex


def parse_mod(query):
    if not isinstance(query, list):
        raise OperationFailure("malformed mod, needs to be an array")
    if len(query) < 2:
        raise OperationFailure("malformed mod, not enough elements")
    if len(query) > 2:
        raise OperationFailure("malformed mod, too many elements")

    divisor = query[0]
    remainder = query[1]

    num_types = (integer_types, float, bson.Decimal128)

    if not isinstance(divisor, num_types):
        raise OperationFailure("malformed mod, divisor not a number")
    if not isinstance(remainder, num_types):
        remainder = 0

    if isinstance(divisor, bson.Decimal128):
        divisor = divisor.to_decimal()
    if isinstance(remainder, bson.Decimal128):
        remainder = remainder.to_decimal()

    def mod_scan(field_value, query):
        for value in field_value:
            if isinstance(value, bool) or not isinstance(value, num_types):
                continue
            if isinstance(value, bson.Decimal128):
                value = value.to_decimal()
            if int(value % divisor) == int(remainder):
                return True
        return False

    @keep(query)
    def _mod(fieldwalker):
        field_value = fieldwalker.value
        if mod_scan(field_value, query):
            return True

    return _mod


def parse_jsonSchema(query):
    @keep(query)
    def _jsonSchema(fieldwalker):
        pass

    return _jsonSchema
