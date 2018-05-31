
import re
from copy import deepcopy
from datetime import datetime

from bson.py3compat import integer_types, string_type
from bson.regex import Regex, str_flags_to_int
from bson.son import SON
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.decimal128 import Decimal128
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from bson.binary import Binary
from bson.code import Code
from bson.int64 import Int64

from ..errors import OperationFailure

from .core import (
    gravity,
    FieldWalker,
    Weighted,
    _cmp_decimal,
    _decimal128_INF,
    _decimal128_NaN_ls,
)

from .helpers import (
    PY36,
    RE_PATTERN_TYPE,
    is_mapping_type,
    is_array_type,
    is_pattern_type,
    keep,
)


def ordering(field_walkers, order):
    """
    """
    total = len(field_walkers)
    pre_sect_stack = []

    for path, revr in order.items():
        is_reverse = bool(1 - revr)
        value_stack = []

        for indx, field_walker in enumerate(field_walkers):
            # get field value
            field_walker = FieldWalker(field_walker.doc)(path)
            elements = field_walker.value.elements
            if elements:
                value = tuple([Weighted(val) for val in elements])

                if len(value):
                    # list will firstly compare with other doc by it's smallest
                    # or largest member
                    value = max(value) if is_reverse else min(value)

            elif not field_walker.exists:
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
            ordereddoc.append(field_walkers[indx])

            # define section
            # maintain the sorting result in next level sorting
            if not value == last_doc:
                sect_id += 1
            sect_stack.append(sect_id)
            last_doc = value

        # save result for next level sorting
        field_walkers = ordereddoc
        pre_sect_stack = sect_stack

    return field_walkers


class LogicBox(list):
    """A callable operator/logic array for document filtering

    By defining a `theme`, the instance's bool processing logic will behave
    differently when being called, and return different operation result.

    Only operator functions or `LogicBox` type instance are valid member of
    the `LogicBox`.

    Args:
        theme (str): A document field path or logic name ($and/$or/$nor/$not).

    """

    def __init__(self, theme, implicity=False):
        self.theme = theme
        self.implicity = implicity
        self._logic = {

            "$and": self.__call_and,
            "$or": self.__call_or,
            "$nor": self.__call_nor,
            "$not": self.__call_not,

            "$elemMatch": self.__call_elemMatch,

        }

    @property
    def __name__(self):
        return self.__repr__()

    def __repr__(self):
        """Display `theme` and `LogicBox` or operators content within"""
        content = []
        name = "[{}]"
        if not self.implicity:
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

    def __call__(self, field_walker):
        """Recursively calling `LogicBox` or operators content within

        A short-circuit logic sub-structure, passing `FieldWalker` instance.

        Args:
            field_walker (FieldWalker): Recived from `QueryFilter` instance.

        """
        try:
            return self._logic[self.theme](field_walker)
        except KeyError:
            return self.__call_field(field_walker)

    def __gen(self, field_walker):
        return (cond(field_walker) for cond in self[:])

    def __call_field(self, field_walker):
        """Entering document field context before process"""
        with field_walker(self.theme):
            return all(self.__gen(field_walker))

    def __call_elemMatch(self, field_walker):
        """"""
        field_value_array = field_walker.value.arrays
        for value in field_value_array:
            for v in value:
                field_walker.value.elements = [v]
                field_walker.value.arrays = []
                if all(self.__gen(field_walker)):
                    return True

    def __call_and(self, field_walker):
        return all(self.__gen(field_walker))

    def __call_or(self, field_walker):
        return any(self.__gen(field_walker))

    def __call_nor(self, field_walker):
        return not any(self.__gen(field_walker))

    def __call_not(self, field_walker):
        return not all(self.__gen(field_walker))


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
            "$not": self.parse_not(),

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
            "$elemMatch": self.parse_elemMatch(),
            "$size": parse_size,

            # Evaluation
            "$jsonSchema": parse_jsonSchema,
            "$mod": parse_mod,
            "$regex": parse_regex,

        }

        # Start parsing query object
        self.conditions = self.parser(spec)
        self.__field_walker = None

        # ready to be called.

    def __repr__(self):
        return "QueryFilter({})".format(str(self.conditions))

    def __call__(self, doc):
        """Recursively calling `LogicBox` or operators content within

        A short-circuit logic structure to determine the document can pass the
        filter or not.

        Args:
            doc (dict): Document recived from database.

        """
        self.__field_walker = FieldWalker(doc)
        return all(cond(self.__field_walker) for cond in self.conditions)

    @property
    def field_walker(self):
        return self.__field_walker

    def parser(self, spec):
        """Top-level parser"""

        # Implementation of implicity $and operation, fundamental query
        # container.
        logic_box = LogicBox("$and", implicity=True)

        for path, sub_spec in spec.items():
            if path.startswith("$"):
                try:
                    logic_box.append(self.pathless_ops[path](sub_spec))
                except KeyError:
                    raise OperationFailure(
                        "unknown top level operator: {}".format(path))
            else:
                logic_box.append(self.subparser(path, sub_spec))

        return logic_box

    def subparser(self, path, sub_spec):
        """Field-level parser"""

        # Implementation of field-level operation container.
        logic_box = LogicBox(path)

        # There are two processing direction in field-level, one is filtering
        # with operators, the other is implicity value $eq operation.
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
        if isinstance(sub_spec, Regex):
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
                if op == "$ne" and isinstance(value, (RE_PATTERN_TYPE, Regex)):
                    raise OperationFailure("Can't have RegEx as arg to $ne.")
                # is predictable ?
                if op in ("$gt", "$gte", "$lt", "$lte"):
                    if isinstance(value, (RE_PATTERN_TYPE, Regex)):
                        raise OperationFailure(
                            "Can't have RegEx as arg to predicate over "
                            "field {!r}.".format(path))

                try:
                    logic_box.append(self.field_ops[op](value))
                except KeyError:
                    raise OperationFailure("unknown operator: {}".format(op))
        else:
            logic_box.append(parse_eq(sub_spec))

        return logic_box

    def parse_logic(self, theme):
        """Logical operator parser (un-themed)
        """
        def _parse_logic(sub_spec):
            """Themed logical operator
            """
            if not is_array_type(sub_spec):
                raise OperationFailure("{} must be an array".format(theme))

            logic_box = LogicBox(theme)

            for cond in sub_spec:
                if not is_mapping_type(cond):
                    raise OperationFailure(
                        "$or/$and/$nor entries need to be full objects")

                logic_box.append(self.parser(cond))
            return logic_box

        return _parse_logic

    def parse_not(self):
        """`$not` logical operator
        """
        def _parse_not(sub_spec):
            # $not logic only available in field-level
            if isinstance(sub_spec, (RE_PATTERN_TYPE, Regex)):
                return self.subparser("$not", {"$regex": sub_spec})

            elif is_mapping_type(sub_spec):
                for op in sub_spec:
                    if op not in self.field_ops:
                        raise OperationFailure("unknown operator: "
                                               "{}".format(op))
                    if op == "$regex":
                        raise OperationFailure("$not cannot have a regex")

                return self.subparser("$not", sub_spec)

            else:
                raise OperationFailure("$not needs a regex or a document")

        return _parse_not

    def parse_elemMatch(self):
        """`$elemMatch` field-level operator
        """
        def _parse_elemMatch(sub_spec):
            # $elemMatch only available in field-level
            if not is_mapping_type(sub_spec):
                raise OperationFailure("$elemMatch needs an Object")

            for op in sub_spec:
                if op in self.field_ops:
                    return self.subparser("$elemMatch", sub_spec)
                elif not op.startswith("$") or op in self.pathless_ops:
                    return parse_elemMatch(sub_spec)
                else:
                    raise OperationFailure("unknown operator: {}".format(op))

        return _parse_elemMatch


def _is_expression_obj(sub_spec):
    return (is_mapping_type(sub_spec) and
            next(iter(sub_spec)).startswith("$"))


# Only for preserving `int` type flags to bypass
# internal "flags must be string" type check
class _FALG(object):
    def __init__(self, int_flags):
        self.retrieve = int_flags
    __slots__ = ["retrieve"]


def _modify_regex_optins(sub_spec):
    """Merging $regex and $options values in query document

    Besides string type value, field $regex accept `bson.Regex` and
    `re._pattern_type` in pymongo. For re.flags and $options, based
    on the key order of dict, seconded will override the first, if
    they both exists in the query document.
    """
    new_sub_spec = None
    _re = None
    flags = ""

    for key, val in sub_spec.items():
        if key == "$options":
            flags = val
        if key == "$regex" and isinstance(val, (RE_PATTERN_TYPE, Regex)):
            flags = _FALG(val.flags)
            # We will deepcopy `sub_spec` later for merging "$regex" and
            # "$options" to query parser, but we can't deepcopy regex
            # object, so move it to somewhere else and retrieve it later.
            _re = sub_spec["$regex"]
            sub_spec["$regex"] = None

    new_sub_spec = deepcopy(sub_spec)
    new_sub_spec["$regex"] = {
        "pattern": _re.pattern if _re else sub_spec["$regex"],
        "flags": flags
    }

    if "#" in new_sub_spec["$regex"]["pattern"].rsplit("\n")[-1]:
        # (NOTE) davidlatwe:
        #   if pound(#) char exists in $regex string value and not ends with
        #   newline(\n), Mongo raise error. (but the message seems incomplete)
        raise OperationFailure("Regular expression is invalid: missing )")

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


def _is_comparable(val, qry):
    return gravity(val, weight_only=True) == gravity(qry, weight_only=True)


def _eq_match(field_walker, query):
    """
    Document key order matters

    In PY3.6, `dict` has order-preserving, but two different key ordered
    dictionary are still equal.
    https://docs.python.org/3.6/whatsnew/3.6.html#new-dict-implementation

    ```python 3.6
    >>> print({'a': 1.0, 'b': 1.0})
    {'a': 1.0, 'b': 1.0}
    >>> print({'b': 1.0, 'a': 1.0})
    {'b': 1.0, 'a': 1.0}
    >>> {'a': 1.0, 'b': 1.0} == {'b': 1.0, 'a': 1.0}
    True
    ```

    So before finding equal element in field, have to convert to `SON` first.

    """
    if is_mapping_type(query):
        if PY36 and not isinstance(query, SON):
            query = SON(query)
        for val in field_walker.value:
            if is_mapping_type(val):
                if PY36 and not isinstance(val, SON):
                    val = SON(val)
                if query == val:
                    return True
    else:
        if query is None:
            if field_walker.array_field_missing:
                return True
            if field_walker.array_status_normal:
                return False

        if isinstance(query, Decimal128):
            query = _cmp_decimal(query)

        for val in field_walker.value:
            if isinstance(val, Decimal128):
                val = _cmp_decimal(val)

            if val == query and _is_comparable(val, query):
                return True


def parse_eq(query):
    @keep(query)
    def _eq(field_walker):
        return _eq_match(field_walker, query)

    return _eq


def parse_ne(query):
    @keep(query)
    def _ne(field_walker):
        return not _eq_match(field_walker, query)

    return _ne


def parse_gt(query):
    @keep(query)
    def _gt(field_walker):
        for value in field_walker.value:
            if _is_comparable(value, query):
                if query in _decimal128_NaN_ls:
                    return False
                if Weighted(value) > Weighted(query):
                    return True
            elif isinstance(query, (MinKey, MaxKey)):
                return True

    return _gt


def parse_gte(query):
    @keep(query)
    def _gte(field_walker):
        for value in field_walker.value:
            if _is_comparable(value, query):
                if query in _decimal128_NaN_ls:
                    return True if value in _decimal128_NaN_ls else False
                if query == _decimal128_INF and not value == _decimal128_INF:
                    return False
                if Weighted(value) >= Weighted(query):
                    return True
            elif isinstance(query, (MinKey, MaxKey)):
                return True

    return _gte


def parse_lt(query):
    @keep(query)
    def _lt(field_walker):
        for value in field_walker.value:
            if _is_comparable(value, query):
                if value in _decimal128_NaN_ls:
                    return False
                if Weighted(value) < Weighted(query):
                    return True
            elif isinstance(query, (MinKey, MaxKey)):
                return True

    return _lt


_dec_NaN_INF_ls = tuple(list(_decimal128_NaN_ls) + [_decimal128_INF])


def parse_lte(query):
    @keep(query)
    def _lte(field_walker):
        for value in field_walker.value:
            if _is_comparable(value, query):
                if query in _decimal128_NaN_ls:
                    return True if value in _decimal128_NaN_ls else False
                if query == _decimal128_INF and value in _decimal128_NaN_ls:
                    return False
                if query not in _dec_NaN_INF_ls and value in _dec_NaN_INF_ls:
                    return False
                if Weighted(value) <= Weighted(query):
                    return True
            elif isinstance(query, (MinKey, MaxKey)):
                return True

    return _lte


def _in_match(field_walker, query):
    """Helper function for $in and $nin
    """
    q_regex = []
    q_value = []
    for q in query:
        if is_pattern_type(q):
            q_regex.append(q)
        elif isinstance(q, Regex):
            try:
                q_regex.append(q.try_compile())
            except re.error as e:
                raise OperationFailure("Regular expression is invalid:"
                                       " {}".format(e))
        else:
            q_value.append(q)

    for q in q_value:
        if _eq_match(field_walker, q):
            return True

    for q in q_regex:
        for value in field_walker.value:
            if isinstance(value, string_type) and q.search(value):
                return True


def parse_in(query):
    if not is_array_type(query):
        raise OperationFailure("$in needs an array")

    if any(_is_expression_obj(q) for q in query):
        raise OperationFailure("cannot nest $ under $in")

    @keep(query)
    def _in(field_walker):
        return _in_match(field_walker, query)

    return _in


def parse_nin(query):
    if not is_array_type(query):
        raise OperationFailure("$nin needs an array")

    if any(_is_expression_obj(q) for q in query):
        raise OperationFailure("cannot nest $ under $nin")

    @keep(query)
    def _nin(field_walker):
        return not _in_match(field_walker, query)

    return _nin


"""
Field-level Query Operators
- Array
"""


def parse_all(query):

    field_op_ls = set(QueryFilter({}).field_ops.keys())
    field_op_ls.remove("$eq")
    field_op_ls.remove("$not")

    if not is_array_type(query):
        raise OperationFailure("$all needs an array")

    if is_mapping_type(query[0]) and next(iter(query[0])) == "$elemMatch":
        go_match = True
        for q in query:
            if not (is_mapping_type(q) and next(iter(q)) == "$elemMatch"):
                raise OperationFailure("$all/$elemMatch has to be consistent")
    else:
        go_match = False
        for q in query:
            if is_mapping_type(q) and next(iter(q)) in field_op_ls:
                raise OperationFailure("no $ expressions in $all")

    @keep(query)
    def _all(field_walker):
        if go_match:
            for q in query:
                queryfilter = QueryFilter(q["$elemMatch"])
                for value in field_walker.value.arrays:
                    if not any(queryfilter(v) for v in value):
                        return False
            return True
        else:
            for q in query:
                if q not in field_walker.value:
                    return False
            return True

    return _all


def parse_elemMatch(query):
    # (NOTE) $elemMatch in MontyDB may require document input to proceed
    #        further filter error.OperationFailure check, here we put one
    #        fake doc {}
    QueryFilter(query)({})

    @keep(query)
    def _elemMatch(field_walker):
        queryfilter = QueryFilter(query)
        for value in field_walker.value.arrays:
            for v in value:
                if queryfilter(v):
                    return True

    return _elemMatch


def parse_size(query):
    if isinstance(query, float):
        raise OperationFailure("$size must be a whole number")
    if not isinstance(query, int):
        raise OperationFailure("$size needs a number")

    @keep(query)
    def _size(field_walker):
        for value in field_walker.value.arrays:
            if len(value) == query:
                return True

    return _size


"""
Field-level Query Operators
- Element
"""


def parse_exists(query):
    @keep(query)
    def _exists(field_walker):
        return field_walker.exists == bool(query)

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
    "maxKey": 127
}


_BSON_TYPE_ID = tuple(BSON_TYPE_ALIAS_ID.values())


def obj_to_bson_type_id(obj):

    BSON_TYPE_ID = {
        float: 1,
        # string: 2,
        dict: 3,
        list: 4,
        tuple: 4,
        Binary: 5,
        # bytes: 5,
        # undefined (Deprecated)
        ObjectId: 7,
        bool: 8,
        datetime: 9,
        type(None): 10,
        # regex: 11,
        # dbPointer (Deprecated)
        # javascript: 13,
        # symbol (Deprecated)
        # javascriptWithScope: 15,
        int: 16,
        Timestamp: 17,
        Int64: 18,
        Decimal128: 19,
        MinKey: -1,
        MaxKey: 127
    }

    try:
        type_id = BSON_TYPE_ID[type(obj)]
    except KeyError:
        if is_mapping_type(obj):
            type_id = 3
        elif isinstance(obj, Code):  # also an instance of string_type
            type_id = 13 if obj.scope is None else 15
        elif isinstance(obj, string_type):
            type_id = 2
        elif isinstance(obj, bytes):
            type_id = 5
        elif isinstance(obj, (RE_PATTERN_TYPE, Regex)):
            type_id = 11
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
            if isinstance(q, string_type):
                try:
                    int_types.append(BSON_TYPE_ALIAS_ID[q])
                except KeyError:
                    raise OperationFailure(
                        "Unknown type name alias: {}".format(q))
            elif isinstance(q, int):
                if q not in _BSON_TYPE_ID:
                    raise OperationFailure(
                        "Invalid numerical type code: {}".format(q))
                int_types.append(q)
            else:
                raise OperationFailure(
                    "type must be represented as a number or a string")
        return int_types

    if not is_array_type(query):
        query = set(str_type_to_int([query]))
    query = set(str_type_to_int(query))

    @keep(query)
    def _type(field_walker):
        if field_walker.exists:
            bids = get_bson_type_id_set(field_walker.value)
            return bids.intersection(query)

    return _type


"""
Field-level Query Operators
- Evaluation
"""


def parse_regex(query):
    if isinstance(query, Regex):
        q = query.try_compile()
    else:
        if not isinstance(query["pattern"], string_type):
            raise OperationFailure("$regex has to be a string")
        if not isinstance(query["flags"], (string_type, _FALG)):
            raise OperationFailure("$options has to be a string")

        if isinstance(query["flags"], _FALG):
            flags = query["flags"].retrieve
        else:
            flags = str_flags_to_int(query["flags"])

        q = re.compile(query["pattern"], flags)

    @keep(query)
    def _regex(field_walker):
        for value in field_walker.value:
            if isinstance(value, (string_type, bytes)) and q.search(value):
                return True

    return _regex


def parse_mod(query):
    if not is_array_type(query):
        raise OperationFailure("malformed mod, needs to be an array")
    if len(query) < 2:
        raise OperationFailure("malformed mod, not enough elements")
    if len(query) > 2:
        raise OperationFailure("malformed mod, too many elements")

    divisor = query[0]
    remainder = query[1]

    num_types = (integer_types, float, Decimal128)

    if not isinstance(divisor, num_types):
        raise OperationFailure("malformed mod, divisor not a number")
    if not isinstance(remainder, num_types):
        remainder = 0

    if isinstance(divisor, Decimal128):
        divisor = divisor.to_decimal()
    if isinstance(remainder, Decimal128):
        remainder = remainder.to_decimal()

    def mod_scan(field_value, query):
        for value in field_value:
            if isinstance(value, bool) or not isinstance(value, num_types):
                continue
            if isinstance(value, Decimal128):
                value = value.to_decimal()
            if int(value % divisor) == int(remainder):
                return True
        return False

    @keep(query)
    def _mod(field_walker):
        field_value = field_walker.value
        if mod_scan(field_value, query):
            return True

    return _mod


def parse_jsonSchema(query):
    @keep(query)
    def _jsonSchema(field_walker):
        pass

    return _jsonSchema
