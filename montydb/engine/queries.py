
import re
from copy import deepcopy
from bson.py3compat import integer_types, string_type
from bson.regex import Regex, str_flags_to_int
from bson.son import SON
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.code import Code

from ..errors import OperationFailure

from .base import (
    FieldWalker,
    Weighted,
    BSON_TYPE_ALIAS_ID,
    obj_to_bson_type_id,
)

from .helpers import (
    PY3,
    PY36,
    RE_PATTERN_TYPE,
    is_mapping_type,
    is_array_type,
    is_pattern_type,
    keep,
)


def ordering(documents, order):
    """
    """
    total = len(documents)
    pre_sect_stack = []

    for path, revr in order.items():
        is_reverse = bool(1 - revr)
        value_stack = []

        for indx, doc in enumerate(documents):
            # get field value
            value = Weighted(FieldWalker(doc)(path).value, is_reverse)
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
            ordereddoc.append(documents[indx])

            # define section
            # maintain the sorting result in next level sorting
            if not value == last_doc:
                sect_id += 1
            sect_stack.append(sect_id)
            last_doc = value

        # save result for next level sorting
        documents = ordereddoc
        pre_sect_stack = sect_stack

    return documents


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
        field_walker = FieldWalker(doc)
        return all(cond(field_walker) for cond in self.conditions)

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
            for op in sub_spec:
                if op not in self.field_ops:
                    raise OperationFailure("unknown operator: {}".format(op))

            return self.subparser("$not", sub_spec)

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
    `re._pattern_type` in pymongo, in those cases, if $options exists,
    $options flags will override the flags inside `bson.Regex` or
    `re._pattern_type` object, but if Python version lower than 3,
    $options will NOT override.
    """
    new_sub_spec = None
    _re = None
    if isinstance(sub_spec["$regex"], (RE_PATTERN_TYPE, Regex)):
        # Can't deepcopy this, put to somewhere else and retrieve it later
        _re = sub_spec["$regex"]
        sub_spec["$regex"] = None

    new_sub_spec = deepcopy(sub_spec)
    new_sub_spec["$regex"] = {
        "pattern": _re.pattern if _re else sub_spec["$regex"],
        "flags": sub_spec.get("$options", "")
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

    if _re and ("$options" not in sub_spec or not PY3):
        # Restore `re._pattern_type` or `Regex` object's flags if $options
        # not exists or Python version lower than 3
        new_sub_spec["$regex"]["flags"] = _FALG(_re.flags)

    return new_sub_spec


"""
Field-level Query Operators
- Comparison
"""


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

        if query in field_walker.value:
            # Handling bool type query
            if isinstance(query, bool):
                for v in field_walker.value:
                    if isinstance(v, bool) and query == v:
                        return True
                return False
            # Not to match with bool type when querying 1 or 0
            if query in [0, 1]:
                for v in field_walker.value:
                    if not isinstance(v, bool) and query == v:
                        return True
                return False

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


def _is_comparable(v1, v2):
    # Same type
    if type(v1) is type(v2):
        if isinstance(v1, Code):
            # Code object with scope is different from code without scope
            if v1.scope is None and v2.scope is None:
                return True
            elif not (v1.scope is None or v2.scope is None):
                return True
            else:
                return False
        else:
            return True

    # Type Bracketing
    # String
    if isinstance(v1, string_type) and isinstance(v2, string_type):
        if isinstance(v1, Code) or isinstance(v2, Code):
            # Code also an instance of string_type
            return False
        if isinstance(v1, Binary) or isinstance(v2, Binary):
            # Binary also an instance of string_type in PY2
            return False
        return True
    # Binary
    if isinstance(v1, (bytes, Binary)) and isinstance(v2, (bytes, Binary)):
        return True
    # Object
    if is_mapping_type(v1) and is_mapping_type(v2):
        return True
    # Numberic
    if not any([isinstance(v1, bool), isinstance(v2, bool)]):
        number_type = (integer_types, float, Int64, Decimal128)
        if isinstance(v1, number_type) and isinstance(v2, number_type):
            return True

    return False


def parse_gt(query):
    @keep(query)
    def _gt(field_walker):
        for value in field_walker.value:
            if _is_comparable(value, query):
                print(Weighted(value))
                print(Weighted(query))
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
                if Weighted(value) >= Weighted(query):
                    return True

    return _gte


def parse_lt(query):
    @keep(query)
    def _lt(field_walker):
        for value in field_walker.value:
            if _is_comparable(value, query):
                if Weighted(value) < Weighted(query):
                    return True

    return _lt


def parse_lte(query):
    @keep(query)
    def _lte(field_walker):
        for value in field_walker.value:
            if _is_comparable(value, query):
                if Weighted(value) <= Weighted(query):
                    return True

    return _lte


def _in_match(field_value, query):
    """Helper function for $in and $nin
    """
    q_regex = []
    for q in query:
        if is_pattern_type(q):
            q_regex.append(q)
        if isinstance(q, Regex):
            try:
                q_regex.append(q.try_compile())
            except re.error as e:
                raise OperationFailure("Regular expression is invalid:"
                                       " {}".format(e))

    def search(value):
        if value in query:
            return True
        if isinstance(value, string_type) and q_regex:
            return any(q.search(value) for q in q_regex)

    return any(search(fv) for fv in field_value)


def parse_in(query):
    if not is_array_type(query):
        raise OperationFailure("$in needs an array")

    if any(_is_expression_obj(q) for q in query):
        raise OperationFailure("cannot nest $ under $in")

    @keep(query)
    def _in(field_walker):
        return _in_match(field_walker.value, query)

    return _in


def parse_nin(query):
    if not is_array_type(query):
        raise OperationFailure("$nin needs an array")

    if any(_is_expression_obj(q) for q in query):
        raise OperationFailure("cannot nest $ under $nin")

    @keep(query)
    def _nin(field_walker):
        return not _in_match(field_walker.value, query)

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


_BSON_TYPE_ID = tuple(BSON_TYPE_ALIAS_ID.values())


def parse_type(query):
    def bson_type_id(values):
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
            bids = bson_type_id(field_walker.value.elements)
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

    if not isinstance(divisor, (int, float)):
        raise OperationFailure("malformed mod, divisor not a number")
    if not isinstance(remainder, (int, float)):
        remainder = 0

    def mod_scan(field_value, query):
        for value in field_value:
            if isinstance(value, bool):
                continue
            if not isinstance(value, (int, float)):
                continue
            if int(value % divisor) == remainder:
                return True
        return False

    @keep(query)
    def _mod(field_walker):
        if field_walker.embedded_in_array and not field_walker.index_posed:
            field_value = field_walker.value.arrays
            for value in field_value:
                if is_array_type(value):
                    if mod_scan(value, query):
                        return True
        else:
            field_value = field_walker.value.elements
            if mod_scan(field_value, query):
                return True

    return _mod


def parse_jsonSchema(query):
    @keep(query)
    def _jsonSchema(field_walker):
        pass

    return _jsonSchema
