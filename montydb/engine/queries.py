
import re
from bson.py3compat import integer_types, string_type

from .errors import OperationFailure

from .logic import (
    FieldWalker,
    Weighted,
    BSON_TYPE_ALIAS_ID,
)

from .helpers import (
    is_mapping_type,
    is_array_type,
    keep,
)


class FieldContext(FieldWalker):
    """Document traversal context manager

    A helper for document field-level operators working inside a field-themed
    `LogicBox` object.

    Before each field-themed `LogicBox` processing any operators within, it
    will first *call* this helper instance and send a document field path to
    find and cache document field's value for operators' later use, after all
    operators been processed, the context will reset.

    Args:
        doc (dict): Document passed from `QueryFilter` instance.

    """

    def __init__(self, doc):
        super(FieldContext, self).__init__(doc)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.reset()


class LogicBox(list):
    """A callable operator/logic array for document filtering

    By defining a `theme`, the instance's bool processing logic will behave
    differently when being called, and return different operation result.

    Only operator functions or `LogicBox` type instance are valid member of
    the `LogicBox`.

    Args:
        theme (str): A document field path or logic name ($and/$or/$nor/$not).

    """

    def __init__(self, theme):
        self.theme = theme
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
        content = ["Theme({})".format(self.theme)]
        for i in self[:]:
            if callable(i):
                if hasattr(i, "_keep"):
                    content.append("{}({})".format(i.__name__, i._keep()))
                else:
                    content.append(i.__name__)
            else:
                content.append(i)

        return "{}{} ".format("LogicBox", content).replace("'", "")

    def __call__(self, field_context):
        """Recursively calling `LogicBox` or operators content within

        A short-circuit logic sub-structure, passing `FieldContext` instance.

        Args:
            field_context (FieldContext): Recived from `QueryFilter` instance.

        """
        try:
            return self._logic[self.theme](field_context)
        except KeyError:
            return self.__call_field(field_context)

    def __gen(self, field_context):
        return (cond(field_context) for cond in self[:])

    def __call_field(self, field_context):
        """Entering document field context before process"""
        with field_context(self.theme):
            return all(self.__gen(field_context))

    def __call_elemMatch(self, field_context):
        """"""
        field_value = field_context.value[:-1]
        if field_context.nested:
            field_value = [fv for fv in field_value if is_array_type(fv)]
        for value in field_value:
            field_context.value = [value]
            if all(self.__gen(field_context)):
                return True

    def __call_and(self, field_context):
        return all(self.__gen(field_context))

    def __call_or(self, field_context):
        return any(self.__gen(field_context))

    def __call_nor(self, field_context):
        return not any(self.__gen(field_context))

    def __call_not(self, field_context):
        return not all(self.__gen(field_context))


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
            "$type": None,

            # Array
            "$all": parse_all,
            "$elemMatch": self.parse_elemMatch(),
            "$size": parse_size,

            # Evaluation
            "$jsonSchema": None,
            "$mod": None,

        }

        # Start parsing query object
        self.conditions = self.parser(spec)

        # ready to be called.

    def __repr__(self):
        return "{}{{ {}}}".format("QueryFilter", str(self.conditions))

    def __call__(self, doc):
        """Recursively calling `LogicBox` or operators content within

        A short-circuit logic structure to determine the document can pass the
        filter or not.

        Args:
            doc (dict): Document recived from database.

        """
        field_context = FieldContext(doc)
        return all(cond(field_context) for cond in self.conditions)

    def parser(self, spec):
        """Top-level parser"""

        # Implementation of implicity $and operation, fundamental query
        # container.
        logic_box = LogicBox("$and")

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
        if _is_expression_obj(sub_spec):
            for op, value in sub_spec.items():
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
                if not op.startswith("$") or op in self.pathless_ops:
                    return parse_elemMatch(sub_spec)

        return _parse_elemMatch


def _is_expression_obj(sub_spec):
    return (is_mapping_type(sub_spec) and
            next(iter(sub_spec)).startswith("$"))


"""
Field-level Query Operators
- Comparison
"""


def parse_eq(query):
    @keep(query)
    def _eq(field_context):
        return query in field_context.value

    return _eq


def parse_ne(query):
    @keep(query)
    def _ne(field_context):
        return query not in field_context.value

    return _ne


def _is_comparable(v1, v2):
    if type(v1) is type(v2):
        return True
    if isinstance(v1, string_type) and isinstance(v2, string_type):
        return True
    if is_mapping_type(v1) and is_mapping_type(v2):
        return True
    if not any([isinstance(v1, bool), isinstance(v2, bool)]):
        number_type = (integer_types, float)
        if isinstance(v1, number_type) and isinstance(v2, number_type):
            return True
    return False


def parse_gt(query):
    @keep(query)
    def _gt(field_context):
        for value in field_context.value:
            if _is_comparable(value, query):
                if Weighted(value) > Weighted(query):
                    return True

    return _gt


def parse_gte(query):
    @keep(query)
    def _gte(field_context):
        for value in field_context.value:
            if _is_comparable(value, query):
                if Weighted(value) >= Weighted(query):
                    return True

    return _gte


def parse_lt(query):
    @keep(query)
    def _lt(field_context):
        for value in field_context.value:
            if _is_comparable(value, query):
                if Weighted(value) < Weighted(query):
                    return True

    return _lt


def parse_lte(query):
    @keep(query)
    def _lte(field_context):
        for value in field_context.value:
            if _is_comparable(value, query):
                if Weighted(value) <= Weighted(query):
                    return True

    return _lte


def _in_match(field_value, query):
    """Helper function for $in and $nin
    """
    q_regex = [q for q in query if isinstance(q, re._pattern_type)]

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
    def _in(field_context):
        return _in_match(field_context.value, query)

    return _in


def parse_nin(query):
    if not is_array_type(query):
        raise OperationFailure("$nin needs an array")

    if any(_is_expression_obj(q) for q in query):
        #  (NOTE): MongoDB(3.6) raise "cannot nest $ under *$in*",
        #          this should be *$nin*, but leave it as is for now.
        raise OperationFailure("cannot nest $ under $in")

    @keep(query)
    def _nin(field_context):
        return not _in_match(field_context.value, query)

    return _nin


"""
Field-level Query Operators
- Array
"""


def parse_all(query):
    if not is_array_type(query):
        raise OperationFailure("$all needs an array")

    @keep(query)
    def _all(field_context):
        for q in query:
            if q not in field_context.value:
                return False
        return True

    return _all


def parse_elemMatch(query):
    @keep(query)
    def _elemMatch(field_context):
        doc_filter = QueryFilter(query)
        for emb_doc in field_context.value[:-1]:
            if doc_filter(emb_doc):
                return True

    return _elemMatch


def parse_size(query):
    if isinstance(query, float):
        raise OperationFailure("$size must be a whole number")
    if not isinstance(query, int):
        raise OperationFailure("$size needs a number")

    @keep(query)
    def _size(field_context):
        return len(field_context.value[:-1]) == query

    return _size


"""
Field-level Query Operators
- Element
"""


def parse_exists(query):
    @keep(query)
    def _exists(field_context):
        return field_context.exists == query

    return _exists


def bson_type_id(value):
    pass


def parse_type(query):  # Not finished
    if not is_array_type(query):
        query = tuple(query)
    query = [BSON_TYPE_ALIAS_ID.get(v, v) for v in query]

    @keep(query)
    def _type(field_context):
        return bson_type_id(field_context.value[:-1]) in query

    return _type
