
import re

from ..errors import OperationFailure

from ..vendor.six import string_types


class FieldContext(object):
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
        self.doc = doc
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.reset()

    def __call__(self, path):
        """For `LogicBox` calling on `with` statement and cache field value"""
        self.value = self.doc
        for field in path.split("."):
            try:
                self.in_array = False
                if isinstance(self.value, (list, tuple)) and field.isdigit():
                    # Currently inside an array type value
                    # with given index path.
                    self.in_array = True
                    field = int(field)
                self.value = self.value[field]
                self.exists = True
            except (KeyError, IndexError, TypeError):
                self.reset()
                break

        return self

    def reset(self):
        self.value = None
        self.exists = False
        self.in_array = False


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
        self.gen = None

        self._logic = {

            "$and": self.__call_and,
            "$or": self.__call_or,
            "$nor": self.__call_nor,
            "$not": self.__call_not,

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
        self.gen = (cond(field_context) for cond in self[:])
        try:
            return self._logic[self.theme]()
        except KeyError:
            return self.__call_field(field_context)

    def __call_field(self, field_context):
        """Entering document field context before process"""
        with field_context(self.theme):
            return all(self.gen)

    def __call_and(self):
        return all(self.gen)

    def __call_or(self):
        return any(self.gen)

    def __call_nor(self):
        return not any(self.gen)

    def __call_not(self):
        return not all(self.gen)


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
            "$elemMatch": None,
            "$size": None,

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
            if not isinstance(sub_spec, (list, tuple)):
                raise OperationFailure("{} must be an array".format(theme))

            logic_box = LogicBox(theme)

            for cond in sub_spec:
                if not isinstance(cond, dict):
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
            return self.subparser("$not", sub_spec)

        return _parse_not


def _is_expression_obj(sub_spec):
    return (isinstance(sub_spec, dict) and
            next(iter(sub_spec)).startswith("$"))


"""
Field-level Query Operators
"""


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


def bson_type_id(value):
    pass


def __add_attrib(deco):
    """Decorator helper"""
    def meta_decorator(value):
        def add_attrib(func):
            func._keep = lambda: value
            return func
        return add_attrib
    return meta_decorator


@__add_attrib
def __keep(query):
    """A decorator that preserve operation query for operator"""
    def _(func):
        return query
    return _


"""
Comparison Query Operators
"""


def parse_eq(query):
    @__keep(query)
    def _eq(field_context):
        if field_context.value == query:
            return True
        if isinstance(field_context.value, (list, tuple)):
            if not field_context.in_array:
                return query in field_context.value

    return _eq


def parse_ne(query):
    @__keep(query)
    def _ne(field_context):
        if isinstance(field_context.value, (list, tuple)):
            return query not in field_context.value
        return field_context.value != query

    return _ne


def _is_same_type(value1, value2):
    if type(value1) is type(value2):
        return True
    if isinstance(value1, string_types) and isinstance(value2, string_types):
        return True
    return False


def parse_gt(query):
    @__keep(query)
    def _gt(field_context):
        if _is_same_type(field_context.value, query):
            return field_context.value > query
        if isinstance(field_context.value, (list, tuple)):
            for value in field_context.value:
                if _is_same_type(value, query) and value > query:
                    return True

    return _gt


def parse_gte(query):
    @__keep(query)
    def _gte(field_context):
        if _is_same_type(field_context.value, query):
            return field_context.value >= query
        if isinstance(field_context.value, (list, tuple)):
            for value in field_context.value:
                if _is_same_type(value, query) and value >= query:
                    return True

    return _gte


def parse_lt(query):
    @__keep(query)
    def _lt(field_context):
        if _is_same_type(field_context.value, query):
            return field_context.value < query
        if isinstance(field_context.value, (list, tuple)):
            for value in field_context.value:
                if _is_same_type(value, query) and value < query:
                    return True

    return _lt


def parse_lte(query):
    @__keep(query)
    def _lte(field_context):
        if _is_same_type(field_context.value, query):
            return field_context.value <= query
        if isinstance(field_context.value, (list, tuple)):
            for value in field_context.value:
                if _is_same_type(value, query) and value <= query:
                    return True

    return _lte


def _in_match(field_value, query):
    """Helper function for $in and $nin
    """
    q_regex = [q for q in query if isinstance(q, re._pattern_type)]

    def search(value):
        if value in query:
            return True
        if isinstance(value, string_types) and q_regex:
            return any(q.search(value) for q in q_regex)

    if isinstance(field_value, (list, tuple)):
        return any(search(fv) for fv in field_value)
    return search(field_value)


def parse_in(query):
    if not isinstance(query, (list, tuple)):
        raise OperationFailure("$in needs an array")

    if any(_is_expression_obj(q) for q in query):
        raise OperationFailure("cannot nest $ under $in")

    @__keep(query)
    def _in(field_context):
        return _in_match(field_context.value, query)

    return _in


def parse_nin(query):
    if not isinstance(query, (list, tuple)):
        raise OperationFailure("$nin needs an array")

    if any(_is_expression_obj(q) for q in query):
        #  (NOTE): MongoDB(3.6) raise "cannot nest $ under *$in*",
        #          this should be *$nin*, but leave it as is for now.
        raise OperationFailure("cannot nest $ under $in")

    @__keep(query)
    def _nin(field_context):
        return not _in_match(field_context.value, query)

    return _nin


"""
Array Query Operators
"""


def parse_all(query):
    if not isinstance(query, (list, tuple)):
        raise OperationFailure("$all needs an array")

    @__keep(query)
    def _all(field_context):  # Query an Array of Embedded Documents ($in)
        if len(query) == 1 and query[0] == field_context.value:
            return True
        elif not isinstance(field_context.value, (list, tuple)):
            return False
        for q in query:
            if q not in field_context.value:
                return False
        return True

    return _all


def parse_elemMatch(query):
    @__keep(query)
    def _elemMatch(field_context):
        pass

    return _elemMatch


def parse_size(query):
    @__keep(query)
    def _size(field_context):
        pass

    return _size


"""
Element Query Operators
"""


def parse_exists(query):
    @__keep(query)
    def _exists(field_context):
        return field_context.exists == query

    return _exists


def parse_type(query):  # Not finished
    if not isinstance(query, (list, tuple)):
        query = tuple(query)
    query = [BSON_TYPE_ALIAS_ID.get(v, v) for v in query]

    @__keep(query)
    def _type(field_context):
        return bson_type_id(field_context.value) in query

    return _type
