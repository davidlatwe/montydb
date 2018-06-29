
from collections import OrderedDict, MutableMapping

from bson.py3compat import string_type
from bson.decimal128 import Decimal128

from ..errors import WriteError
from .core import FieldWriteError, Weighted, SimpleGetter
from .queries import QueryFilter
from .helpers import is_numeric_type


class Updator(object):

    def __init__(self, spec, array_filters=None):

        self.update_ops = {

            # field update ops
            "$inc": parse_inc,
            "$min": parse_min,
            "$max": parse_max,
            "$mul": parse_mul,
            "$rename": parse_rename,
            "$set": parse_set,
            "$setOnInsert": None,
            "$unset": parse_unset,
            "$currentDate": None,

        }

        self.fields_to_update = []
        self.array_filters = self.array_filter_parser(array_filters or [])
        self.operations = OrderedDict(sorted(self.parser(spec).items()))
        self.__fieldwalker = None

    def __repr__(self):
        pass

    def __call__(self, fieldwalker):
        if fieldwalker is None:
            return None

        self.__fieldwalker = fieldwalker
        for operator in self.operations.values():
            operator(fieldwalker)
        return fieldwalker.commit()

    @property
    def fieldwalker(self):
        return self.__fieldwalker

    def array_filter_parser(self, array_filters):
        filters = {}
        for i, filter_ in enumerate(array_filters):
            top = ""
            conds = {}
            for identifier, cond in filter_.items():
                id_s = identifier.split(".", 1)
                if not top and id_s[0] in filters:
                    msg = ("Found multiple array filters with the same "
                           "top-level field name {}".format(id_s[0]))
                    raise WriteError(msg, code=9)
                if top and id_s[0] != top:
                    msg = ("Error parsing array filter: Expected a single "
                           "top-level field name, found {0!r} and {1!r}"
                           "".format(top, id_s[0]))
                    raise WriteError(msg, code=9)
                top = id_s[0]
                if len(id_s) > 1:
                    conds.update({"{}.{}".format("{}", id_s[1]): cond})
                else:
                    conds.update({"{}": cond})

            filters[top] = lambda x, c=conds: QueryFilter(
                {k.format(x): v for k, v in c.items()})

        return filters

    def parser(self, spec):
        if not next(iter(spec)).startswith("$"):
            raise ValueError("update only works with $ operators")

        update_stack = {}
        idnt_tops = list(self.array_filters.keys())
        for op, cmd_doc in spec.items():
            if op not in self.update_ops:
                raise WriteError("Unknown modifier: {}".format(op))
            if not isinstance(cmd_doc, MutableMapping):
                msg = ("Modifiers operate on fields but we found type {0} "
                       "instead. For example: {{$mod: {{<field>: ...}}}} "
                       "not {1}".format(type(cmd_doc).__name__, spec))
                raise WriteError(msg, code=9)
            for field, value in cmd_doc.items():
                for top in idnt_tops:
                    if "$[{}]".format(top) in field:
                        idnt_tops.remove(top)
                        break

                update_stack[field] = self.update_ops[op](
                    field, value, self.array_filters)

                self.check_conflict(field)
                if op == "$rename":
                    self.check_conflict(value)

        if idnt_tops:
            msg = ("The array filter for identifier {0!r} was not "
                   "used in the update {1}".format(idnt_tops[0], spec))
            raise WriteError(msg, code=9)

        return update_stack

    def check_conflict(self, field):
        for staged in self.fields_to_update:
            if field.startswith(staged) or staged.startswith(field):
                msg = ("Updating the path {0!r} would create a "
                       "conflict at {1!r}".format(field, staged[:len(field)]))
                raise WriteError(msg, code=40)

        self.fields_to_update.append(field)


def parse_inc(field, value, array_filters):
    if not is_numeric_type(value):
        val_repr_ = "{!r}" if isinstance(value, string_type) else "{}"
        val_repr_ = val_repr_.format(value)
        msg = ("Cannot increment with non-numeric argument: "
               "{{{0}: {1}}}".format(field, val_repr_))
        raise WriteError(msg, code=14)

    def _inc(fieldwalker):

        def inc(old_val, inc_val, field):
            if old_val is not None and not is_numeric_type(old_val):
                _id = fieldwalker.doc["_id"]
                value_type = type(old_val).__name__
                msg = ("Cannot apply $inc to a value of non-numeric type. "
                       "{{_id: {0}}} has the field {1!r} of non-numeric type "
                       "{2}".format(_id, field, value_type))
                raise WriteError(msg, code=14)

            is_decimal128 = False
            if isinstance(old_val, Decimal128):
                is_decimal128 = True
                old_val = old_val.to_decimal()
            if isinstance(inc_val, Decimal128):
                is_decimal128 = True
                inc_val = inc_val.to_decimal()

            if is_decimal128:
                return Decimal128((old_val or 0) + inc_val)
            else:
                return (old_val or 0) + inc_val

        try:
            fieldwalker.go(field).set(value, inc, array_filters)
        except FieldWriteError as err:
            msg = err.message if hasattr(err, 'message') else str(err)
            raise WriteError(msg, code=err.code)

    return _inc


def parse_min(field, value, array_filters):
    def _min(fieldwalker):
        def min(old_val, min_val, field):
            old_val = Weighted(old_val)
            min_val = Weighted(min_val)
            return min_val.value if min_val < old_val else old_val.value

        try:
            fieldwalker.go(field).set(value, min, array_filters)
        except FieldWriteError as err:
            msg = err.message if hasattr(err, 'message') else str(err)
            raise WriteError(msg, code=err.code)

    return _min


def parse_max(field, value, array_filters):
    def _max(fieldwalker):
        def max(old_val, max_val, field):
            old_val = Weighted(old_val)
            max_val = Weighted(max_val)
            return max_val.value if max_val > old_val else old_val.value

        try:
            fieldwalker.go(field).set(value, max, array_filters)
        except FieldWriteError as err:
            msg = err.message if hasattr(err, 'message') else str(err)
            raise WriteError(msg, code=err.code)

    return _max


def parse_mul(field, value, array_filters):
    if not is_numeric_type(value):
        val_repr_ = "{!r}" if isinstance(value, string_type) else "{}"
        val_repr_ = val_repr_.format(value)
        msg = ("Cannot multiply with non-numeric argument: "
               "{{{0}: {1}}}".format(field, val_repr_))
        raise WriteError(msg, code=14)

    def _mul(fieldwalker):
        def mul(old_val, mul_val, field):
            if old_val is not None and not is_numeric_type(old_val):
                _id = fieldwalker.doc["_id"]
                value_type = type(old_val).__name__
                msg = ("Cannot apply $mul to a value of non-numeric type. "
                       "{{_id: {0}}} has the field {1!r} of non-numeric type "
                       "{2}".format(_id, field, value_type))
                raise WriteError(msg, code=14)

            is_decimal128 = False
            if isinstance(old_val, Decimal128):
                is_decimal128 = True
                old_val = old_val.to_decimal()
            if isinstance(mul_val, Decimal128):
                is_decimal128 = True
                mul_val = mul_val.to_decimal()

            if is_decimal128:
                return Decimal128((old_val or 0) * mul_val)
            else:
                return (old_val or 0.0) * mul_val

        try:
            fieldwalker.go(field).set(value, mul, array_filters)
        except FieldWriteError as err:
            msg = err.message if hasattr(err, 'message') else str(err)
            raise WriteError(msg, code=err.code)

    return _mul


def parse_rename(field, new_field, array_filters):
    if not isinstance(new_field, string_type):
        msg = ("The 'to' field for $rename must be a string: {0}: {1}"
               "".format(field, new_field))
        raise WriteError(msg, code=2)

    if field == new_field:
        msg = ("The source and target field for $rename must differ: "
               "{0}: {1!r}".format(field, new_field))
        raise WriteError(msg, code=2)

    if field.startswith(new_field) or new_field.startswith(field):
        msg = ("The source and target field for $rename must not be on the "
               "same path: {0}: {1!r}".format(field, new_field))
        raise WriteError(msg, code=2)

    def _rename(fieldwalker):
        getter = SimpleGetter()  # internal getter

        getter.run(fieldwalker, field)
        if getter.exists:
            value = getter.value
        elif getter.array_field:
            _id = fieldwalker.doc["_id"]
            msg = ("The source field cannot be an array element, "
                   "{0!r} in doc with _id: {1} has an array field "
                   "called {2!r}".format(field, _id, getter.array_field))
            raise WriteError(msg, code=2)
        else:
            return False

        getter.run(fieldwalker, new_field)
        if getter.array_field:
            _id = fieldwalker.doc["_id"]
            msg = ("The destination field cannot be an array element, "
                   "{0!r} in doc with _id: {1} has an array field "
                   "called {2!r}".format(new_field, _id, getter.array_field))
            raise WriteError(msg, code=2)

        try:
            fieldwalker.go(field).drop()
            fieldwalker.go(new_field).set(value)
        except FieldWriteError as err:
            msg = err.message if hasattr(err, 'message') else str(err)
            raise WriteError(msg, code=err.code)

    return _rename


def parse_set(field, value, array_filters):
    def _set(fieldwalker):
        try:
            fieldwalker.go(field).set(value, pick_with=array_filters)
        except FieldWriteError as err:
            msg = err.message if hasattr(err, 'message') else str(err)
            raise WriteError(msg, code=err.code)

    return _set


def parse_setOnInsert(field, value):
    def _setOnInsert(fieldwalker):
        raise NotImplementedError

    return _setOnInsert


def parse_unset(field, value, array_filters):
    def _unset(fieldwalker):
        try:
            fieldwalker.go(field).drop(array_filters)
        except FieldWriteError as err:
            msg = err.message if hasattr(err, 'message') else str(err)
            raise WriteError(msg, code=err.code)

    return _unset


def parse_currentDate(field, value):
    def _currentDate(fieldwalker):
        raise NotImplementedError

    return _currentDate
