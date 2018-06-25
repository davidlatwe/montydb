
from collections import OrderedDict

from bson.py3compat import string_type

from ..errors import WriteError
from .core import FieldSetValueError
from .queries import QueryFilter
from .helpers import is_numeric_type


class Updator(object):

    def __init__(self, spec, array_filters=None):

        self.update_ops = {

            # field update ops
            "$inc": parse_inc,
            "$min": parse_min,
            "$max": None,
            "$mul": None,
            "$rename": None,
            "$set": None,
            "$setOnInsert": None,
            "$unset": None,
            "$currentDate": None,

        }

        self.array_filters = self.array_filter_parser(array_filters or [])
        self.operations = OrderedDict(sorted(self.parser(spec).items()))
        self.__fieldwalker = None

    def __repr__(self):
        pass

    def __call__(self, fieldwalker):
        if fieldwalker is None:
            return None

        self.__fieldwalker = fieldwalker
        results = []
        for operator in self.operations.values():
            results.append(operator(fieldwalker))
        return any(results)

    @property
    def fieldwalker(self):
        return self.__fieldwalker

    def array_filter_parser(self, array_filters):
        filters = {}
        for filter_ in array_filters:
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
            filters[top] = lambda x: QueryFilter(
                {k.format(x): v for k, v in conds.items()})

        return filters

    def parser(self, spec):
        if not next(iter(spec)).startswith("$"):
            raise ValueError("update only works with $ operators")

        field_to_update = {}
        idnt_tops = list(self.array_filters.keys())
        for op, cmd_doc in spec.items():
            if op not in self.update_ops:
                raise WriteError("Unknown modifier: {}".format(op))
            for field, value in cmd_doc.items():
                for top in idnt_tops:
                    if "$[{}]".format(top) in field:
                        idnt_tops.remove(top)
                        break
                if field in field_to_update:
                    raise WriteError("Updating the path {0!r} would create a "
                                     "conflict at {0!r}".format(field))
                field_to_update[field] = self.update_ops[op](
                    field, value, self.array_filters)

        if idnt_tops:
            msg = ("The array filter for identifier {0!r} was not "
                   "used in the update {1}".format(idnt_tops[0], spec))
            raise WriteError(msg, code=9)

        return field_to_update


def parse_inc(field, value, array_filters):
    if not is_numeric_type(value):
        val_repr_ = "{!r}" if isinstance(value, string_type) else "{}"
        val_repr_ = val_repr_.format(value)
        msg = ("Cannot increment with non-numeric argument: "
               "{{{0}: {1}}}".format(field, val_repr_))
        raise WriteError(msg, code=14)

    def _inc(fieldwalker):

        def inc(old_val, inc_val):
            if old_val is not None and not is_numeric_type(old_val):
                _id = fieldwalker.doc["_id"]
                value_type = type(old_val).__name__
                field_name = field.split(".")[-1]
                msg = ("Cannot apply $inc to a value of non-numeric type. "
                       "{{_id: {0}}} has the field {1!r} of non-numeric type "
                       "{2}".format(_id, field_name, value_type))
                raise WriteError(msg, code=14)

            return (old_val or 0) + inc_val

        try:
            return fieldwalker.go(field).set(value, inc, array_filters)
        except FieldSetValueError as err:
            msg = ("Cannot create field {0!r} in element "
                   "{1}".format(*err.details))
            raise WriteError(msg, code=28)

    return _inc


def parse_min(field, value):
    def _min(fieldwalker):
        raise NotImplementedError

    return _min


def parse_max(field, value):
    def _max(fieldwalker):
        raise NotImplementedError

    return _max


def parse_mul(field, value):
    def _mul(fieldwalker):
        raise NotImplementedError

    return _mul


def parse_rename(field, value):
    def _rename(fieldwalker):
        raise NotImplementedError

    return _rename


def parse_set(field, value):
    def _set(fieldwalker):
        raise NotImplementedError

    return _set


def parse_setOnInsert(field, value):
    def _setOnInsert(fieldwalker):
        raise NotImplementedError

    return _setOnInsert


def parse_unset(field, value):
    def _unset(fieldwalker):
        raise NotImplementedError

    return _unset


def parse_currentDate(field, value):
    def _currentDate(fieldwalker):
        raise NotImplementedError

    return _currentDate
