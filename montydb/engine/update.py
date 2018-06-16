
from ..errors import WriteError


class Updator(object):

    def __init__(self, spec):

        self.update_ops = {

            # field update ops
            "$inc": parse_inc,
            "$min": None,
            "$max": None,
            "$mul": None,
            "$rename": None,
            "$set": None,
            "$setOnInsert": None,
            "$unset": None,
            "$currentDate": None,

        }

        self.operations = self.parser(spec)
        self.__field_walker = None

    def __repr__(self):
        pass

    def __call__(self, fieldwalker):
        self.__field_walker = fieldwalker
        for operator in self.operations.values():
            operator(fieldwalker)
        return fieldwalker.doc

    @property
    def field_walker(self):
        return self.__field_walker

    def parser(self, spec):
        if not next(iter(spec)).startswith("$"):
            raise ValueError("update only works with $ operators")

        field_to_update = dict()
        for op, cmd_doc in spec.items():
            if op not in self.update_ops:
                raise WriteError("Unknown modifier: {}".format(op))
            for field, value in cmd_doc.items():
                if field in field_to_update:
                    raise WriteError("Updating the path {0!r} would create a "
                                     "conflict at {0!r}".format(field))
                field_to_update[field] = self.update_ops[op](field, value)

        return field_to_update


def parse_inc(field, value):
    def _inc(field_walker):
        field_walker(field)
        if (field_walker.exists and (
                field_walker.value.arrays or
                not isinstance(field_walker.value.elements[0], int))):
            _id = field_walker.doc["_id"]
            value_type = type(field_walker.value.elements[0])
            raise WriteError("Cannot apply $inc to a value of non-numeric "
                             "type. {{_id: {0}}} has the field {1!r} of "
                             "non-numeric type {2}".format(_id,
                                                           field,
                                                           value_type))
        if field_walker.exists:
            new_val = field_walker.value.elements[0] + value
        else:
            new_val = value

        res = field_walker.setval(new_val)
        if res is not None:
            raise WriteError("Cannot create field {0!r} in element "
                             "{{{1}: {2}}}".format(*res))

    return _inc


def parse_min(field, value):
    def _min(field_walker):
        pass

    return _min


def parse_max(field, value):
    def _max(field_walker):
        pass

    return _max


def parse_mul(field, value):
    def _mul(field_walker):
        pass

    return _mul


def parse_rename(field, value):
    def _rename(field_walker):
        pass

    return _rename


def parse_set(field, value):
    def _set(field_walker):
        pass

    return _set


def parse_setOnInsert(field, value):
    def _setOnInsert(field_walker):
        pass

    return _setOnInsert


def parse_unset(field, value):
    def _unset(field_walker):
        pass

    return _unset


def parse_currentDate(field, value):
    def _currentDate(field_walker):
        pass

    return _currentDate
