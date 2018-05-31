
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

        self.conditions = self.parser(spec)
        self.__field_walker = None

    def __repr__(self):
        pass

    def __call__(self, fieldwalker):
        self.__field_walker = fieldwalker
        return fieldwalker.doc

    @property
    def field_walker(self):
        return self.__field_walker

    def parser(self, spec):
        if not next(iter(spec)).startswith("$"):
            raise ValueError("update only works with $ operators")

        field_to_update = []
        for op, cmd in spec.items():
            if op not in self.update_ops:
                raise WriteError("Unknown modifier: {}".format(op))
            for field in cmd:
                if field in field_to_update:
                    raise WriteError("Cannot update {0!r} and {0!r} at the "
                                     "same time".format(field))
                field_to_update.append(field)


def parse_inc(query):
    def _inc(field_walker):
        return field_walker

    return _inc
