from collections import OrderedDict
from datetime import datetime

from ..errors import WriteError

from .field_walker import FieldWalker, FieldWriteError
from .weighted import Weighted, _cmp_decimal
from .queries import QueryFilter, ordering
from ..types import (
    bson,
    string_types,
    is_numeric_type,
    is_duckument_type,
    is_integer_type,
    keep,
)


def _update(fieldwalker, field, value, evaluator, array_filters):

    fieldwalker.go(field)
    try:
        fieldwalker.set(value, evaluator, array_filters)
    # Take error message and put error code
    except FieldWriteError as err:
        raise WriteError(str(err), code=err.code)


def _drop(fieldwalker, field, array_filters):

    fieldwalker.go(field)
    try:
        fieldwalker.drop(array_filters)
    # Take error message and put error code
    except FieldWriteError as err:
        raise WriteError(str(err), code=err.code)


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
            "$setOnInsert": self.parse_set_on_insert,
            "$unset": parse_unset,
            "$currentDate": parse_currentDate,
            # array update ops
            # $                 implemented in FieldWalker
            # $[]               implemented in FieldWalker
            # $[<identifier>]   implemented in FieldWalker
            "$addToSet": parse_add_to_set,
            "$pop": parse_pop,
            "$pull": parse_pull,
            "$push": parse_push,
            "$pullAll": parse_pull_all,
            # $each             implemented in Eacher
            # $position         implemented in Eacher
            # $slice            implemented in Eacher
            # $sort             implemented in Eacher
        }

        self.fields_to_update = []
        self.array_filters = self.array_filter_parser(array_filters or [])
        # sort by key (operator)
        self.operations = OrderedDict(sorted(self.parser(spec).items()))
        self.__insert = None
        self.__fieldwalker = None

    def __repr__(self):
        pass

    def __call__(self, fieldwalker, do_insert=False):
        """Update document and return a bool value indicate changed or not"""
        self.__fieldwalker = fieldwalker
        self.__insert = do_insert

        with fieldwalker:
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
                    msg = (
                        "Found multiple array filters with the same "
                        "top-level field name {}".format(id_s[0])
                    )
                    raise WriteError(msg, code=9)

                if top and id_s[0] != top:
                    msg = (
                        "Error parsing array filter: Expected a single "
                        "top-level field name, found {0!r} and {1!r}"
                        "".format(top, id_s[0])
                    )
                    raise WriteError(msg, code=9)

                top = id_s[0]
                conds.update({identifier: cond})

            filters[top] = QueryFilter(conds)

        return filters

    def parser(self, spec):
        if not next(iter(spec)).startswith("$"):
            raise ValueError("update only works with $ operators")

        update_stack = {}
        idnt_tops = list(self.array_filters.keys())

        for op, cmd_doc in spec.items():
            if op not in self.update_ops:
                raise WriteError("Unknown modifier: {}".format(op))

            if not is_duckument_type(cmd_doc):
                msg = (
                    "Modifiers operate on fields but we found type {0} "
                    "instead. For example: {{$mod: {{<field>: ...}}}} "
                    "not {1}".format(type(cmd_doc).__name__, spec)
                )
                raise WriteError(msg, code=9)

            for field, value in cmd_doc.items():
                for top in list(idnt_tops):
                    if "$[{}]".format(top) in field:
                        idnt_tops.remove(top)

                update_stack[field] = self.update_ops[op](
                    field, value, self.array_filters
                )

                self.check_conflict(field)
                if op == "$rename":
                    self.check_conflict(value)

        if idnt_tops:
            msg = (
                "The array filter for identifier {0!r} was not "
                "used in the update {1}".format(idnt_tops[0], spec)
            )
            raise WriteError(msg, code=9)

        return update_stack

    def check_conflict(self, field):
        for staged in self.fields_to_update:
            if field.startswith(staged) or staged.startswith(field):
                msg = (
                    "Updating the path {0!r} would create a "
                    "conflict at {1!r}".format(field, staged[: len(field)])
                )
                raise WriteError(msg, code=40)

        self.fields_to_update.append(field)

    def parse_set_on_insert(self, field, value, array_filters):
        @keep(value)
        def _set_on_insert(fieldwalker):
            if self.__insert:
                parse_set(field, value, array_filters)(fieldwalker)

        return _set_on_insert


def parse_inc(field, value, array_filters):
    if not is_numeric_type(value):
        val_repr_ = "{!r}" if isinstance(value, string_types) else "{}"
        val_repr_ = val_repr_.format(value)
        msg = "Cannot increment with non-numeric argument: {{{0}: {1}}}".format(
            field, val_repr_
        )
        raise WriteError(msg, code=14)

    @keep(value)
    def _inc(fieldwalker):
        def evaluator(node, inc_val):
            old_val = node.value
            if node.exists and not is_numeric_type(old_val):
                _id = fieldwalker.doc["_id"]
                value_type = type(old_val).__name__
                msg = (
                    "Cannot apply $inc to a value of non-numeric type. "
                    "{{_id: {0}}} has the field {1!r} of non-numeric type "
                    "{2}".format(_id, str(node), value_type)
                )
                raise WriteError(msg, code=14)

            is_decimal128 = False
            if isinstance(old_val, bson.Decimal128):
                is_decimal128 = True
                old_val = old_val.to_decimal()
            if isinstance(inc_val, bson.Decimal128):
                is_decimal128 = True
                inc_val = inc_val.to_decimal()

            if is_decimal128:
                return bson.Decimal128((old_val or 0) + inc_val)
            else:
                return (old_val or 0) + inc_val

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _inc


def parse_min(field, value, array_filters):
    @keep(value)
    def _min(fieldwalker):
        def evaluator(node, min_val):
            old_val = node.value
            if node.exists:
                old_val = Weighted(old_val)
                min_val = Weighted(min_val)
                return min_val.value if min_val < old_val else old_val.value
            else:
                return min_val

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _min


def parse_max(field, value, array_filters):
    @keep(value)
    def _max(fieldwalker):
        def evaluator(node, max_val):
            old_val = node.value
            if node.exists:
                old_val = Weighted(old_val)
                max_val = Weighted(max_val)
                return max_val.value if max_val > old_val else old_val.value
            else:
                return max_val

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _max


def parse_mul(field, value, array_filters):
    if not is_numeric_type(value):
        val_repr_ = "{!r}" if isinstance(value, string_types) else "{}"
        val_repr_ = val_repr_.format(value)
        msg = "Cannot multiply with non-numeric argument: {{{0}: {1}}}".format(
            field, val_repr_
        )
        raise WriteError(msg, code=14)

    @keep(value)
    def _mul(fieldwalker):
        def evaluator(node, mul_val):
            old_val = node.value
            if node.exists and not is_numeric_type(old_val):
                _id = fieldwalker.doc["_id"]
                value_type = type(old_val).__name__
                msg = (
                    "Cannot apply $mul to a value of non-numeric type. "
                    "{{_id: {0}}} has the field {1!r} of non-numeric type "
                    "{2}".format(_id, str(node), value_type)
                )
                raise WriteError(msg, code=14)

            is_decimal128 = False
            if isinstance(old_val, bson.Decimal128):
                is_decimal128 = True
                old_val = old_val.to_decimal()
            if isinstance(mul_val, bson.Decimal128):
                is_decimal128 = True
                mul_val = mul_val.to_decimal()

            if is_decimal128:
                return bson.Decimal128((old_val or 0) * mul_val)
            else:
                return (old_val or 0.0) * mul_val

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _mul


def _get_array_member(fieldvalues):
    for node in fieldvalues.nodes:
        if node.in_array:
            return node


def parse_rename(field, new_field, array_filters):
    if not isinstance(new_field, string_types):
        msg = "The 'to' field for $rename must be a string: {0}: {1}".format(
            field, new_field
        )
        raise WriteError(msg, code=2)

    if field == new_field:
        msg = (
            "The source and target field for $rename must differ: "
            "{0}: {1!r}".format(field, new_field)
        )
        raise WriteError(msg, code=2)

    if field.startswith(new_field) or new_field.startswith(field):
        msg = (
            "The source and target field for $rename must not be on the "
            "same path: {0}: {1!r}".format(field, new_field)
        )
        raise WriteError(msg, code=2)

    @keep(new_field)
    def _rename(fieldwalker):

        probe = FieldWalker(fieldwalker.doc)

        probe.go(field).get()
        fieldvalues = probe.value

        if not fieldvalues.is_exists():
            return

        value = next(fieldvalues.iter_plain())

        array_member = _get_array_member(fieldvalues)
        if array_member is not None:
            _id = probe.doc["_id"]
            array_field = str(array_member.parent)
            msg = (
                "The source field cannot be an array element, "
                "{0!r} in doc with _id: {1} has an array field "
                "called {2!r}".format(field, _id, array_field)
            )
            raise WriteError(msg, code=2)

        probe.go(new_field).get()
        fieldvalues = probe.value

        array_member = _get_array_member(fieldvalues)
        if array_member is not None:
            _id = probe.doc["_id"]
            array_field = str(array_member.parent)
            msg = (
                "The destination field cannot be an array element, "
                "{0!r} in doc with _id: {1} has an array field "
                "called {2!r}".format(new_field, _id, array_field)
            )
            raise WriteError(msg, code=2)

        _drop(fieldwalker, field, array_filters)
        _update(fieldwalker, new_field, value, None, array_filters)

    return _rename


def parse_set(field, value, array_filters):
    @keep(value)
    def _set(fieldwalker):

        _update(fieldwalker, field, value, None, array_filters)

    return _set


def parse_unset(field, _, array_filters):
    @keep(field)
    def _unset(fieldwalker):

        _drop(fieldwalker, field, array_filters)

    return _unset


def parse_currentDate(field, value, array_filters):
    date_type = {
        "date": datetime.utcnow(),
        "timestamp": bson.Timestamp(datetime.utcnow(), 1),
    }

    if not isinstance(value, bool):
        if not is_duckument_type(value):
            msg = (
                "{} is not valid type for $currentDate. Please use a "
                "boolean ('true') or a $type expression ({{$type: "
                "'timestamp/date'}}).".format(type(value).__name__)
            )
            raise WriteError(msg, code=2)

        for k, v in value.items():
            if k != "$type":
                msg = "Unrecognized $currentDate option: {}".format(k)
                raise WriteError(msg, code=2)
            if v not in date_type:
                msg = (
                    "The '$type' string field is required to be 'date' "
                    "or 'timestamp': {$currentDate: {field : {$type: "
                    "'date'}}}"
                )
                raise WriteError(msg, code=2)

            value = date_type[v]
    else:
        value = date_type["date"]

    @keep(value)
    def _currentDate(fieldwalker):
        parse_set(field, value, array_filters)(fieldwalker)

    return _currentDate


def parse_add_to_set(field, value_or_each, array_filters):
    if is_duckument_type(value_or_each) and next(iter(value_or_each)) == "$each":
        value = EachAdder(value_or_each)
        run_each = True
    else:
        value = value_or_each
        run_each = False

    @keep(value)
    def _add_to_set(fieldwalker):
        def evaluator(node, new_elem):
            old_val = node.value
            if node.exists and not isinstance(old_val, list):
                value_type = type(old_val).__name__
                msg = (
                    "Cannot apply $addToSet to non-array field. Field "
                    "named {0!r} has non-array type {1}"
                    "".format(str(node), value_type)
                )
                raise WriteError(msg, code=2)

            if run_each:
                eacher = new_elem
                new_array = eacher(old_val)
            else:
                new_array = (old_val or [])[:]
                if new_elem not in new_array:
                    new_array.append(new_elem)

            return new_array

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _add_to_set


def parse_pop(field, value, array_filters):
    if not is_numeric_type(value):
        msg = "Expected a number in: {0}: {1!r}".format(field, value)
        raise WriteError(msg, code=9)
    else:
        try:
            value = float(value)
            msg_raw = "Expected an integer: {0}: {1!r}"
        except TypeError:
            msg_raw = "Cannot represent as a 64-bit integer: {0}: {1!r}"
            value = float(value.to_decimal())

        if value not in (1.0, -1.0):
            raise WriteError(msg_raw.format(field, value), code=9)

    @keep(value)
    def _pop(fieldwalker):
        def evaluator(node, pop_ind):
            old_val = node.value
            if node.exists and not isinstance(old_val, list):
                value_type = type(old_val).__name__
                msg = (
                    "Path {0!r} contains an element of non-array type "
                    "{1!r}".format(str(node), value_type)
                )
                raise WriteError(msg, code=14)

            if not node.exists:
                # do nothing
                return old_val

            if pop_ind == 1:
                return old_val[:-1]
            else:
                return old_val[1:]

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _pop


def parse_pull(field, value_or_conditions, array_filters):
    if is_duckument_type(value_or_conditions):
        query_spec = {}
        for k, v in value_or_conditions.items():
            if not k[:1] == "$":
                query_spec[".".join((field, k))] = v
            else:
                query_spec[field] = {k: v}
        queryfilter = QueryFilter(query_spec)
    else:
        queryfilter = QueryFilter({field: value_or_conditions})

    @keep(queryfilter)
    def _pull(fieldwalker):
        def evaluator(node, _):
            old_val = node.value
            if node.exists and not isinstance(old_val, list):
                msg = "Cannot apply $pull to a non-array value"
                raise WriteError(msg, code=2)

            if not node.exists:
                # do nothing
                return old_val

            new_array = []
            for elem in old_val:
                result = queryfilter({field: elem})

                if not result:
                    new_array.append(elem)
            return new_array

        _update(fieldwalker, field, None, evaluator, array_filters)

    return _pull


def parse_push(field, value_or_each, array_filters):
    if is_duckument_type(value_or_each) and "$each" in value_or_each:
        value = EachPusher(value_or_each)
        run_each = True
    else:
        value = value_or_each
        run_each = False

    @keep(value)
    def _push(fieldwalker):
        def evaluator(node, new_elem):
            old_val = node.value
            if node.exists and not isinstance(old_val, list):
                value_type = type(old_val).__name__
                _id = fieldwalker.doc["_id"]
                msg = (
                    "The field {0!r} must be an array but is of type "
                    "{1} in document {{_id: {2}}}"
                    "".format(str(node), value_type, _id)
                )
                raise WriteError(msg, code=2)

            if run_each:
                eacher = new_elem
                new_array = eacher(old_val)
            else:
                new_array = (old_val or [])[:]
                new_array.append(new_elem)

            return new_array

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _push


def parse_pull_all(field, value, array_filters):
    if not isinstance(value, list):
        value_type = type(value).__name__
        msg = "$pullAll requires an array argument but was given a {}".format(
            value_type
        )
        raise WriteError(msg, code=2)

    @keep(value)
    def _pull_all(fieldwalker):
        def evaluator(node, pull_list):
            old_val = node.value
            if node.exists and not isinstance(old_val, list):
                msg = "Cannot apply $pull to a non-array value"
                raise WriteError(msg, code=2)

            if not node.exists:
                # do nothing
                return old_val

            def convert(lst):
                for val in lst:
                    if isinstance(val, bson.Decimal128):
                        yield _cmp_decimal(val)
                    else:
                        yield val

            pull_list = list(convert(pull_list))
            old_val = list(convert(old_val))

            new_array = [elem for elem in old_val if elem not in pull_list]
            return new_array

        _update(fieldwalker, field, value, evaluator, array_filters)

    return _pull_all


class EachAdder(object):
    def __init__(self, spec):
        spec = spec.copy()

        self.mods = {
            "$each": None,
        }

        for mod, value in spec.items():
            try:
                type_check = self.validators[mod]
            except KeyError:
                raise WriteError(
                    "Found unexpected fields after $each in $addToSet: %s" % spec,
                    code=2,
                )

            self.mods[mod] = type_check(self, value)

    def __call__(self, array):
        new_array = (array or [])[:]
        new_elems = self.mods["$each"][:]

        new_array[0:] += [e for e in new_elems if e not in new_array]
        return new_array

    def _validate_each(self, each):
        try:
            each[:]
        except TypeError:
            type_name = type(each).__name__
            raise WriteError(
                "The argument to $each in $addToSet must be an "
                "array but it was of type %s" % type_name,
                code=14,
            )
        return each

    validators = {
        "$each": _validate_each,
    }


class EachPusher(object):
    def __init__(self, spec):
        spec = spec.copy()

        self.mods = {
            "$each": None,
            "$position": None,
            "$slice": None,
            "$sort": None,
        }

        for mod, value in spec.items():
            try:
                type_check = self.validators[mod]
            except KeyError:
                raise WriteError("Unrecognized clause in $push: %s" % mod, code=2)

            self.mods[mod] = type_check(self, value)

    def __call__(self, array):
        new_array = (array or [])[:]
        new_elems = self.mods["$each"][:]

        position = self.mods["$position"]
        slice = self.mods["$slice"]
        sort = self.mods["$sort"]

        if position is None:
            new_array += new_elems
        else:
            new_array[:position] += new_elems

        if slice is not None:
            if slice >= 0:
                new_array = new_array[:slice]
            else:
                new_array = new_array[slice:]

        if sort is not None:
            if is_duckument_type(sort):
                fieldwalkers = list()
                unsortable = list()
                for elem in new_array:
                    if is_duckument_type(elem):
                        fieldwalkers.append(FieldWalker(elem))
                    else:
                        unsortable.append(elem)

                ordered = ordering(fieldwalkers, sort)
                new_array = [f.doc for f in ordered]

                if unsortable:
                    is_reverse = bool(1 - next(iter(sort.values())))
                    if is_reverse:
                        new_array += unsortable
                    else:
                        new_array[:0] += unsortable

            else:
                is_reverse = bool(1 - sort)
                ordered = sorted((Weighted(e) for e in new_array), reverse=is_reverse)
                new_array = [w.value for w in ordered]

        return new_array

    def _validate_each(self, each):
        try:
            each[:]
        except TypeError:
            type_name = type(each).__name__
            raise WriteError(
                "The argument to $each in $push must be an "
                "array but it was of type: %s" % type_name,
                code=2,
            )
        return each

    def _validate_position(self, position):
        if not is_integer_type(position):
            type_name = type(position).__name__
            raise WriteError(
                "The value for $position must be an integer "
                "value, not of type: %s" % type_name,
                code=2,
            )
        return position

    def _validate_slice(self, slice):
        if not is_integer_type(slice):
            type_name = type(slice).__name__
            raise WriteError(
                "The value for $slice must be an integer "
                "value but was given type: %s" % type_name,
                code=2,
            )
        return slice

    def _validate_sort(self, sort, int_only=False):
        if is_integer_type(sort) or int_only:
            if sort not in (1, -1):
                raise WriteError(
                    "The $sort element value must be either 1 or -1", code=2
                )
            return sort

        if is_duckument_type(sort):
            for key, value in sort.items():
                self._validate_sort(value, int_only=True)
            return sort

        raise WriteError(
            "The $sort is invalid: use 1/-1 to sort the whole "
            "element, or {field:1/-1} to sort embedded fields",
            code=2,
        )

    validators = {
        "$each": _validate_each,
        "$position": _validate_position,
        "$slice": _validate_slice,
        "$sort": _validate_sort,
    }
