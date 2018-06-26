
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_err_1(monty_update, mongo_update):
    # testing update path conflict
    docs = [
        {"a": {"b": 4}}
    ]
    spec = {"$inc": {"a.b": 1}, "$min": {"a.b": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_1(monty_update, mongo_update):
    # testing useless array_filter
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem": {"c": {"$gt": 0}, "b": {"$gt": 4}}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}


def test_update_positional_filtered_err_2(monty_update, mongo_update):
    # testing array_filter duplicated top-level field error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}}, {"elem.c": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_3(monty_update, mongo_update):
    # testing array_filter multiple top-level field error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}}, {"other": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_4(monty_update, mongo_update):
    # testing array_filter not used error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}}, {"notuse": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_5(monty_update, mongo_update):
    # testing array_filter not used error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}},
                     {"notuse": {"$gt": 4}},
                     {"notuse2": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_6(monty_update, mongo_update):
    # testing bad identifier
    docs = [
        {"a": [{"b": {"c": 4, "d": 0}}, {"b": {"c": 4, "d": 1}}]}
    ]
    spec = {"$inc": {"a.$[elem.b].c": 1}}
    array_filters = [{"elem.b.d": {"$gt": 0}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_7(monty_update, mongo_update):
    # testing no identifier
    docs = [
        {"a": [{"b": {"c": 4, "d": 0}}, {"b": {"c": 4, "d": 1}}]}
    ]
    spec = {"$inc": {"a.$[elem].b.c": 1}}
    array_filters = []

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_near_conflict(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1, "a.$[almost].b": 2}}
    array_filters = [{"elem.c": {"$gt": 0}},  # update element 0
                     {"almost": {"b": 4, "c": 0}}]  # update element 1

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 5, "c": 1}, {"b": 6, "c": 0}]}


def test_update_positional_filtered_has_conflict(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1, "a.$[conflict].b": 2}}
    array_filters = [{"elem.c": {"$gt": 0}},  # update element 0
                     {"conflict": {"b": 4, "c": 1}}]  # update element 0, too.

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    assert mongo_err.value.code == monty_err.value.code


def test_update_id(monty_update, mongo_update):
    docs = [
        {"a": 6},
    ]
    spec = {"$inc": {"_id": 3, "b": 9}}
    find = {"a": 6}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find)

    assert mongo_err.value.code == monty_err.value.code


def test_update_positional(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$.b": 1}}
    find = {"a.b": 4}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 3, "c": 1}, {"b": 5, "c": 0}]}


def test_update_positional_without_query_condition(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$.b": 1}}
    find = {}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find)

    assert mongo_err.value.code == monty_err.value.code


def test_update_array_faild_1(monty_update, mongo_update):
    docs = [
        {"a": [{"1": 4}]}
    ]
    spec = {"$inc": {"a.1.$[]": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    assert mongo_err.value.code == monty_err.value.code


def test_update_array_faild_2(monty_update, mongo_update):
    docs = [
        {"a": [{"1": 4}, {"1": 5}]}
    ]
    spec = {"$inc": {"a.1.$[]": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    assert mongo_err.value.code == monty_err.value.code


def test_update_with_dollar_prefixed_field(monty_update, mongo_update):
    docs = [
        {"a": {"1": 4}}
    ]
    spec = {"$inc": {"a.$a": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    assert mongo_err.value.code == monty_err.value.code


def test_update_complex_position_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]}
    ]
    spec = {"$inc": {"a.$[].$": 10}}
    find = {"a.b.1": {"$gt": 5}}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": [1, 5], "2": 10},
                                   {"b": [2, 4], "2": 10},
                                   {"b": [3, 6], "2": 10}]}


def test_update_complex_position_2(monty_update, mongo_update):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]}
    ]
    spec = {"$inc": {"a.$[].b.$": 10}}
    find = {"a.b.1": {"$eq": 6}}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": [1, 5, 10]},
                                   {"b": [2, 4, 10]},
                                   {"b": [3, 6, 10]}]}


def test_update_complex_position_3(monty_update, mongo_update):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]}
    ]
    spec = {"$inc": {"a.$[].$[]": 10}}
    find = {"a.b.1": {"$gt": 5}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find)

    assert mongo_err.value.code == monty_err.value.code


def test_update_empty_array(monty_update, mongo_update):
    docs = [
        {"a": []}
    ]
    spec = {"$inc": {"a.0": 10}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [10]}
