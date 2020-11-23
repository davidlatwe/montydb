
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err
from montydb.types import bson_ as bson

from ...conftest import skip_if_no_bson


def test_update_inc_1(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$inc": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 2}


def test_update_inc_2(monty_update, mongo_update):
    docs = [
        {"a": [1]}
    ]
    spec = {"$inc": {"a": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_inc_3(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$inc": {"a": "1"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_inc_4(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$inc": {"a.1": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 3]}


def test_update_inc_5(monty_update, mongo_update):
    docs = [
        {"a": {"b": 1}}
    ]
    spec = {"$inc": {"a.b": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": 2}}


def test_update_inc_6(monty_update, mongo_update):
    docs = [
        {"a": {"b": [1, 2]}}
    ]
    spec = {"$inc": {"a.b.1": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": [1, 3]}}


def test_update_inc_7(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 0}, {"b": 1}]}
    ]
    spec = {"$inc": {"a.b": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_inc_8(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 0}, {"b": 1}]}
    ]
    spec = {"$inc": {"a.3.c": 5}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 0}, {"b": 1}, None, {"c": 5}]}


def test_update_inc_9(monty_update, mongo_update):
    docs = [
        {"a": [1, {"1": 2}, {"1": 3}]}
    ]
    spec = {"$inc": {"a.1.2": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, {"1": 2, "2": 1}, {"1": 3}]}


def test_update_inc_10(monty_update, mongo_update):
    docs = [
        {"a": [1, {"1": 2}]}
    ]
    spec = {"$inc": {"x.1.2": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, {"1": 2}], "x": {"1": {"2": 1}}}


def test_update_inc_positional_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3}, {"b": 4}]}
    ]
    spec = {"$inc": {"a.$.b": 1}}
    find = {"a.b": 4}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 3}, {"b": 5}]}


def test_update_inc_positional_all_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3}, {"b": 4}]}
    ]
    spec = {"$inc": {"a.$[].b": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 4}, {"b": 5}]}


def test_update_inc_positional_filtered_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.c": {"$gt": 0}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 5, "c": 1}, {"b": 4, "c": 0}]}


def test_update_inc_positional_filtered_2(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.c": {"$gt": 0}, "elem.b": {"$gt": 4}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [
        {"b": 4, "c": 1}, {"b": 6, "c": 1}, {"b": 4, "c": 0}]}


def test_update_inc_positional_filtered_3(monty_update, mongo_update):
    docs = [
        {"a": [5, 2]}
    ]
    spec = {"$inc": {"a.$[elem]": 10}}
    array_filters = [{"elem": {"$lt": 4}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [5, 12]}


def test_update_inc_float(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$inc": {"a": 1.5}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 2.5}


@skip_if_no_bson
def test_update_inc_int64(monty_update, mongo_update):
    docs = [
        {"a": bson.Int64(1)}
    ]
    spec = {"$inc": {"a": 1.5}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 2.5}


@skip_if_no_bson
def test_update_inc_decimal128(monty_update, mongo_update):
    docs = [
        {"a": bson.Decimal128("1.5")}
    ]
    spec = {"$inc": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": bson.Decimal128("2.5")}


def test_update_inc_null(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$inc": {"a": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_inc_bool(monty_update, mongo_update):
    docs = [
        {"a": True}
    ]
    spec = {"$inc": {"a": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
