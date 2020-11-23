
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err
from montydb.types import bson_ as bson

from ...conftest import skip_if_no_bson


def test_update_mul_1(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$mul": {"a": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 2}


def test_update_mul_2(monty_update, mongo_update):
    docs = [
        {"a": [1]}
    ]
    spec = {"$mul": {"a": 2}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mul_3(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$mul": {"a": "2"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mul_4(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$mul": {"a.1": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 4]}


def test_update_mul_5(monty_update, mongo_update):
    docs = [
        {"a": {"b": 1}}
    ]
    spec = {"$mul": {"a.b": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": 2}}


def test_update_mul_6(monty_update, mongo_update):
    docs = [
        {"a": {"b": [1, 2]}}
    ]
    spec = {"$mul": {"a.b.1": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": [1, 4]}}


def test_update_mul_7(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 0}, {"b": 1}]}
    ]
    spec = {"$mul": {"a.b": 2}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mul_8(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 0}, {"b": 1}]}
    ]
    spec = {"$mul": {"a.3.c": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 0}, {"b": 1}, None, {"c": 0.0}]}


def test_update_mul_9(monty_update, mongo_update):
    docs = [
        {"a": [1, {"1": 2}, {"1": 3}]}
    ]
    spec = {"$mul": {"a.1.2": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, {"1": 2, "2": 0.0}, {"1": 3}]}


def test_update_mul_10(monty_update, mongo_update):
    docs = [
        {"a": [1, {"1": 2}]}
    ]
    spec = {"$mul": {"x.1.2": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, {"1": 2}], "x": {"1": {"2": 0.0}}}


def test_update_mul_positional_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3}, {"b": 4}]}
    ]
    spec = {"$mul": {"a.$.b": 2}}
    find = {"a.b": 4}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 3}, {"b": 8}]}


def test_update_mul_positional_all_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3}, {"b": 4}]}
    ]
    spec = {"$mul": {"a.$[].b": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 6}, {"b": 8}]}


def test_update_mul_positional_filtered_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$mul": {"a.$[elem].b": 2}}
    array_filters = [{"elem.c": {"$gt": 0}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 8, "c": 1}, {"b": 4, "c": 0}]}


def test_update_mul_positional_filtered_2(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$mul": {"a.$[elem].b": 2}}
    array_filters = [{"elem.c": {"$gt": 0}, "elem.b": {"$gt": 4}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [
        {"b": 4, "c": 1}, {"b": 10, "c": 1}, {"b": 4, "c": 0}]}


def test_update_mul_positional_filtered_3(monty_update, mongo_update):
    docs = [
        {"a": [5, 2]}
    ]
    spec = {"$mul": {"a.$[elem]": 10}}
    array_filters = [{"elem": {"$lt": 4}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [5, 20]}


def test_update_mul_float(monty_update, mongo_update):
    docs = [
        {"a": 2}
    ]
    spec = {"$mul": {"a": 1.5}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 3.0}


@skip_if_no_bson
def test_update_mul_int64(monty_update, mongo_update):
    docs = [
        {"a": bson.Int64(2)}
    ]
    spec = {"$mul": {"a": 1.5}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 3.0}


@skip_if_no_bson
def test_update_mul_decimal128(monty_update, mongo_update):
    docs = [
        {"a": bson.Decimal128("1.5")}
    ]
    spec = {"$mul": {"a": 2}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": bson.Decimal128("3.0")}


def test_update_mul_null(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$mul": {"a": 2}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mul_bool(monty_update, mongo_update):
    docs = [
        {"a": True}
    ]
    spec = {"$mul": {"a": 2}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
