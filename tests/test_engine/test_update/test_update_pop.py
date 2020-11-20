
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_pop_1(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 3]}
    ]
    spec = {"$pop": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 2]}


def test_update_pop_2(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 3]}
    ]
    spec = {"$pop": {"a": -1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [2, 3]}


def test_update_pop_3(monty_update, mongo_update):
    docs = []
    spec = {"$pop": {"a": 3}}  # Expected an integer

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pop_4(monty_update, mongo_update):
    from bson.decimal128 import Decimal128
    docs = []
    # Cannot represent as a 64-bit integer
    spec = {"$pop": {"a": Decimal128("1.1")}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pop_5(monty_update, mongo_update):
    docs = []
    spec = {"$pop": {"a": "not num"}}  # Expected a number

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pop_6(monty_update, mongo_update):
    docs = [
        {"a": {"b": "not array"}}
    ]
    spec = {"$pop": {"a.b": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pop_7(monty_update, mongo_update):
    docs = [
        {"a": []}
    ]
    spec = {"$pop": {"a": True}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pop_8(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$pop": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {}
