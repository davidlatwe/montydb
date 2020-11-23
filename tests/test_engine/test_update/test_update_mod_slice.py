
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_mod_slice_1(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": [20, 30, 10], "$slice": 2}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 20]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_slice_2(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": [20, 30, 10], "$slice": -2}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [30, 10]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_slice_3(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": [], "$slice": 0}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": []}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_slice_4(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 30, 40, 50]}
    ]
    spec = {"$push": {"a": {"$each": [], "$slice": -3}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [30, 40, 50]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_slice_5(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 30, 40, 50]}
    ]
    spec = {"$push": {"a": {"$each": [80, 90], "$slice": 4, "$position": 2}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 20, 80, 90]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_slice_6(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": [], "$slice": True}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
