import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_mod_position_1(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": [20, 30, 10], "$position": 0}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [20, 30, 10, 10]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_position_2(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 30]}
    ]
    spec = {"$push": {"a": {"$each": [70, 80], "$position": 2}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 20, 70, 80, 30]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_position_3(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 30]}
    ]
    spec = {"$push": {"a": {"$each": [70, 80], "$position": -2}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 70, 80, 20, 30]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_position_4(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 30]}
    ]
    spec = {"$addToSet": {"a": {"$each": [70, 30, 80], "$position": 2}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_position_5(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 30]}
    ]
    spec = {"$push": {"a": {"$each": [70, 80], "$position": True}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
