import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_mod_each_1(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": [20, 30, 10]}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 20, 30, 10]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_each_2(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$addToSet": {"a": {"$each": [20, 30, 10]}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 20, 30]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_each_3(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$set": [5], "$each": [20, 30, 10]}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_each_4(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$addToSet": {"a": {"$each": [5], "$sort": 1}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_each_5(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$addToSet": {"a": {"$sort": 1, "$each": [5]}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_each_6(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$addToSet": {"a": {"$each": None}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_each_7(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": None}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
