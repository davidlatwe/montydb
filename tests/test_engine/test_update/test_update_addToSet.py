
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_addToSet_1(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$addToSet": {"a": 3}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 2, 3]}


def test_update_addToSet_2(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$addToSet": {"a": [3]}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 2, [3]]}


def test_update_addToSet_3(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$addToSet": {"a": 3}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [3]}


def test_update_addToSet_4(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$addToSet": {"a": 3}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_addToSet_5(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 3]}
    ]
    spec = {"$addToSet": {"a": 3}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 2, 3]}
