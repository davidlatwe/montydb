
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_push_1(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 3]}
    ]
    spec = {"$push": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 2, 3, 1]}


def test_update_push_2(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$push": {"a": 3}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_push_3(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$push": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1]}
