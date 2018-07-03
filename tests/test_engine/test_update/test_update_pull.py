
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_pull_1(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 3]}
    ]
    spec = {"$pull": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [2, 3]}


def test_update_pull_2(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$pull": {"a": 3}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    assert mongo_err.value.code == monty_err.value.code


def test_update_pull_3(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$pull": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {}


def test_update_pull_4(monty_update, mongo_update):
    docs = [
        {"a": [[1, 2], [2, 1]]}
    ]
    spec = {"$pull": {"a": [1, 2]}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [[2, 1]]}


def test_update_pull_5(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 1, "c": 1}, {"c": 1, "b": 1}]}
    ]
    spec = {"$pull": {"a": {"b": 1, "c": 1}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": []}  # {"c": 1, "b": 1}


def test_update_pull_6(monty_update, mongo_update):
    docs = [
        {"a": [{"x": {"b": 1, "c": 1}}, {"x": {"c": 1, "b": 1}}]}
    ]
    spec = {"$pull": {"a": {"x": {"b": 1, "c": 1}}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"x": {"c": 1, "b": 1}}]}  # {"c": 1, "b": 1}
