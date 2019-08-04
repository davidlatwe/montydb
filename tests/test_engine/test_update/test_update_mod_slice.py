
import pytest


def test_update_mod_slice_1(monty_update, mongo_update):
    docs = [
        {"a": [10]}
    ]
    spec = {"$push": {"a": {"$each": [20, 30, 10], "$slice": 2}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 20]}
    mongo_c.rewind()

    with pytest.raises(Exception):  # Remove this
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

    with pytest.raises(Exception):  # Remove this
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

    with pytest.raises(Exception):  # Remove this
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

    with pytest.raises(Exception):  # Remove this
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

    with pytest.raises(Exception):  # Remove this
        monty_c = monty_update(docs, spec)
        assert next(mongo_c) == next(monty_c)
