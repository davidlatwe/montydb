
import pytest


def test_update_mod_sort_1(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 60, 30, 10]}
    ]
    spec = {"$push": {"a": {"$each": [40, 70], "$sort": 1}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 10, 20, 30, 40, 60, 70]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_2(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5}, {"x": 2}, {"x": 1}]}
    ]
    spec = {"$push": {"a": {"$each": [{"x": -3}], "$sort": {"x": 1}}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [{"x": -3}, {"x": 1}, {"x": 2}, {"x": 5}]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_3(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5}, 1, {"x": 2}, 5, True, 4]}
    ]
    spec = {"$push": {"a": {"$each": [{"x": -3}], "$sort": {"x": 1}}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [
        1, 5, True, 4, {"x": -3}, {"x": 2}, {"x": 5}
    ]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_4(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5}, 1, {"x": 2}, 5, True, 4]}
    ]
    spec = {"$push": {"a": {"$each": [{"x": -3}], "$sort": {"x": -1}}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [
        {"x": 5}, {"x": 2}, {"x": -3}, 1, 5, True, 4
    ]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)
