import pytest


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
