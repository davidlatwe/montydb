
from datetime import datetime
from bson.timestamp import Timestamp
from bson.max_key import MaxKey


def test_update_min_1(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$min": {"a": 0}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 0}


def test_update_min_2(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$min": {"a": True}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": True}


def test_update_min_3(monty_update, mongo_update):
    docs = [
        {"a": {"b": 5}, "0": None}
    ]
    spec = {"$min": {"a": {"b": 3}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": 3}, "0": None}


def test_update_min_4(monty_update, mongo_update):
    docs = [
        {"a": {"b": 5}}
    ]
    spec = {"$min": {"a": MaxKey()}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": 5}}


def test_update_min_5(monty_update, mongo_update):
    docs = [
        {"a": datetime(1998, 8, 22)}
    ]
    spec = {"$min": {"a": datetime(1000, 8, 22)}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": datetime(1000, 8, 22)}


def test_update_min_6(monty_update, mongo_update):
    docs = [
        {"a": Timestamp(10, 5)}
    ]
    spec = {"$min": {"a": Timestamp(10, 0)}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": Timestamp(10, 0)}
