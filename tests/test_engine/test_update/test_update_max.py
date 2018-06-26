
from datetime import datetime
from bson.timestamp import Timestamp
from bson.max_key import MaxKey


def test_update_max_1(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$max": {"a": 10}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 10}


def test_update_max_2(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$max": {"a": True}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 1}


def test_update_max_3(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$max": {"a": True}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": True}


def test_update_max_4(monty_update, mongo_update):
    docs = [
        {"a": {"b": 5}}
    ]
    spec = {"$max": {"a": MaxKey()}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": MaxKey()}


def test_update_max_5(monty_update, mongo_update):
    docs = [
        {"a": datetime(1998, 8, 22)}
    ]
    spec = {"$max": {"a": datetime(1000, 8, 22)}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": datetime(1998, 8, 22)}


def test_update_max_6(monty_update, mongo_update):
    docs = [
        {"a": Timestamp(10, 5)}
    ]
    spec = {"$max": {"a": Timestamp(10, 10)}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": Timestamp(10, 10)}
