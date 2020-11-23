
from montydb.types import bson_ as bson
from datetime import datetime

from ...conftest import skip_if_no_bson


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


@skip_if_no_bson
def test_update_max_4(monty_update, mongo_update):
    docs = [
        {"a": {"b": 5}}
    ]
    spec = {"$max": {"a": bson.MaxKey()}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": bson.MaxKey()}


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


@skip_if_no_bson
def test_update_max_6(monty_update, mongo_update):
    docs = [
        {"a": bson.Timestamp(10, 5)}
    ]
    spec = {"$max": {"a": bson.Timestamp(10, 10)}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": bson.Timestamp(10, 10)}


def test_update_max_7(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$max": {"a": []}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": []}


def test_update_max_8(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$max": {"a": None}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": None}
