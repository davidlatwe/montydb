
from montydb.types import bson_ as bson
from datetime import datetime

from ...conftest import skip_if_no_bson


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


@skip_if_no_bson
def test_update_min_4(monty_update, mongo_update):
    docs = [
        {"a": {"b": 5}}
    ]
    spec = {"$min": {"a": bson.MaxKey()}}

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


@skip_if_no_bson
def test_update_min_6(monty_update, mongo_update):
    docs = [
        {"a": bson.Timestamp(10, 5)}
    ]
    spec = {"$min": {"a": bson.Timestamp(10, 0)}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": bson.Timestamp(10, 0)}


def test_update_min_7(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$min": {"a": []}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": None}


def test_update_min_8(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$min": {"a": None}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": None}
