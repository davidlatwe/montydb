
import pytest
import re

from montydb.errors import OperationFailure
from montydb.types import Regex

from ...conftest import skip_if_no_bson


def test_qop_nin_1(monty_find, mongo_find):
    docs = [
        {"a": 0},
        {"a": 1}
    ]
    spec = {"a": {"$nin": [0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_nin_2(monty_find, mongo_find):
    docs = [
        {"a": [1, 0]},
        {"a": [1, 2]},
        {"a": 3},
    ]
    spec = {"a": {"$nin": [0, 2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 2


def test_qop_nin_3(monty_find, mongo_find):
    docs = [
        {"a": {"1": 5}},
        {"a": [1, 2]},
        {"a": 0},
    ]
    spec = {"a.1": {"$nin": [5, 2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 2


def test_qop_nin_4(monty_find, mongo_find):
    docs = [
        {"a": {"b": 5}},
        {"a": {"b": [2]}},
        {"a": {"c": [2, 5]}},
    ]
    spec = {"a.b": {"$nin": [5, 2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 2


def test_qop_nin_5(monty_find, mongo_find):
    docs = [
        {"a": {"b": [[0]]}},
        {"a": {"b": [2]}},
        {"a": {"b": 2}},
    ]
    spec = {"a.b": {"$nin": [[2], [0]]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 2


def test_qop_nin_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]},
        {"a": [{"b": 3}, {"b": 4}]},
        {"x": 5},
    ]
    spec = {"a.b": {"$nin": [2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_nin_7(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]}
    ]
    spec = {"a.b": {"$nin": [True]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_nin_8(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}]},
        {"a": [{"x": 1}]},
    ]
    spec = {"a.b": {"$nin": [None]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 0


@skip_if_no_bson
def test_qop_nin_9(monty_find, mongo_find):
    docs = [
        {"a": "banana"},
    ]
    spec = {"a": {"$nin": [Regex("^a")]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_nin_10(monty_find, mongo_find):
    docs = [
        {"a": [Regex("*")]},
    ]
    spec = {"a": {"$nin": [[Regex("*")]]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_nin_11(monty_find, mongo_find):
    docs = [
        {"a": "banana"},
    ]
    spec = {"a": {"$nin": [re.compile("^a")]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_nin_12(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
    ]
    spec = {"a": {"$nin": [Regex("*")]}}

    monty_c = monty_find(docs, spec)

    # Regular expression is invalid
    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_nin_13(monty_find, mongo_find):
    docs = [
        {"a": 5},
    ]
    spec = {"a": {"$nin": 5}}

    monty_c = monty_find(docs, spec)

    # $nin needs an array
    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_nin_14(monty_find, mongo_find):
    docs = [
        {"a": 5},
    ]
    spec = {"a": {"$nin": [5, {"$exists": 1}]}}

    monty_c = monty_find(docs, spec)

    # cannot nest $ under $nin
    with pytest.raises(OperationFailure):
        next(monty_c)
