
import pytest
import re

from montydb.errors import OperationFailure
from montydb.types import PY3, bson_ as bson

from ...conftest import skip_if_no_bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_in_1(monty_find, mongo_find):
    docs = [
        {"a": 0},
        {"a": 1}
    ]
    spec = {"a": {"$in": [0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_in_2(monty_find, mongo_find):
    docs = [
        {"a": [1, 0]},
        {"a": [1, 2]}
    ]
    spec = {"a": {"$in": [0, 2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_in_3(monty_find, mongo_find):
    docs = [
        {"a": {"1": 5}},
        {"a": [1, 2]}
    ]
    spec = {"a.1": {"$in": [5, 2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_in_4(monty_find, mongo_find):
    docs = [
        {"a": {"b": 5}},
        {"a": {"b": [2]}}
    ]
    spec = {"a.b": {"$in": [5, 2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_in_5(monty_find, mongo_find):
    docs = [
        {"a": {"b": [[0]]}},
        {"a": {"b": [2]}}
    ]
    spec = {"a.b": {"$in": [[2], [0]]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_in_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]}
    ]
    spec = {"a.b": {"$in": [2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_in_7(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]}
    ]
    spec = {"a.b": {"$in": [True]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_in_8(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}]},
        {"a": [{"x": 1}]},
    ]
    spec = {"a.b": {"$in": [None]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 1


@skip_if_no_bson
def test_qop_in_9(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
    ]
    spec = {"a": {"$in": [bson.Regex("^a")]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_in_10(monty_find, mongo_find):
    docs = [
        {"a": [bson.Regex("*")]},
    ]
    spec = {"a": {"$in": [[bson.Regex("*")]]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_in_11(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
    ]
    spec = {"a": {"$in": [re.compile("^a")]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_in_12(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
    ]
    spec = {"a": {"$in": [bson.Regex("*")]}}

    monty_c = monty_find(docs, spec)

    # Regular expression is invalid
    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_in_13(monty_find, mongo_find):
    docs = [
        {"a": 5},
    ]
    spec = {"a": {"$in": 5}}

    monty_c = monty_find(docs, spec)

    # $in needs an array
    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_in_14(monty_find, mongo_find):
    docs = [
        {"a": 5},
    ]
    spec = {"a": {"$in": [5, {"$exists": 1}]}}

    monty_c = monty_find(docs, spec)

    # cannot nest $ under $in
    with pytest.raises(OperationFailure):
        next(monty_c)


@skip_if_no_bson
def test_qop_in_15(monty_find, mongo_find):
    docs = [
        {"a": [{"b": bson.Binary(b"00")}]},
        {"a": [{"b": bson.Binary(b"01")}]},
    ]
    spec = {"a.b": {"$in": [b"01"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    count = 1 if PY3 else 0
    assert count_documents(mongo_c, spec) == count
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    if PY3:
        assert next(mongo_c) == next(monty_c)
        mongo_c.rewind()
        assert next(mongo_c)["_id"] == 1
