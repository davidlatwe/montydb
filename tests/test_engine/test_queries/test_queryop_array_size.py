
import pytest
from montydb.errors import OperationFailure


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_size_1(monty_find, mongo_find):
    docs = [
        {"a": [0, 1, 2]},
        {"a": [0, 1]}
    ]
    spec = {"a": {"$size": 3}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_size_2(monty_find, mongo_find):
    docs = [
        {"a": [[0, 1], 1]},
        {"a": [0, [0, 1]]}
    ]
    spec = {"a.0": {"$size": 2}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_size_3(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [0, 1]}, {"b": [0]}]},
        {"a": [{"b": [0, 1, 2]}, {"b": []}]},
    ]
    spec = {"a.0.b": {"$size": 2}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_size_4(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [0, 1]}, {"b": [0]}]},
        {"a": [{"b": [0, 1, 2]}, {"b": []}]},
    ]
    spec = {"a.b": {"$size": 2}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_size_5(monty_find, mongo_find):
    docs = [
        {"a": [0, 1, 2]}
    ]
    spec = {"a": {"$size": 0.5}}  # $size must be a whole number

    monty_c = monty_find(docs, spec)

    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_size_6(monty_find, mongo_find):
    docs = [
        {"a": [0, 1, 2]}
    ]
    spec = {"a": {"$size": "not_num"}}  # $size needs a number

    monty_c = monty_find(docs, spec)

    with pytest.raises(OperationFailure):
        next(monty_c)
