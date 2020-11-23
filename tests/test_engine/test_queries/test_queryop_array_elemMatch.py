
import pytest
from montydb.errors import OperationFailure


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_elemMatch_1(monty_find, mongo_find):
    docs = [
        {"a": [3, 2, 1]},
        {"a": [4, 5]}
    ]
    spec = {"a": {"$elemMatch": {"$eq": 1}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_2(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]},
        {"a": [{"b": 3}, {"b": 4}]},
    ]
    spec = {"a": {"$elemMatch": {"b": 1}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_3(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [10, 11]}, {"b": 2}]},
        {"a": [{"b": [20, 21]}, {"b": 4}]},
    ]
    spec = {"a.0": {"$elemMatch": {"b": {"$gt": 20}}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_elemMatch_4(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]},
        {"a": [{"b": 3}, {"b": 4}]},
    ]
    spec = {"a.0.b": {"$elemMatch": {"$eq": 1}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_elemMatch_5(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [1]}, {"b": 2}]},
        {"a": [{"b": 3}, {"b": 4}]},
    ]
    spec = {"a.0.b": {"$elemMatch": {"$eq": 1}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_6(monty_find, mongo_find):
    docs = [
        {"a": [75, 82]},
        {"a": [75, 88]},
    ]
    spec = {"a": {"$elemMatch": {"$gte": 80, "$lt": 85}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_7(monty_find, mongo_find):
    docs = [
        {"a": [{"b": "x", "c": 9}, {"b": "z", "c": 8}]},
        {"a": [{"b": "x", "c": 8}, {"b": "z", "c": 6}]},
    ]
    spec = {"a": {"$elemMatch": {"b": "z", "c": {"$gte": 8}}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_8(monty_find, mongo_find):
    docs = [
        {"a": [{"b": "x", "c": 9}, {"b": "z", "c": 8}]},
        {"a": [{"b": "x", "c": 8}, {"b": "z", "c": 6}]},
        {"a": [{"b": "y", "c": 8}, {"b": "z", "c": 7}]},
    ]
    spec = {"a": {"$elemMatch": {"$or": [{"b": "x"}, {"c": 6}]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_9(monty_find, mongo_find):
    docs = [
        {"a": [[[1, 2], True], [[1, 2], True]]},
        {"a": [[[1, 0], True], [[1, 3], False]]},
    ]
    spec = {"a": {"$elemMatch": {"$or": [{"0": [1, 0]}, {"1": False}]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_10(monty_find, mongo_find):
    docs = [
        {"a": [[[1, 2], True], [[1, 2], True], {"0": [1, 0], "1": False}]},
        {"a": [[[1, 2], True], [[1, 2], True], {"0": [1, 0]}]},
        {"a": [[[1, 0], True], [[1, 3], False]]},
    ]
    spec = {"a": {"$elemMatch": {"$or": [{"0": [1, 0]}, {"1": False}]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 3
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(3):
        assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_11(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [10, 11]}, {"b": 2}]},  # won't get picked
        {"a": [[{"b": [10, 11]}], {"b": 2}]},
        {"a": [[{"b": [20, 21]}], {"b": 4}]},
    ]
    spec = {"a.0": {"$elemMatch": {"$and": [{"b": [10, 11]}]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_12(monty_find, mongo_find):
    docs = [
        {"a": [[{"b": [10, 11]}]]},
        {"a": [[{"b": [20, 21]}]]},
    ]
    spec = {"a.0.b": {"$elemMatch": {"$and": [{"0": 10}]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_elemMatch_13(monty_find, mongo_find):
    docs = [
        {"a": [[{"b": [[10], 11]}]]},
        {"a": [[{"b": [[20], 21]}]]},
    ]
    spec = {"a.0.b": {"$elemMatch": {"$and": [{"0": 10}]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_elemMatch_14(monty_find, mongo_find):
    docs = [
        {"a": [3, 2, 1]}
    ]
    spec = {"a": {"$elemMatch": True}}  # $elemMatch needs an Object

    monty_c = monty_find(docs, spec)

    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_elemMatch_15(monty_find, mongo_find):
    docs = [
        {"a": ["some"]},
    ]
    spec = {"a": {"$elemMatch": {"$exists": True}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)
