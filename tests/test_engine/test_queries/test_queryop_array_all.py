
import pytest
from montydb.engine.core import FieldWalker
from montydb.errors import OperationFailure


def test_qop_all_1(monty_find, mongo_find):
    docs = [
        {"a": [1, 2, 3]},
        {"a": [2, 5]}
    ]
    spec = {"a": {"$all": [2, 3]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [1, 2, 3, [1, 2, 3]]
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_all_2(monty_find, mongo_find):
    docs = [
        {"a": [1, 2, 3]},
        {"a": [2, 5]}
    ]
    spec = {"a": {"$all": [2, 3]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [1, 2, 3, [1, 2, 3]]
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_all_3(monty_find, mongo_find):
    docs = [
        {"a": [1, 2, 3]},
        {"a": [2, 3]}
    ]
    spec = {"a": {"$all": [2, 3]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [1, 2, 3, [1, 2, 3]]
    assert FieldWalker(docs[1])("a").value == [2, 3, [2, 3]]
    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_all_4(monty_find, mongo_find):
    docs = [
        {"a": [3, 1, 2]},
        {"a": [3, 2]}
    ]
    spec = {"a": {"$all": [2, 3]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [3, 1, 2, [3, 1, 2]]
    assert FieldWalker(docs[1])("a").value == [3, 2, [3, 2]]
    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_all_5(monty_find, mongo_find):
    docs = [
        {"a": [3, 1, 2]},
        {"a": [[2, 3], 1]},
        {"a": [[3, 2], 1]},
    ]
    spec = {"a": {"$all": [[2, 3]]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[1])("a").value == [[2, 3], 1, [[2, 3], 1]]
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_all_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]},
        {"a": [{"b": 6}, {"b": 8}]},
    ]
    spec = {"a": {"$all": [{"$elemMatch": {"b": {"$lt": 5}}}]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a.b").value == [1, 2]
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_all_7(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1, "c": False}, {"b": 2, "c": False}]},
        {"a": [{"b": 1, "c": True}, {"b": 2, "c": True}]}
    ]
    spec = {"a": {"$all": [{"$elemMatch": {"b": {"$lt": 5}}},
                           {"$elemMatch": {"c": False}}]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_all_8(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1, "c": False}, {"b": 2, "c": False}]},
        {"a": [{"b": 1, "c": True}, {"b": 2, "c": True}]}
    ]
    spec = {"a": {"$all": [{"$elemMatch": {"b": {"$lt": 5}, "c": False}}]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_all_9(monty_find, mongo_find):
    docs = [
        {"a": ["some-value"]}
    ]
    spec = {"a": {"$all": [{"$elemMatch": {}},
                           {"$in": []}]}}

    monty_c = monty_find(docs, spec)

    # $all/$elemMatch has to be consistent
    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_all_10(monty_find, mongo_find):
    docs = [
        {"a": ["some-value"]}
    ]
    spec = {"a": {"$all": [{"$in": []},
                           {"$elemMatch": {}}]}}

    monty_c = monty_find(docs, spec)

    # no $ expressions in $all
    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_all_11(monty_find, mongo_find):
    docs = [
        {"a": ["some-value"]}
    ]
    spec = {"a": {"$all": ["some-value",
                           {"$elemMatch": {"b": {"$lt": 5}}}]}}

    monty_c = monty_find(docs, spec)

    # no $ expressions in $all
    with pytest.raises(OperationFailure):
        next(monty_c)


def test_qop_all_12(monty_find, mongo_find):
    docs = [
        {"a": ["some-value"]}
    ]
    spec = {"a": {"$all": True}}

    monty_c = monty_find(docs, spec)

    # $all needs an array
    with pytest.raises(OperationFailure):
        next(monty_c)
