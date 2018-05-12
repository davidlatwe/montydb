
from montydb.engine.base import FieldWalker

from bson.binary import Binary
from bson.code import Code
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.py3compat import PY3


def test_qop_eq_1(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 0}
    ]
    spec = {"a": 1}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [1]
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_eq_2(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 0}
    ]
    spec = {"a": {"$eq": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [1]
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_eq_3(monty_find, mongo_find):
    docs = [
        {"a": [1]},
        {"a": 1}
    ]
    spec = {"a": {"$eq": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [1, [1]]
    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_eq_4(monty_find, mongo_find):
    docs = [
        {"a": [1]},
        {"a": [[1], 2]}
    ]
    spec = {"a": {"$eq": [1]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[1])("a").value == [[1], 2, [[1], 2]]
    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_eq_5(monty_find, mongo_find):
    docs = [
        {"a": [2, 1]},
        {"a": [1, 2]},
        {"a": [[2, 1], 3]},
        {"a": [[1, 2], 3]},
    ]
    spec = {"a": {"$eq": [2, 1]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [2, 1, [2, 1]]
    assert FieldWalker(docs[2])("a").value == [[2, 1], 3, [[2, 1], 3]]
    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_eq_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": Binary(b"00")}]},
        {"a": [{"b": Binary(b"01")}]},
    ]
    spec = {"a.b": {"$eq": b"01"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    count = 1 if PY3 else 0
    assert mongo_c.count() == count
    assert monty_c.count() == mongo_c.count()
    if PY3:
        assert next(mongo_c) == next(monty_c)
        mongo_c.rewind()
        assert next(mongo_c)["_id"] == 1


def test_qop_eq_7(monty_find, mongo_find):
    docs = [
        {"a": [{"b": Code("a")}]},
    ]
    spec = {"a.b": {"$eq": "a"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_eq_8(monty_find, mongo_find):
    docs = [
        {"a": [{"b": "a"}]},
    ]
    spec = {"a.b": {"$eq": Code("a")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_eq_9(monty_find, mongo_find):
    docs = [
        {"a": 1},
    ]
    spec = {"a": {"$eq": Int64(1)}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_eq_10(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.0},
    ]
    spec = {"a": {"$eq": Decimal128("1")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()


def test_qop_eq_11(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.0},
    ]
    spec = {"a": {"$eq": Decimal128("1.0")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
