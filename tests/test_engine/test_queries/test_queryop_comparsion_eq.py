
from montydb.types import PY3, bson_ as bson

from ...conftest import skip_if_no_bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_eq_1(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 0}
    ]
    spec = {"a": 1}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_eq_2(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 0}
    ]
    spec = {"a": {"$eq": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_eq_3(monty_find, mongo_find):
    docs = [
        {"a": [1]},
        {"a": 1}
    ]
    spec = {"a": {"$eq": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
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

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
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

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_eq_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": bson.Binary(b"00")}]},
        {"a": [{"b": bson.Binary(b"01")}]},
    ]
    spec = {"a.b": {"$eq": b"01"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    count = 1 if PY3 else 0
    assert count_documents(mongo_c, spec) == count
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    if PY3:
        assert next(mongo_c) == next(monty_c)
        mongo_c.rewind()
        assert next(mongo_c)["_id"] == 1


@skip_if_no_bson
def test_qop_eq_7(monty_find, mongo_find):
    docs = [
        {"a": [{"b": bson.Code("a")}]},
    ]
    spec = {"a.b": {"$eq": "a"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_eq_8(monty_find, mongo_find):
    docs = [
        {"a": [{"b": "a"}]},
    ]
    spec = {"a.b": {"$eq": bson.Code("a")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_eq_9(monty_find, mongo_find):
    docs = [
        {"a": 1},
    ]
    spec = {"a": {"$eq": bson.Int64(1)}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_eq_10(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.0},
    ]
    spec = {"a": {"$eq": bson.Decimal128("1")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_eq_11(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.0},
    ]
    spec = {"a": {"$eq": bson.Decimal128("1.0")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_eq_12(monty_find, mongo_find):
    docs = [
        {"tags": [["ssl", "security"], "warning"]}
    ]
    spec = {"tags.0": "security"}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
