
from montydb.types import bson_ as bson
from ...conftest import skip_if_no_bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_mod_1(monty_find, mongo_find):
    docs = [
        {"a": 8}
    ]
    spec = {"a": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_2(monty_find, mongo_find):
    docs = [
        {"a": [8]}
    ]
    spec = {"a": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_3(monty_find, mongo_find):
    docs = [
        {"a": [3, [8]]}
    ]
    spec = {"a": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_mod_4(monty_find, mongo_find):
    docs = [
        {"a": bson.Int64(8)}
    ]
    spec = {"a": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_mod_5(monty_find, mongo_find):
    docs = [
        {"a": bson.Decimal128("8")}
    ]
    spec = {"a": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_mod_6(monty_find, mongo_find):
    docs = [
        {"a": 8}
    ]
    spec = {"a": {"$mod": [bson.Int64(4), 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_mod_7(monty_find, mongo_find):
    docs = [
        {"a": 8}
    ]
    spec = {"a": {"$mod": [bson.Decimal128("4"), 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_8(monty_find, mongo_find):
    docs = [
        {"a": 8}
    ]
    spec = {"a": {"$mod": [4, "NOT_NUM"]}}  # remainder set to 0

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_mod_9(monty_find, mongo_find):
    docs = [
        {"a": 9}
    ]
    spec = {"a": {"$mod": [4, bson.Int64("1")]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_mod_10(monty_find, mongo_find):
    docs = [
        {"a": 9}
    ]
    spec = {"a": {"$mod": [4, bson.Decimal128("1")]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_11(monty_find, mongo_find):
    docs = [
        {"a": 8}
    ]
    spec = {"a": {"$mod": [4, 0.5]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_12(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 5}, {"b": 8}]}
    ]
    spec = {"a.b": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_13(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 5}, {"b": [8]}]}
    ]
    spec = {"a.b": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_14(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 5}, {"b": [8]}]}
    ]
    spec = {"a.b": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_mod_15(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 5}, {"b": [8]}]}
    ]
    spec = {"a.b.0": {"$mod": [4, 0]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
