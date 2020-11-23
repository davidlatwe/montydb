
import re
from montydb.types import bson_ as bson
from ...conftest import skip_if_no_bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_not_1(monty_find, mongo_find):
    docs = [
        {"a": 4},
        {"x": 8}
    ]
    spec = {"a": {"$not": {"$eq": 8}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_not_2(monty_find, mongo_find):
    docs = [
        {"a": 6},
        {"a": [6]}
    ]
    spec = {"a": {"$not": {"$eq": 6}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_not_3(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 8}, {"b": 6}]},
    ]
    spec = {"a.b": {"$not": {"$in": [6]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_not_4(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
    ]
    spec = {"a": {"$not": bson.Regex("^a")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_not_5(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "banana"},
    ]
    spec = {"a": {"$not": re.compile("^a")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)
