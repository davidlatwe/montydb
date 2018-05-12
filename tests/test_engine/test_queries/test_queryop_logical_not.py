
import re
from bson.regex import Regex


def test_qop_not_1(monty_find, mongo_find):
    docs = [
        {"a": 4},
        {"x": 8}
    ]
    spec = {"a": {"$not": {"$eq": 8}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()


def test_qop_not_2(monty_find, mongo_find):
    docs = [
        {"a": 6},
        {"a": [6]}
    ]
    spec = {"a": {"$not": {"$eq": 6}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_not_3(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 8}, {"b": 6}]},
    ]
    spec = {"a.b": {"$not": {"$in": [6]}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_not_4(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
    ]
    spec = {"a": {"$not": Regex("^a")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_not_5(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "banana"},
    ]
    spec = {"a": {"$not": re.compile("^a")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)
