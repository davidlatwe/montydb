
from montydb.engine.base import FieldWalker


def test_qop_exists_1(monty_find, mongo_find):
    docs = [
        {"a": 1}
    ]
    spec = {"a": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").exists is True
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()

    spec = {"a": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_exists_2(monty_find, mongo_find):
    docs = [
        {"b": 1}
    ]
    spec = {"a": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").exists is False
    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()

    spec = {"a": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_exists_3(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.0": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").exists is True
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()

    spec = {"a.0": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_exists_4(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.1": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a.1").exists is False
    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()

    spec = {"a.1": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_exists_5(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"c": 1}]}
    ]
    spec = {"a.b": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a.b").exists is True
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()

    spec = {"a.b": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_exists_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [0]}, {"b": [0, 1]}]}
    ]
    spec = {"a.b.1": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a.b.1").exists is True
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()

    spec = {"a.b.1": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()
