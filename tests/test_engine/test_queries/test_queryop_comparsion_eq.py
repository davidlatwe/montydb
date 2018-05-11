
from montydb.engine.base import FieldWalker


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
