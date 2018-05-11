

from montydb.engine.base import FieldWalker


def test_qop_lt_1(monty_find, mongo_find):
    docs = [
        {"a": []},
        {"a": 2}
    ]
    spec = {"a": {"$lt": [None]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert FieldWalker(docs[0])("a").value == [[]]
    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
