

def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_and_1(monty_find, mongo_find):
    docs = [
        {"a": 8, "b": 4}
    ]
    spec = {"$and": [{"a": {"$gt": 6}}, {"b": {"$lt": 5}}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_and_2(monty_find, mongo_find):
    docs = [
        {"a": [2, 4, 6], "b": True},
        {"a": [0, 1], "b": True}
    ]
    spec = {"$and": [{"a.2": {"$exists": 1}}, {"b": True}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)
