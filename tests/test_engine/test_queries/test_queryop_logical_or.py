

def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_or_1(monty_find, mongo_find):
    docs = [
        {"a": 4, "b": 6}
    ]
    spec = {"$or": [{"a": {"$gt": 2}}, {"b": {"$lt": 20}}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_or_2(monty_find, mongo_find):
    docs = [
        {"a": [0, 1], "b": True},
        {"a": [0, 1], "b": False}
    ]
    spec = {"$or": [{"a.2": {"$exists": 1}}, {"b": False}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 1


def test_qop_or_3(monty_find, mongo_find):
    docs = [
        {"a": [{"c": 1}, {"b": 6}]}
    ]
    spec = {"$or": [{"a.c": {"$exists": 0}}, {"a.b": {"$lt": 8}}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
