

def test_qop_nor_1(monty_find, mongo_find):
    docs = [
        {"a": 4, "b": 6}
    ]
    spec = {"$nor": [{"a": {"$gt": 6}}, {"b": {"$lt": 5}}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_nor_2(monty_find, mongo_find):
    docs = [
        {"a": [0, 1], "b": True},
        {"a": [0, 1], "b": False}
    ]
    spec = {"$nor": [{"a.2": {"$exists": 1}}, {"b": False}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)
    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 0


def test_qop_nor_3(monty_find, mongo_find):
    docs = [
        {"a": [0, 1]}
    ]
    spec = {"$nor": [{"a.2": {"$exists": 1}}, {"b": False}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_nor_4(monty_find, mongo_find):
    docs = [
        {"a": [0, 1]}
    ]
    spec = {"$nor": [{"a.b": 1}]}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
