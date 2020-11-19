
def count_documents(cursor, spec):
    return cursor.collection.count_documents(spec)


def test_find_1(monty_find, mongo_find):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"b": 1, "c": 1}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_find_2(monty_find, mongo_find):
    docs = [
        {"a": {"b": 1, "c": 1}},
        {"a": {"c": 1, "b": 1}},
    ]
    spec = {"a": {"b": 1, "c": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_find_3(monty_find, mongo_find):
    docs = [
        {"a": {"b": {"e": 1, "f": 2}, "c": 1}},
        {"a": {"b": {"f": 2, "e": 1}, "c": 1}},
    ]
    spec = {"a": {"b": {"e": 1, "f": 2}, "c": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)
