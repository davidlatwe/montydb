

def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_exists_1(monty_find, mongo_find):
    docs = [
        {"a": 1}
    ]
    spec = {"a": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    spec = {"a": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_exists_2(monty_find, mongo_find):
    docs = [
        {"b": 1}
    ]
    spec = {"a": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    spec = {"a": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_exists_3(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.0": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    spec = {"a.0": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_exists_4(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.1": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    spec = {"a.1": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_exists_5(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"c": 1}]}
    ]
    spec = {"a.b": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    spec = {"a.b": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_exists_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [0]}, {"b": [0, 1]}]}
    ]
    spec = {"a.b.1": {"$exists": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    spec = {"a.b.1": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_exists_7(monty_find, mongo_find):
    docs = [
        {"a": [{"b": [{"c": [0, {"d": []}]},
                      {"c": [2, {"d": [3, "y"]}]}]},
               {"b": [{"c": [10, {"d": [11, "i"]}]},
                      {"c": [12, {"d": [13, "j"]}]}]}
               ]}
    ]
    spec = {"a.b.c.1.d.2": {"$exists": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
