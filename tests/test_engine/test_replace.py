
def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_replace_one_1(monty_replace, mongo_replace):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"b": 1, "c": 1}
    replacement = {"x": {"y": "z"}}

    monty_c = monty_replace(docs, spec, replacement)
    mongo_c = mongo_replace(docs, spec, replacement)

    assert count_documents(mongo_c) == 2
    assert count_documents(monty_c) == count_documents(mongo_c)
    assert next(mongo_c) == next(monty_c)
    assert next(mongo_c) == next(monty_c)


def test_replace_one_2(monty_replace, mongo_replace):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"x": {"y": "z"}}
    replacement = {"x": {"y": "z"}}

    monty_c = monty_replace(docs, spec, replacement, upsert=True)
    mongo_c = mongo_replace(docs, spec, replacement, upsert=True)

    assert count_documents(mongo_c) == 3
    assert count_documents(monty_c) == count_documents(mongo_c)
    assert next(mongo_c) == next(monty_c)
    assert next(mongo_c) == next(monty_c)


def test_replace_one_with_dot(monty_replace, mongo_replace):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"b": 1, "c": 1}
    replacement = {"x": {"y": {".z": "?"}}}  # That `.z` will pass

    monty_c = monty_replace(docs, spec, replacement)
    mongo_c = mongo_replace(docs, spec, replacement)

    assert count_documents(mongo_c) == 2
    assert count_documents(monty_c) == count_documents(mongo_c)
    assert next(mongo_c) == next(monty_c)
    assert next(mongo_c) == next(monty_c)


def test_replace_one_upsert_with_dot(monty_replace, mongo_replace):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"x": {"y": "z"}}
    replacement = {"x": {"y": {".z": "?"}}}  # That `.z` will pass

    monty_c = monty_replace(docs, spec, replacement, upsert=True)
    mongo_c = mongo_replace(docs, spec, replacement, upsert=True)

    assert count_documents(mongo_c) == 3
    assert count_documents(monty_c) == count_documents(mongo_c)
    assert next(mongo_c) == next(monty_c)
    assert next(mongo_c) == next(monty_c)
