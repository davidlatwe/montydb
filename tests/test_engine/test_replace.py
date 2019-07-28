

def test_replace_one_1(monty_replace, mongo_replace):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"b": 1, "c": 1}
    replacement = {"x": {"y": "z"}}

    monty_c = monty_replace(docs, spec, replacement)
    mongo_c = mongo_replace(docs, spec, replacement)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
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

    assert mongo_c.count() == 3
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
    assert next(mongo_c) == next(monty_c)
