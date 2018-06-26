
def test_update_set_1(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$set": {"a": {"b": 1}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": 1}}


def test_update_set_2(monty_update, mongo_update):
    docs = [
        {"a": "some", "b": "happy", "c": "value"}
    ]
    spec = {"$set": {"a": "one", "c": "boy"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": "one", "b": "happy", "c": "boy"}


def test_update_set_3(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$set": {"a": None}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": None}


def test_update_set_4(monty_update, mongo_update):
    docs = [
        {"a": {"b": "some"}}
    ]
    spec = {"$set": {"a.b": "one"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": "one"}}


def test_update_set_5(monty_update, mongo_update):
    docs = [
        {"a": [1, {"b": 8}]}
    ]
    spec = {"$set": {"a.0": "one", "a.1.b": 12}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": ["one", {"b": 12}]}
