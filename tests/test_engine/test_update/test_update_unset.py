
def test_update_unset_1(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$unset": {"a": ""}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {}


def test_update_unset_2(monty_update, mongo_update):
    docs = [
        {"a": "some", "b": "happy", "c": "value"}
    ]
    spec = {"$unset": {"a": "", "c": ""}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"b": "happy"}


def test_update_unset_3(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$unset": {"a.0": ""}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [None, 2]}


def test_update_unset_4(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$unset": {"a.3": ""}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 2]}


def test_update_unset_5(monty_update, mongo_update):
    docs = [
        {"a": {"b": "some"}}
    ]
    spec = {"$unset": {"a.b": ""}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {}}


def test_update_unset_6(monty_update, mongo_update):
    docs = [
        {"a": [1, {"b": 8}]}
    ]
    spec = {"$unset": {"a.0": "", "a.1.b": ""}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [None, {}]}
