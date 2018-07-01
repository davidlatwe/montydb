

def test_update_setOnInsert_1(monty_update, mongo_update):
    docs = []
    spec = {"$setOnInsert": {"a.$[]": 10}}
    find = {"a": [1]}

    monty_c = monty_update(docs, spec, find, upsert=True)
    mongo_c = mongo_update(docs, spec, find, upsert=True)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [10]}


def test_update_setOnInsert_2(monty_update, mongo_update):
    docs = []
    spec = {"$set": {"a": 9}, "$setOnInsert": {"b": 0}}
    find = {"a": {"$exists": 1}}

    monty_c = monty_update(docs, spec, find, upsert=True)
    mongo_c = mongo_update(docs, spec, find, upsert=True)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 9, "b": 0}


def test_update_setOnInsert_3(monty_update, mongo_update):
    docs = [
        {"a": 4}
    ]
    spec = {"$set": {"a": 9}, "$setOnInsert": {"b": 0}}
    find = {"a": {"$exists": 1}}

    monty_c = monty_update(docs, spec, find, upsert=True)
    mongo_c = mongo_update(docs, spec, find, upsert=True)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": 9}
