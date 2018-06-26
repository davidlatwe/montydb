

def test_upsert_1(monty_update, mongo_update):
    docs = [
        {"a": 0}
    ]
    spec = {"$inc": {"a.$[]": 9}}
    find = {"a": [1]}

    monty_c = monty_update(docs, spec, find, upsert=True)
    mongo_c = mongo_update(docs, spec, find, upsert=True)

    assert mongo_c[1] == monty_c[1]
    monty_c.rewind()
    assert monty_c[1] == {"a": [10]}


def test_upsert_2(monty_update, mongo_update):
    docs = [
        {"a": 0}
    ]
    spec = {"$inc": {"a": 9}}
    find = {"a": 6}

    monty_c = monty_update(docs, spec, find, upsert=True)
    mongo_c = mongo_update(docs, spec, find, upsert=True)

    assert mongo_c[1] == monty_c[1]
    monty_c.rewind()
    assert monty_c[1] == {"a": 15}


def test_upsert_3(monty_update, mongo_update):
    docs = [
        {"a": 0}
    ]
    spec = {"$inc": {"b": 9}}
    find = {"a.b": {"$gt": 6}}

    monty_c = monty_update(docs, spec, find, upsert=True)
    mongo_c = mongo_update(docs, spec, find, upsert=True)

    assert mongo_c[1] == monty_c[1]
    monty_c.rewind()
    assert monty_c[1] == {"b": 9}
