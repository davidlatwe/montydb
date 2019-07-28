

def test_delete_many_1(monty_delete, mongo_delete):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"b": 1, "c": 1}

    monty_c, monty_res = monty_delete(docs, spec)
    mongo_c, mongo_res = mongo_delete(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()

    assert mongo_res.deleted_count == 2
    assert monty_res.deleted_count == mongo_res.deleted_count


def test_delete_one_2(monty_delete, mongo_delete):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"b": 1, "c": 1}

    monty_c, monty_res = monty_delete(docs, spec, del_one=True)
    mongo_c, mongo_res = mongo_delete(docs, spec, del_one=True)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)

    assert mongo_res.deleted_count == 1
    assert monty_res.deleted_count == mongo_res.deleted_count
