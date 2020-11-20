
def count_documents(cursor):
    return cursor.collection.count_documents({})


def test_delete_many_1(monty_delete, mongo_delete):
    docs = [
        {"b": 1, "c": 1},
        {"c": 1, "b": 1},
    ]
    spec = {"b": 1, "c": 1}

    monty_c, monty_res = monty_delete(docs, spec)
    mongo_c, mongo_res = mongo_delete(docs, spec)

    assert count_documents(mongo_c) == 0
    assert count_documents(monty_c) == count_documents(mongo_c)

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

    assert count_documents(mongo_c) == 1
    assert count_documents(monty_c) == count_documents(mongo_c)
    assert next(mongo_c) == next(monty_c)

    assert mongo_res.deleted_count == 1
    assert monty_res.deleted_count == mongo_res.deleted_count
