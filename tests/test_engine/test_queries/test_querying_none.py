
def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_none_query_1(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_none_query_1_ne(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_2(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a.0": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_none_query_2_ne(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a.0": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_3(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a.b.0": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)

    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 0


def test_none_query_3_ne(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a.b.0": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_4(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}]},
        {"a": [{"x": 1}]}
    ]
    spec = {"a.b": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)

    mongo_c.rewind()
    assert next(mongo_c)["_id"] == 1


def test_none_query_4_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}]},
        {"a": [{"x": 1}]}
    ]
    spec = {"a.b": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_5(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.1": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_5_ne(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.1": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"x": 1}]}
    ]
    spec = {"a.b": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_6_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"x": 1}]}
    ]
    spec = {"a.b": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_7(monty_find, mongo_find):
    docs = [
        {"a": [{"1": 1}, {"2": 1}]}
    ]
    spec = {"a.1": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_7_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"1": 1}, {"2": 1}]}
    ]
    spec = {"a.1": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_8(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.1": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_8_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.1": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_9(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.0": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_9_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.0": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_10(monty_find, mongo_find):
    docs = [
        {"a": [0, 1]}
    ]
    spec = {"a.b": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_10_ne(monty_find, mongo_find):
    docs = [
        {"a": [0, 1]}
    ]
    spec = {"a.b": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_11(monty_find, mongo_find):
    docs = [
        {"a": [True, False, {"1": False}]}
    ]
    spec = {"a.1.2": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_11_ne(monty_find, mongo_find):
    docs = [
        {"a": [True, False, {"1": False}]}
    ]
    spec = {"a.1.2": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_12(monty_find, mongo_find):
    docs = [
        {"a": [{}, {"b": "not_null"}]}
    ]
    spec = {"a.b": {"$eq": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_13(monty_find, mongo_find):
    docs = [
        {"a": [{"b": None}, {"b": "not_null"}]}
    ]
    spec = {"a.b": {"$eq": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_14(monty_find, mongo_find):
    docs = [
        {"a": [{"x": [None]}, {"b": "not_null"}]}
    ]
    spec = {"a.b": {"$eq": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_15(monty_find, mongo_find):
    docs = [
        {"a": [True, {'1': False}, False]}
    ]

    spec = {"a.1.1": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    spec = {"a.1.1": False}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_none_query_16(monty_find, mongo_find):
    docs = [
        {"a": [1, 2, 3]}
    ]
    spec = {"a.1.b": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(monty_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
