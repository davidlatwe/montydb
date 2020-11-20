
def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_projection_slice_1(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": 1}, {"b": 3}, {"b": 0}, {"b": 8}]}
    ]
    spec = {"a.b": {"$gt": 2}}
    proj = {"a.b": {"$slice": 2}}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_slice_2(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1, 2, 5, 6]},
        {"a": [8, 1, 5]},
        {"a": [9, 0, 0, 2, 6]},
    ]
    spec = {}
    proj = {"a": {"$slice": [1, 4]}}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 3
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(3):
        assert next(mongo_c) == next(monty_c)


def test_projection_slice_3(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1, 2, 5, 6]},
        {"a": [8, 1, 5]},
        {"a": [9, 0, 0, 2, 6]},
    ]
    spec = {}
    proj = {"a": {"$slice": -3}}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 3
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(3):
        assert next(mongo_c) == next(monty_c)


def test_projection_slice_4(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1, 2, 3, 4, 5, 6, 7]}
    ]
    spec = {}
    proj = {"a": {"$slice": [5, 4]}}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_slice_5(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1, 2, 3, 4, 5, 6, 7]}
    ]
    spec = {}
    proj = {"a": {"$slice": [-5, 4]}}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_slice_6(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1, 2, 3, 4, 5, 6, 7], "x": 100}
    ]
    spec = {}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a": {"$slice": [-5, 4]}, "x": 1}
    run(proj)

    proj = {"a": {"$slice": [-5, 4]}, "x": 0}
    run(proj)
