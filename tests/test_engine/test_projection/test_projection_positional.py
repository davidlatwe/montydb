

def test_projection_positional_1(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": 1}, {"b": 3}]}
    ]
    spec = {"a.b": {"$gt": 2}}
    proj = {"a.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_2(monty_proj, mongo_proj):
    docs = [
        {"a": 85, "b": [{"x": 1, "y": 5}, {"x": 5, "y": 12}]},
        {"a": 60, "b": [{"x": 4, "y": 8}, {"x": 0, "y": 6}]},
        {"a": 90, "b": [{"x": 2, "y": 12}, {"x": 3, "y": 7}]},
    ]
    spec = {"a": {"$gt": 80}, "b.x": {"$gt": 4}}
    proj = {"b.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_3(monty_proj, mongo_proj):
    docs = [
        {"a": [{"x": [1]}, {"x": [5]}]},
        {"a": [{"x": [4]}, {"x": [0]}]},
        {"a": [{"x": [2]}, {"x": [3]}]},
    ]
    spec = {"a.x": {"$gt": 4}}
    proj = {"a.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_4(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [1, 2, 3]}}
    ]
    spec = {"a.b": 2}
    proj = {"a.b.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_5(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [1, 2, 3], "c": [4, 5, 6]}},
        {"a": {"b": [1, 2, 3], "c": [4]}},
    ]
    spec = {"a.b": 2}
    proj = {"a.c.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_projection_positional_6(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [{"c": [1]}, {"c": [2]}, {"c": [3]}]}},
    ]
    spec = {"a.b.c": 2}
    proj = {"a.b.c.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_7(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [{"c": [1, 5]}, {"c": 2}, {"c": [3]}]}},
    ]
    spec = {"a.b.c": 2}
    proj = {"a.b.c.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_8(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]},
    ]
    spec = {"a.b.1": {"$eq": 6}}
    proj = {"a.b.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_9(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": 2}, {"b": [3]}]},
    ]
    spec = {"a.b.1": 5}
    proj = {"a.b.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)
