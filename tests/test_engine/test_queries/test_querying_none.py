
from montydb.engine.core import FieldWalker


def test_none_query_1(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
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

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_2(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a.0": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
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

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_3(monty_find, mongo_find):
    docs = [
        {"a": None},
        {"a": [None]}
    ]
    spec = {"a.b.0": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.b.0")
    assert field_walker_0.value == [None]
    assert field_walker_0.array_status_normal is False

    field_walker_1 = FieldWalker(docs[1])("a.b.0")
    assert field_walker_1.value == [None]
    assert field_walker_1.array_status_normal is True

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
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

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_4(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}]},
        {"a": [{"x": 1}]}
    ]
    spec = {"a.b": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.b")
    assert field_walker_0.value == [1]
    assert field_walker_0.array_field_missing is False

    field_walker_1 = FieldWalker(docs[1])("a.b")
    assert field_walker_1.value == [None]
    assert field_walker_1.array_field_missing is True

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
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

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_5(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.1": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.1")
    assert field_walker_0.value == [None]
    assert field_walker_0.array_field_missing is False
    assert field_walker_0.array_status_normal is True

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_5_ne(monty_find, mongo_find):
    docs = [
        {"a": [0]}
    ]
    spec = {"a.1": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_6(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"x": 1}]}
    ]
    spec = {"a.b": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.b")
    assert field_walker_0.value == [1]
    # not all embedded doc in array has filed "b"
    assert field_walker_0.array_field_missing is True
    assert field_walker_0.array_status_normal is False

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_6_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"b": 1}, {"x": 1}]}
    ]
    spec = {"a.b": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_7(monty_find, mongo_find):
    docs = [
        {"a": [{"1": 1}, {"2": 1}]}
    ]
    spec = {"a.1": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.1")
    assert field_walker_0.value == [1, {"2": 1}]
    # not all embedded doc in array has filed "1"
    assert field_walker_0.array_field_missing is True
    assert field_walker_0.array_status_normal is False

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_7_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"1": 1}, {"2": 1}]}
    ]
    spec = {"a.1": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_8(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.1": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.1")
    assert field_walker_0.value == [5]
    # not all embedded doc in array has filed "1"
    assert field_walker_0.array_field_missing is True
    assert field_walker_0.array_status_normal is False

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_8_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.1": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_9(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.0": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.0")
    assert field_walker_0.value == [1, {"0": 1}]
    assert field_walker_0.array_field_missing is False
    assert field_walker_0.array_status_normal is False

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_9_ne(monty_find, mongo_find):
    docs = [
        {"a": [{"0": 1}, 5]}
    ]
    spec = {"a.0": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_10(monty_find, mongo_find):
    docs = [
        {"a": [0, 1]}
    ]
    spec = {"a.b": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    field_walker_0 = FieldWalker(docs[0])("a.b")
    assert field_walker_0.value == [None]
    assert field_walker_0.array_field_missing is False
    assert field_walker_0.array_status_normal is True

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_none_query_10_ne(monty_find, mongo_find):
    docs = [
        {"a": [0, 1]}
    ]
    spec = {"a.b": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_11(monty_find, mongo_find):
    docs = [
        {"item": [True, False, {"1": False}]}
    ]
    spec = {"item.1.2": None}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_none_query_11_ne(monty_find, mongo_find):
    docs = [
        {"item": [True, False, {"1": False}]}
    ]
    spec = {"item.1.2": {"$ne": None}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()
