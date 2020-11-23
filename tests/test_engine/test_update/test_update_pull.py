
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_pull_1(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 3]}
    ]
    spec = {"$pull": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [2, 3]}


def test_update_pull_2(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$pull": {"a": 3}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pull_3(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$pull": {"a": 1}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {}


def test_update_pull_4(monty_update, mongo_update):
    docs = [
        {"a": [[1, 2], [2, 1]]}
    ]
    spec = {"$pull": {"a": [1, 2]}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [[2, 1]]}


def test_update_pull_5(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 1, "c": 1}, {"c": 1, "b": 1}]}
    ]
    spec = {"$pull": {"a": {"b": 1, "c": 1}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": []}


def test_update_pull_6(monty_update, mongo_update):
    docs = [
        {"a": [{"x": {"b": 1, "c": 1}}, {"x": {"c": 1, "b": 1}}]}
    ]
    spec = {"$pull": {"a": {"x": {"b": 1, "c": 1}}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    # result may differ depend on dict order
    assert next(mongo_c) == next(monty_c)


def test_update_pull_7(monty_update, mongo_update):
    docs = [
        {"a": [{"x": {"b": 1, "c": 1}, "y": 1},
               {"x": {"c": 1, "b": 1}, "y": 1}]},
        {"a": [{"y": 1, "x": {"b": 1, "c": 1}},
               {"y": 1, "x": {"c": 1, "b": 1}}]}
    ]
    spec = {"$pull": {"a": {"x": {"b": 1, "c": 1}, "y": 1}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    # result may differ depend on dict order
    for doc in mongo_c:
        assert doc == next(monty_c)


def test_update_pull_8(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 3, 4]}
    ]
    spec = {"$pull": {"a": {"$gt": 2}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 2]}


def test_update_pull_9(monty_update, mongo_update):
    docs = [
        {"a": ["q", "w", "e", "r"], "b": ["z", "x", "c", "v"]}
    ]
    spec = {"$pull": {"a": {"$in": ["e", "w"]}, "b": "x"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": ["q", "r"], "b": ["z", "c", "v"]}


def test_update_pull_10(monty_update, mongo_update):
    docs = [
        {"a": [{"i": "A", "s": 5}, {"i": "B", "s": 8, "c": "blah"}]}
    ]
    spec = {"$pull": {"a": {"s": 8, "i": "B"}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"i": "A", "s": 5}]}


def test_update_pull_11(monty_update, mongo_update):
    docs = [
        {"a": [{"i": "A", "s": 5}, {"i": "B", "s": 8, "c": "blah"}]}
    ]
    spec = {"$pull": {"a": {"$elemMatch": {"s": 8, "i": "B"}}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"i": "A", "s": 5},
                                   {"i": "B", "s": 8, "c": "blah"}]}


def test_update_pull_12(monty_update, mongo_update):
    docs = [
        {"a": [{"w": [{"q": 1, "a": 4}, {"q": 2, "a": 6}]},
               {"w": [{"q": 1, "a": 8}, {"q": 2, "a": 9}]}]}
    ]
    spec = {"$pull": {"a": {"w": {"$elemMatch": {"q": 2, "a": {"$gte": 8}}}}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"w": [{"q": 1, "a": 4},
                                          {"q": 2, "a": 6}]}]}
