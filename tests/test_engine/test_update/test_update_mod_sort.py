
import pytest
from collections import OrderedDict

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_mod_sort_1(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 60, 30, 10]}
    ]
    spec = {"$push": {"a": {"$each": [40, 70], "$sort": 1}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [10, 10, 20, 30, 40, 60, 70]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_2(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5}, {"x": 2}, {"x": 1}]}
    ]
    spec = {"$push": {"a": {"$each": [{"x": -3}], "$sort": {"x": 1}}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [{"x": -3}, {"x": 1}, {"x": 2}, {"x": 5}]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_3(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5}, 1, {"x": 2}, 5, True, 4]}
    ]
    spec = {"$push": {"a": {"$each": [{"x": -3}], "$sort": {"x": 1}}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [
        1, 5, True, 4, {"x": -3}, {"x": 2}, {"x": 5}
    ]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_4(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5}, 1, {"x": 2}, 5, True, 4]}
    ]
    spec = {"$push": {"a": {"$each": [{"x": -3}], "$sort": {"x": -1}}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [
        {"x": 5}, {"x": 2}, {"x": -3}, 1, 5, True, 4
    ]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_5(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 60]}
    ]
    spec = {"$push": {"a": {"$each": [], "$sort": []}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_sort_6(monty_update, mongo_update):
    docs = [
        {"a": [10, 20, 60]}
    ]
    spec = {"$push": {"a": {"$each": [], "$sort": 2}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_sort_7(monty_update, mongo_update):
    sort = OrderedDict()  # (NOTE): Preserve field order
    sort["x"] = -1
    sort["y"] = 1

    docs = [
        {"a": [{"x": 5, "y": 1}, {"x": 4, "y": 2}, {"x": 4, "y": 0}]}
    ]
    spec = {"$push": {"a": {"$each": [], "$sort": sort}}}

    mongo_c = mongo_update(docs, spec)
    assert next(mongo_c) == {"a": [
        {"x": 5, "y": 1}, {"x": 4, "y": 0}, {"x": 4, "y": 2}
    ]}
    mongo_c.rewind()

    monty_c = monty_update(docs, spec)
    assert next(mongo_c) == next(monty_c)


def test_update_mod_sort_8(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5, "y": 1}, {"x": 4, "y": 2}, {"x": 4, "y": 0}]}
    ]
    spec = {"$push": {"a": {"$each": [], "$sort": {"x": 3, "y": 1}}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_mod_sort_9(monty_update, mongo_update):
    docs = [
        {"a": [{"x": 5, "y": 1}, {"x": 4, "y": 2}, {"x": 4, "y": 0}]}
    ]
    spec = {"$push": {"a": {"$each": [], "$sort": {"x": {"z": 1}, "y": 1}}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
