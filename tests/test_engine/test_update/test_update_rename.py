
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_rename_1(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$rename": {"a": "b"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"b": 1}


def test_update_rename_2(monty_update, mongo_update):
    docs = [
        {"a": "some", "b": "happy", "c": "value"}
    ]
    spec = {"$rename": {"b": "a"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": "happy", "c": "value"}


def test_update_rename_3(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$rename": {"a": "b"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"b": [1, 2]}


def test_update_rename_4(monty_update, mongo_update):
    docs = [
        {"a": {"b": "some"}}
    ]
    spec = {"$rename": {"a.b": "a"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_rename_5(monty_update, mongo_update):
    docs = [
        {"a": [1, {"b": 8}]}
    ]
    spec = {"$rename": {"a.1.b": "b"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_rename_6(monty_update, mongo_update):
    docs = [
        {"a": {"b": 8}}
    ]
    spec = {"$rename": {"a.b": 5}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_rename_7(monty_update, mongo_update):
    docs = [
        {"a": {"b": "some"}, "c": []}
    ]
    spec = {"$rename": {"a.b": "c.0"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_rename_8(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$rename": {"a": "a"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_rename_9(monty_update, mongo_update):
    docs = [
        {"a": {"b": 8}}
    ]
    spec = {"$rename": {"a.b": "b"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {}, "b": 8}


def test_update_rename_10(monty_update, mongo_update):
    docs = [
        {"a": {"b": 5}, "b": 8}
    ]
    spec = {"$rename": {"b": "a.b"}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": 8}}


def test_update_rename_11(monty_update, mongo_update):
    docs = [
        {"a": [{"b": "some"}]}
    ]
    spec = {"$rename": {"a.0.b": "a"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
