
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err
from montydb.types import bson_ as bson

from ...conftest import skip_if_no_bson


def test_update_pullAll_1(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 1, 3]}
    ]
    spec = {"$pullAll": {"a": [1]}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [2, 3]}


def test_update_pullAll_2(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$pullAll": {"a": [1]}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pullAll_3(monty_update, mongo_update):
    docs = [
        {"a": [1, 2]}
    ]
    spec = {"$pullAll": {"a": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        next(monty_update(docs, spec))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_pullAll_4(monty_update, mongo_update):
    docs = [
        {}
    ]
    spec = {"$pullAll": {"a": [1, 2]}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {}


@skip_if_no_bson
def test_update_pullAll_5(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 2.0, bson.Decimal128("2.0"), 3]}
    ]
    spec = {"$pullAll": {"a": [2]}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 3]}


@skip_if_no_bson
def test_update_pullAll_6(monty_update, mongo_update):
    docs = [
        {"a": [1, 2, 2.0, 3]}
    ]
    spec = {"$pullAll": {"a": [bson.Decimal128("2.0")]}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [1, 3]}
