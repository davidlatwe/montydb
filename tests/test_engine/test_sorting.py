
import pytest
import re

from montydb.types import bson_ as bson
from datetime import datetime

from montydb.errors import OperationFailure as monty_op_err
from pymongo.errors import OperationFailure as mongo_op_err

from ..conftest import skip_if_no_bson


def test_sort_1(monty_sort, mongo_sort):
    docs = [
        {"a": 4},
        {"a": 5},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_2(monty_sort, mongo_sort):
    docs = [
        {"a": [1, 2, 3]},
        {"a": [2, 3, 4]},
    ]
    sort = [("a", 1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_3(monty_sort, mongo_sort):
    docs = [
        {"a": [1, 2, 3]},
        {"a": [2, 3, 4]},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_4(monty_sort, mongo_sort):
    docs = [
        {"a": []},
        {"a": None},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_5(monty_sort, mongo_sort):
    docs = [
        {"a": 0},
        {"a": 1.1},
        {"a": bson.Int64(2)},
        {"a": bson.Decimal128("3.3")}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_6(monty_sort, mongo_sort):
    docs = [
        {"a": bson.Binary(b"00")},
        {"a": bson.Binary(b"01")},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_7(monty_sort, mongo_sort):
    docs = [
        {"a": bson.Code("x")},
        {"a": bson.Code("x", {})},
        {"a": bson.Code("x", {"m": 0})},
        {"a": bson.Code("x", {"m": 1})},
        {"a": bson.Code("x", {"n": 0})},
        {"a": bson.Code("x", {"n": 1})},
        {"a": bson.Code("y")},
        {"a": bson.Code("y", {})},
        {"a": bson.Code("y", {"m": 0})},
        {"a": bson.Code("y", {"m": 1})},
        {"a": bson.Code("y", {"n": 0})},
        {"a": bson.Code("y", {"n": 1})},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_8(monty_sort, mongo_sort):
    docs = [
        {"a": bson.MinKey()},
        {"a": bson.MaxKey()}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_9(monty_sort, mongo_sort):
    docs = [
        {"a": bson.ObjectId(b"000000000000")},
        {"a": bson.ObjectId(b"000000000001")}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_10(monty_sort, mongo_sort):
    docs = [
        {"a": datetime(1900, 1, 1)},
        {"a": datetime(1900, 1, 2)}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_11(monty_sort, mongo_sort):
    docs = [
        {"a": bson.Timestamp(0, 1)},
        {"a": bson.Timestamp(1, 1)}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_12(monty_sort, mongo_sort):
    docs = [
        {"a": bson.Regex("^a")},
        {"a": bson.Regex("^b")}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_13(monty_sort, mongo_sort):
    docs = [
        {"a": bson.Regex("^a")},
        {"a": bson.Regex("^a", "i")},
        {"a": bson.Regex("^a", "ix")},
        {"a": bson.Regex("^b")},
        {"a": bson.Regex("^b", "i")},
        {"a": bson.Regex("^b", "ix")},
        {"a": re.compile("^c")},
        {"a": re.compile("^c", re.IGNORECASE)},
        {"a": re.compile("^c", re.IGNORECASE | re.VERBOSE)},
        {"a": bson.Regex("~a")},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_14(monty_sort, mongo_sort):
    docs = [
        {"a": "x", "b": 0},
        {"a": "x", "b": 1},
        {"a": "y", "b": 0},
        {"a": "y", "b": 1},
        {"a": "z", "b": 0},
        {"a": "z", "b": 1},
    ]
    sort = [("a", -1), ("b", 1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_15(monty_sort, mongo_sort):
    docs = [
        {"a": {"x": 10}, "b": 0},
        {"a": {"x": 11}, "b": 1},
        {"a": {"x": 10}, "b": 0},
        {"a": {"x": 11}, "b": 1},
        {"a": {"x": 10}, "b": 0},
        {"a": {"x": 11}, "b": 1},
    ]
    sort = [("a.x", -1), ("b", 1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_16(monty_sort, mongo_sort):
    docs = [
        {"a": {"x": [6, 10]}, "b": 0},
        {"a": {"x": [5, 11]}, "b": 1},
        {"a": {"x": [6, 10]}, "b": 0},
        {"a": {"x": [5, 11]}, "b": 1},
        {"a": {"x": [6, 10]}, "b": 0},
        {"a": {"x": [5, 11]}, "b": 1},
    ]
    sort = [("a.x.1", -1), ("b", 1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_17(monty_sort, mongo_sort):
    docs = [
        {"a": [{"b": 1}, {"b": 2}]},
        {"a": [{"b": 0}, {"b": 4}]},
        {"a": [{"b": 5}, {"b": 8}]},
        {"a": [{"b": 0}, {"b": 6}]},
    ]
    sort = [("a.b", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_18(monty_sort, mongo_sort):
    docs = [
        {"a": ["x", True]},
        {"a": None},
        {"a": []},
        {"a": [5, []]},
        {"a": {"s": 7}},
        {"a": {"s": [9]}},
        {"a": {"s": 10}},
        {"a": 6},
        {"a": 4},
        {"a": [5, None]},
        {"a": [5, [1]]},
        {"a": [bson.Decimal128("4.5"), bson.Binary(b"0")]},
        {"a": [{"s": 5}, False]},
        {"a": [{"s": 9}]},
        {"a": [True, "y"]},
        {"a": []},
    ]
    sort = [("_id", 1), ("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


@skip_if_no_bson
def test_sort_19(monty_sort, mongo_sort):
    docs = [
        {"a": ["x", True]},
        {"a": None},
        {"a": []},
        {"a": [5, []]},
        {"a": {"s": 7}},
        {"a": {"s": [9]}},
        {"a": {"s": 10}},
        {"a": 6},
        {"a": 4},
        {"a": [5, None]},
        {"a": [5, [1]]},
        {"a": [bson.Decimal128("4.5"), bson.Binary(b"0")]},
        {"a": [{"s": 5}, False]},
        {"a": [{"s": 9}]},
        {"a": [True, "y"]},
        {"a": bson.Binary(b"a")},
        {"a": b"bytes"},
        {"a": ["abc"]},
        {"a": "banana"},
        {"a": "appple"},
        {"a": [bson.Regex("^a", "ix")]},
        {"a": bson.Regex("^b")},
        {"a": bson.Code("x", {"m": 0})},
        {"a": bson.Code("y")},
        {"a": bson.Code("y", {})},
        {"a": bson.Code("y", {"m": 0})},
        {"a": bson.MinKey()},
        {"a": bson.MaxKey()},
        {"a": bson.Timestamp(0, 1)},
        {"a": bson.Timestamp(1, 1)},
        {"a": bson.ObjectId(b"000000000000")},
        {"a": bson.ObjectId(b"000000000001")},
        {"a": datetime(1900, 1, 1)},
        {"a": datetime(1900, 1, 2)},
    ]
    sort = [("_id", 1), ("a", 1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_20(monty_sort, mongo_sort):
    docs = [
        {"a": []},
        {"a": [{"b": 1}]},
        {"a": [[]]},
        {"a": [None]},
        {"a": None},  # won't be exists
        {"a": "b"},  # won't be exists
        {"a": ["b"]},
        {"a": [5]},
    ]
    sort = [("a.0", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_21(monty_sort, mongo_sort):
    docs = [
        {"a": 4},
        {"a": 5},
    ]
    sort = [("a", 3)]

    with pytest.raises(mongo_op_err) as mongo_err:
        mongo_c = mongo_sort(docs, sort)
        next(mongo_c)

    with pytest.raises(monty_op_err) as monty_err:
        monty_c = monty_sort(docs, sort)
        next(monty_c)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
