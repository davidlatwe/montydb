

import re

from datetime import datetime
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.regex import Regex
from bson.code import Code


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


def test_sort_5(monty_sort, mongo_sort):
    docs = [
        {"a": 0},
        {"a": 1.1},
        {"a": Int64(2)},
        {"a": Decimal128("3.3")}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_6(monty_sort, mongo_sort):
    docs = [
        {"a": Binary(b"00")},
        {"a": Binary(b"01")},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_7(monty_sort, mongo_sort):
    docs = [
        {"a": Code("x")},
        {"a": Code("x", {})},
        {"a": Code("x", {"m": 0})},
        {"a": Code("x", {"m": 1})},
        {"a": Code("x", {"n": 0})},
        {"a": Code("x", {"n": 1})},
        {"a": Code("y")},
        {"a": Code("y", {})},
        {"a": Code("y", {"m": 0})},
        {"a": Code("y", {"m": 1})},
        {"a": Code("y", {"n": 0})},
        {"a": Code("y", {"n": 1})},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_8(monty_sort, mongo_sort):
    docs = [
        {"a": MinKey()},
        {"a": MaxKey()}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_9(monty_sort, mongo_sort):
    docs = [
        {"a": ObjectId(b"000000000000")},
        {"a": ObjectId(b"000000000001")}
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


def test_sort_11(monty_sort, mongo_sort):
    docs = [
        {"a": Timestamp(0, 1)},
        {"a": Timestamp(1, 1)}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_12(monty_sort, mongo_sort):
    docs = [
        {"a": Regex("^a")},
        {"a": Regex("^b")}
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


def test_sort_13(monty_sort, mongo_sort):
    docs = [
        {"a": Regex("^a")},
        {"a": Regex("^a", "i")},
        {"a": Regex("^a", "ix")},
        {"a": Regex("^b")},
        {"a": Regex("^b", "i")},
        {"a": Regex("^b", "ix")},
        {"a": re.compile("^c")},
        {"a": re.compile("^c", re.IGNORECASE)},
        {"a": re.compile("^c", re.IGNORECASE | re.VERBOSE)},
        {"a": Regex("~a")},
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
        {"a": [Decimal128("4.5"), Binary(b"0")]},
        {"a": [{"s": 5}, False]},
        {"a": [{"s": 9}]},
        {"a": [True, "y"]},
        {"a": []},
    ]
    sort = [("a", -1)]

    monty_c = monty_sort(docs, sort)
    mongo_c = mongo_sort(docs, sort)

    for i in range(len(docs)):
        assert next(mongo_c)["_id"] == next(monty_c)["_id"]


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
        {"a": [Decimal128("4.5"), Binary(b"0")]},
        {"a": [{"s": 5}, False]},
        {"a": [{"s": 9}]},
        {"a": [True, "y"]},
        {"a": []},
        {"a": [Regex("^a", "ix")]},
        {"a": Regex("^b")},
        {"a": Code("x", {"m": 0})},
        {"a": Code("y")},
        {"a": Code("y", {})},
        {"a": Code("y", {"m": 0})},
        {"a": MinKey()},
        {"a": MaxKey()},
        {"a": Timestamp(0, 1)},
        {"a": Timestamp(1, 1)},
        {"a": ObjectId(b"000000000000")},
        {"a": ObjectId(b"000000000001")},
        {"a": datetime(1900, 1, 1)},
        {"a": datetime(1900, 1, 2)},
    ]
    sort = [("a", 1)]

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
