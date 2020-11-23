
import pytest
from montydb.errors import OperationFailure
from montydb.types import PY3, bson_ as bson
from datetime import datetime

from ...conftest import skip_if_no_bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_gt_1(monty_find, mongo_find):
    docs = [
        {"a": 0},
        {"a": 1}
    ]
    spec = {"a": {"$gt": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_2(monty_find, mongo_find):
    docs = [
        {"a": "x"},
        {"a": "y"}
    ]
    spec = {"a": {"$gt": "x"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_3(monty_find, mongo_find):
    docs = [
        {"a": 10},
        {"a": "10"}
    ]
    spec = {"a": {"$gt": 10}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_gt_4(monty_find, mongo_find):
    docs = [
        {"a": True},
        {"a": False}
    ]
    spec = {"a": {"$gt": False}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_5(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": False}
    ]
    spec = {"a": {"$gt": False}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_gt_6(monty_find, mongo_find):
    docs = [
        {"a": [1, 2]},
        {"a": [3, 4]}
    ]
    spec = {"a": {"$gt": [2, 3]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_7(monty_find, mongo_find):
    docs = [
        {"a": {"b": 4}},
        {"a": {"b": 6}}
    ]
    spec = {"a": {"$gt": {"b": 5}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_8(monty_find, mongo_find):
    docs = [
        {"a": {"b": 4}},
        {"a": {"e": 4}}
    ]
    spec = {"a": {"$gt": {"c": 4}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_9(monty_find, mongo_find):
    oid_0 = bson.ObjectId(b"000000000000")
    oid_1 = bson.ObjectId(b"000000000001")
    docs = [
        {"a": oid_0},
        {"a": oid_1}
    ]
    spec = {"a": {"$gt": oid_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_10(monty_find, mongo_find):
    dt_0 = datetime(1900, 1, 1)
    dt_1 = datetime(1900, 1, 2)
    docs = [
        {"a": dt_0},
        {"a": dt_1}
    ]
    spec = {"a": {"$gt": dt_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_11(monty_find, mongo_find):
    ts_0 = bson.Timestamp(0, 1)
    ts_1 = bson.Timestamp(1, 1)
    docs = [
        {"a": ts_0},
        {"a": ts_1}
    ]
    spec = {"a": {"$gt": ts_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_12(monty_find, mongo_find):
    min_k = bson.MinKey()
    max_k = bson.MaxKey()
    docs = [
        {"a": min_k},
        {"a": max_k}
    ]
    spec = {"a": {"$gt": min_k}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_13(monty_find, mongo_find, mongo_version):
    oid_0 = bson.ObjectId(b"000000000000")
    max_k = bson.MaxKey()
    min_k = bson.MinKey()
    docs = [
        {"a": oid_0},
        {"a": max_k},
        {"a": min_k},
        {"a": 55},
    ]
    spec = {"a": {"$gt": max_k}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    count = 3 if mongo_version[0] == 3 else 0

    assert count_documents(mongo_c, spec) == count
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(count):
        assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_14(monty_find, mongo_find):
    ts_0 = bson.Timestamp(0, 1)
    dt_1 = datetime(1900, 1, 2)
    docs = [
        {"a": ts_0},
        {"a": dt_1}
    ]
    spec = {"a": {"$gt": ts_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0  # They don't sort together
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_gt_15(monty_find, mongo_find):
    docs = [
        {"a": [1]},
        {"a": 2}
    ]
    spec = {"a": {"$gt": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_16(monty_find, mongo_find):
    docs = [
        {"a": [2, 3]},
        {"a": 2}
    ]
    spec = {"a": {"$gt": 2}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_17(monty_find, mongo_find):
    docs = [
        {"a": [1, 3]},
        {"a": 2}
    ]
    spec = {"a": {"$gt": [1]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_qop_gt_18(monty_find, mongo_find):
    docs = [
        {"a": [1, 3]},
        {"a": 2}
    ]
    spec = {"a": {"$gt": [2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_gt_19(monty_find, mongo_find):
    docs = [
        {"a": [None]},
        {"a": 2}
    ]
    spec = {"a": {"$gt": []}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_20(monty_find, mongo_find):
    long_ = bson.Int64(10)
    int_ = 10
    float_ = 10.0
    decimal_ = bson.Decimal128("10.0")
    docs = [
        {"a": long_},
        {"a": int_},
        {"a": float_},
        {"a": decimal_}
    ]
    spec = {"a": {"$gt": 9.5}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 4
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(4):
        assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_21(monty_find, mongo_find):
    docs = [
        {"a": bson.Decimal128("1.1")},
        {"a": bson.Decimal128("NaN")},
        {"a": bson.Decimal128("-NaN")},
        {"a": bson.Decimal128("sNaN")},
        {"a": bson.Decimal128("-sNaN")},
        {"a": bson.Decimal128("Infinity")}
    ]
    spec = {"a": {"$gt": bson.Decimal128("0")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_22(monty_find, mongo_find):
    bin_0 = bson.Binary(b"0")
    bin_1 = bson.Binary(b"1")
    byt_0 = b"0"
    byt_1 = b"1"
    docs = [
        {"a": bin_0},
        {"a": bin_1},
        {"a": byt_0},
        {"a": byt_1}
    ]
    spec = {"a": {"$gt": bin_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2 if PY3 else 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    if PY3:
        for i in range(2):
            assert next(mongo_c) == next(monty_c)
    else:
        assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_23(monty_find, mongo_find):
    bin_0 = bson.Binary(b"0")
    bin_1 = bson.Binary(b"1")
    byt_0 = b"0"
    byt_1 = b"1"
    docs = [
        {"a": bin_0},
        {"a": bin_1},
        {"a": byt_0},
        {"a": byt_1}
    ]
    spec = {"a": {"$gt": byt_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2 if PY3 else 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    if PY3:
        for i in range(2):
            assert next(mongo_c) == next(monty_c)
    else:
        assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_24(monty_find, mongo_find):
    code_0 = bson.Code("0")
    code_1 = bson.Code("1")
    docs = [
        {"a": code_0},
        {"a": code_1}
    ]
    spec = {"a": {"$gt": code_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_25(monty_find, mongo_find):
    code_0 = bson.Code("0")
    code_1 = bson.Code("1")
    code_1s = bson.Code("1", {})
    docs = [
        {"a": code_1},
        {"a": code_1s}
    ]
    spec = {"a": {"$gt": code_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_26(monty_find, mongo_find):
    code_0s = bson.Code("0", {})
    code_1s = bson.Code("1", {})
    docs = [
        {"a": code_0s},
        {"a": code_1s}
    ]
    spec = {"a": {"$gt": code_0s}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_27(monty_find, mongo_find):
    code_1as = bson.Code("1", {"a": 5})
    code_1bs = bson.Code("1", {"b": 5})
    code_1cs = bson.Code("1", {"c": 5})
    docs = [
        {"a": code_1as},
        {"a": code_1bs},
        {"a": code_1cs}
    ]
    spec = {"a": {"$gt": code_1bs}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


@skip_if_no_bson
def test_qop_gt_28(monty_find, mongo_find):
    regex_0 = bson.Regex("^0")
    regex_a = bson.Regex("^a")
    docs = [
        {"a": regex_a},
    ]
    spec = {"a": {"$gt": regex_0}}

    monty_c = monty_find(docs, spec)

    # Can't have RegEx as arg to predicate
    with pytest.raises(OperationFailure):
        next(monty_c)


@skip_if_no_bson
def test_qop_gt_29(monty_find, mongo_find):
    docs = [
        {"a": bson.Decimal128("1.1")},
        {"a": bson.Decimal128("NaN")},
        {"a": bson.Decimal128("-NaN")},
        {"a": bson.Decimal128("sNaN")},
        {"a": bson.Decimal128("-sNaN")},
        {"a": bson.Decimal128("Infinity")},
        {"a": 0},
        {"a": -10.0},
        {"a": 10.0},
    ]
    spec = {"a": {"$gt": bson.Decimal128("NaN")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_gt_30(monty_find, mongo_find):
    docs = [
        {"a": bson.Decimal128("1.1")},
        {"a": bson.Decimal128("NaN")},
        {"a": bson.Decimal128("-NaN")},
        {"a": bson.Decimal128("sNaN")},
        {"a": bson.Decimal128("-sNaN")},
        {"a": bson.Decimal128("Infinity")},
        {"a": 0},
        {"a": -10.0},
        {"a": 10.0},
    ]
    spec = {"a": {"$gt": bson.Decimal128("-NaN")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_gt_31(monty_find, mongo_find):
    docs = [
        {"a": bson.Decimal128("1.1")},
        {"a": bson.Decimal128("NaN")},
        {"a": bson.Decimal128("-NaN")},
        {"a": bson.Decimal128("sNaN")},
        {"a": bson.Decimal128("-sNaN")},
        {"a": bson.Decimal128("Infinity")},
        {"a": 0},
        {"a": -10.0},
        {"a": 10.0},
    ]
    spec = {"a": {"$gt": bson.Decimal128("Infinity")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_gt_32(monty_find, mongo_find):
    docs = [
        {"a": bson.Decimal128("1.1")},
        {"a": bson.Decimal128("NaN")},
        {"a": bson.Decimal128("-NaN")},
        {"a": bson.Decimal128("sNaN")},
        {"a": bson.Decimal128("-sNaN")},
        {"a": bson.Decimal128("Infinity")},
        {"a": 0},
        {"a": -10.0},
        {"a": 10.0},
    ]
    spec = {"a": {"$gt": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 3
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(3):
        assert next(mongo_c) == next(monty_c)
