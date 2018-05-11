
import pytest
from montydb.errors import OperationFailure

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

from bson.py3compat import PY3


def test_qop_gte_1(monty_find, mongo_find):
    docs = [
        {"a": 0},
        {"a": 1}
    ]
    spec = {"a": {"$gte": 0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_2(monty_find, mongo_find):
    docs = [
        {"a": "x"},
        {"a": "y"}
    ]
    spec = {"a": {"$gte": "x"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_3(monty_find, mongo_find):
    docs = [
        {"a": 10},
        {"a": "10"}
    ]
    spec = {"a": {"$gte": 10}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_4(monty_find, mongo_find):
    docs = [
        {"a": True},
        {"a": False}
    ]
    spec = {"a": {"$gte": False}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_5(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": False}
    ]
    spec = {"a": {"$gte": False}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_6(monty_find, mongo_find):
    docs = [
        {"a": [1, 2]},
        {"a": [3, 4]}
    ]
    spec = {"a": {"$gte": [2, 3]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_7(monty_find, mongo_find):
    docs = [
        {"a": {"b": 4}},
        {"a": {"b": 6}}
    ]
    spec = {"a": {"$gte": {"b": 5}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_8(monty_find, mongo_find):
    docs = [
        {"a": {"b": 4}},
        {"a": {"e": 4}}
    ]
    spec = {"a": {"$gte": {"c": 4}}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_9(monty_find, mongo_find):
    oid_0 = ObjectId(b"000000000000")
    oid_1 = ObjectId(b"000000000001")
    docs = [
        {"a": oid_0},
        {"a": oid_1}
    ]
    spec = {"a": {"$gte": oid_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_10(monty_find, mongo_find):
    dt_0 = datetime(1900, 1, 1)
    dt_1 = datetime(1900, 1, 2)
    docs = [
        {"a": dt_0},
        {"a": dt_1}
    ]
    spec = {"a": {"$gte": dt_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_11(monty_find, mongo_find):
    ts_0 = Timestamp(0, 1)
    ts_1 = Timestamp(1, 1)
    docs = [
        {"a": ts_0},
        {"a": ts_1}
    ]
    spec = {"a": {"$gte": ts_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_12(monty_find, mongo_find):
    min_k = MinKey()
    max_k = MaxKey()
    docs = [
        {"a": min_k},
        {"a": max_k}
    ]
    spec = {"a": {"$gte": min_k}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_13(monty_find, mongo_find):
    oid_0 = ObjectId(b"000000000000")
    max_k = MaxKey()
    min_k = MinKey()
    docs = [
        {"a": oid_0},
        {"a": max_k},
        {"a": min_k},
        {"a": 55},
    ]
    spec = {"a": {"$gte": max_k}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 4
    assert monty_c.count() == mongo_c.count()
    for i in range(4):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_14(monty_find, mongo_find):
    ts_0 = Timestamp(0, 1)
    dt_1 = datetime(1900, 1, 2)
    docs = [
        {"a": ts_0},
        {"a": dt_1}
    ]
    spec = {"a": {"$gte": ts_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_15(monty_find, mongo_find):
    docs = [
        {"a": [1]},
        {"a": 2}
    ]
    spec = {"a": {"$gte": 1}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_16(monty_find, mongo_find):
    docs = [
        {"a": [2, 3]},
        {"a": 2}
    ]
    spec = {"a": {"$gte": 2}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_17(monty_find, mongo_find):
    docs = [
        {"a": [1, 3]},
        {"a": 2}
    ]
    spec = {"a": {"$gte": [1]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_18(monty_find, mongo_find):
    docs = [
        {"a": [1, 3]},
        {"a": 2}
    ]
    spec = {"a": {"$gte": [2]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_gte_19(monty_find, mongo_find):
    docs = [
        {"a": [None]},
        {"a": 2}
    ]
    spec = {"a": {"$gte": []}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_20(monty_find, mongo_find):
    long_ = Int64(10)
    int_ = 10
    float_ = 10.0
    decimal_ = Decimal128("10.0")
    docs = [
        {"a": long_},
        {"a": int_},
        {"a": float_},
        {"a": decimal_}
    ]
    spec = {"a": {"$gte": 9.5}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 4
    assert monty_c.count() == mongo_c.count()
    for i in range(4):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_21(monty_find, mongo_find):
    docs = [
        {"a": Decimal128("1.1")},
        {"a": Decimal128("NaN")},
        {"a": Decimal128("-NaN")},
        {"a": Decimal128("sNaN")},
        {"a": Decimal128("-sNaN")},
        {"a": Decimal128("Infinity")}
    ]
    spec = {"a": {"$gte": Decimal128("0")}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_22(monty_find, mongo_find):
    bin_0 = Binary(b"0")
    bin_1 = Binary(b"1")
    byt_0 = b"0"
    byt_1 = b"1"
    docs = [
        {"a": bin_0},
        {"a": bin_1},
        {"a": byt_0},
        {"a": byt_1}
    ]
    spec = {"a": {"$gte": bin_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 4 if PY3 else 2
    assert monty_c.count() == mongo_c.count()
    r = 4 if PY3 else 2
    for i in range(r):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_23(monty_find, mongo_find):
    bin_0 = Binary(b"0")
    bin_1 = Binary(b"1")
    byt_0 = b"0"
    byt_1 = b"1"
    docs = [
        {"a": bin_0},
        {"a": bin_1},
        {"a": byt_0},
        {"a": byt_1}
    ]
    spec = {"a": {"$gte": byt_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 4 if PY3 else 2
    assert monty_c.count() == mongo_c.count()
    r = 4 if PY3 else 2
    for i in range(r):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_24(monty_find, mongo_find):
    code_0 = Code("0")
    code_1 = Code("1")
    docs = [
        {"a": code_0},
        {"a": code_1}
    ]
    spec = {"a": {"$gte": code_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_25(monty_find, mongo_find):
    code_0 = Code("0")
    code_1 = Code("1")
    code_1s = Code("1", {})
    docs = [
        {"a": code_1},
        {"a": code_1s}
    ]
    spec = {"a": {"$gte": code_0}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(mongo_c) == next(monty_c)


def test_qop_gte_26(monty_find, mongo_find):
    code_0s = Code("0", {})
    code_1s = Code("1", {})
    docs = [
        {"a": code_0s},
        {"a": code_1s}
    ]
    spec = {"a": {"$gte": code_0s}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_27(monty_find, mongo_find):
    code_1as = Code("1", {"a": 5})
    code_1bs = Code("1", {"b": 5})
    code_1cs = Code("1", {"c": 5})
    docs = [
        {"a": code_1as},
        {"a": code_1bs},
        {"a": code_1cs}
    ]
    spec = {"a": {"$gte": code_1bs}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_qop_gte_28(monty_find, mongo_find):
    regex_0 = Regex("^0")
    regex_a = Regex("^a")
    docs = [
        {"a": regex_a},
    ]
    spec = {"a": {"$gte": regex_0}}

    monty_c = monty_find(docs, spec)

    # Can't have RegEx as arg to predicate
    with pytest.raises(OperationFailure):
        next(monty_c)
