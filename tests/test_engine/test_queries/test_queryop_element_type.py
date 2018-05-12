
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


def test_qop_type_1(monty_find, mongo_find):
    docs = [
        {"a": 0.1}
    ]
    spec = {"a": {"$type": 1}}  # double

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_2(monty_find, mongo_find):
    docs = [
        {"a": "string"}
    ]
    spec = {"a": {"$type": 2}}  # string

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_3(monty_find, mongo_find):
    docs = [
        {"a": {"doc": "object"}}
    ]
    spec = {"a": {"$type": 3}}  # object

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_4(monty_find, mongo_find):
    docs = [
        {"a": []}
    ]
    spec = {"a": {"$type": 4}}  # array

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_5(monty_find, mongo_find):
    docs = [
        {"a": Binary(b"0")}
    ]
    spec = {"a": {"$type": 5}}  # binData

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_6(monty_find, mongo_find):
    # undefined (Deprecated)
    assert True


def test_qop_type_7(monty_find, mongo_find):
    docs = [
        {"a": ObjectId(b"000000000000")}
    ]
    spec = {"a": {"$type": 7}}  # objectId

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_8(monty_find, mongo_find):
    docs = [
        {"a": True},
        {"a": 1}
    ]
    spec = {"a": {"$type": 8}}  # bool

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_type_9(monty_find, mongo_find):
    docs = [
        {"a": datetime(1985, 11, 12)}
    ]
    spec = {"a": {"$type": 9}}  # date

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_10(monty_find, mongo_find):
    docs = [
        {"a": None}
    ]
    spec = {"a": {"$type": 10}}  # null

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_11(monty_find, mongo_find):
    docs = [
        {"a": Regex("^a")},
        {"a": re.compile("^a")}
    ]
    spec = {"a": {"$type": 11}}  # regex

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()


def test_qop_type_12(monty_find, mongo_find):
    # dbPointer (Deprecated)
    assert True


def test_qop_type_13(monty_find, mongo_find):
    docs = [
        {"a": Code("a")}
    ]
    spec = {"a": {"$type": 13}}  # javascript

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_14(monty_find, mongo_find):
    # symbol (Deprecated)
    assert True


def test_qop_type_15(monty_find, mongo_find):
    docs = [
        {"a": Code("a", {})}
    ]
    spec = {"a": {"$type": 15}}  # javascript with scope

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_16(monty_find, mongo_find):
    docs = [
        {"a": 1}
    ]
    spec = {"a": {"$type": 16}}  # int

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_17(monty_find, mongo_find):
    docs = [
        {"a": Timestamp(0, 1)}
    ]
    spec = {"a": {"$type": 17}}  # timestamp

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_18(monty_find, mongo_find):
    docs = [
        {"a": Int64(1)}
    ]
    spec = {"a": {"$type": 18}}  # long

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_19(monty_find, mongo_find):
    docs = [
        {"a": Decimal128("1.1")}
    ]
    spec = {"a": {"$type": 19}}  # decimal

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_20(monty_find, mongo_find):
    docs = [
        {"a": MinKey()}
    ]
    spec = {"a": {"$type": -1}}  # minKey

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_21(monty_find, mongo_find):
    docs = [
        {"a": MaxKey()}
    ]
    spec = {"a": {"$type": 127}}  # maxKey

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_type_22(monty_find, mongo_find):
    docs = [
        {"a": 1}
    ]
    spec = {"a": {"$type": "long"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_type_23(monty_find, mongo_find):
    docs = [
        {"a": 1.1}
    ]
    spec = {"a": {"$type": "decimal"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_type_24(monty_find, mongo_find):
    docs = [
        {"a": "not_code"}
    ]
    spec = {"a": {"$type": "javascript"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 0
    assert monty_c.count() == mongo_c.count()


def test_qop_type_25(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": Int64(2)},
        {"a": Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "int"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_type_26(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": Int64(2)},
        {"a": Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "double"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_type_27(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": Int64(2)},
        {"a": Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "long"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_type_28(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": Int64(2)},
        {"a": Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "decimal"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_type_29(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 0},
        {"a": True},
        {"a": False},
    ]
    spec = {"a": {"$type": "bool"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(monty_c) == next(mongo_c)


def test_qop_type_30(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": Int64(2)},
        {"a": Decimal128("2.2")}
    ]
    spec = {"a": {"$type": ["decimal", 16]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 2
    assert monty_c.count() == mongo_c.count()
    for i in range(2):
        assert next(monty_c) == next(mongo_c)


def test_qop_type_31(monty_find, mongo_find):
    docs = [
        {"a": [1]},
    ]
    spec = {"a": {"$type": ["int"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_type_32(monty_find, mongo_find):
    docs = [
        {"a": [1]},
    ]
    spec = {"a": {"$type": ["array"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)
