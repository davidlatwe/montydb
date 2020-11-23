
import re
from montydb.types import bson_ as bson
from datetime import datetime

from ...conftest import skip_if_no_bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_type_1(monty_find, mongo_find):
    docs = [
        {"a": 0.1}
    ]
    spec = {"a": {"$type": 1}}  # double

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_2(monty_find, mongo_find):
    docs = [
        {"a": "string"}
    ]
    spec = {"a": {"$type": 2}}  # string

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_3(monty_find, mongo_find):
    docs = [
        {"a": {"doc": "object"}}
    ]
    spec = {"a": {"$type": 3}}  # object

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_4(monty_find, mongo_find):
    docs = [
        {"a": []}
    ]
    spec = {"a": {"$type": 4}}  # array

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_5(monty_find, mongo_find):
    docs = [
        {"a": bson.Binary(b"0")}
    ]
    spec = {"a": {"$type": 5}}  # binData

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_6(monty_find, mongo_find):
    # undefined (Deprecated)
    assert True


@skip_if_no_bson
def test_qop_type_7(monty_find, mongo_find):
    docs = [
        {"a": bson.ObjectId(b"000000000000")}
    ]
    spec = {"a": {"$type": 7}}  # objectId

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_8(monty_find, mongo_find):
    docs = [
        {"a": True},
        {"a": 1}
    ]
    spec = {"a": {"$type": 8}}  # bool

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


def test_qop_type_9(monty_find, mongo_find):
    docs = [
        {"a": datetime(1985, 11, 12)}
    ]
    spec = {"a": {"$type": 9}}  # date

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_10(monty_find, mongo_find):
    docs = [
        {"a": None}
    ]
    spec = {"a": {"$type": 10}}  # null

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_11(monty_find, mongo_find):
    docs = [
        {"a": bson.Regex("^a")},
        {"a": re.compile("^a")}
    ]
    spec = {"a": {"$type": 11}}  # regex

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_12(monty_find, mongo_find):
    # dbPointer (Deprecated)
    assert True


@skip_if_no_bson
def test_qop_type_13(monty_find, mongo_find):
    docs = [
        {"a": bson.Code("a")}
    ]
    spec = {"a": {"$type": 13}}  # javascript

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_14(monty_find, mongo_find):
    # symbol (Deprecated)
    assert True


@skip_if_no_bson
def test_qop_type_15(monty_find, mongo_find):
    docs = [
        {"a": bson.Code("a", {})}
    ]
    spec = {"a": {"$type": 15}}  # javascript with scope

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_16(monty_find, mongo_find):
    docs = [
        {"a": 1}
    ]
    spec = {"a": {"$type": 16}}  # int

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_17(monty_find, mongo_find):
    docs = [
        {"a": bson.Timestamp(0, 1)}
    ]
    spec = {"a": {"$type": 17}}  # timestamp

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_18(monty_find, mongo_find):
    docs = [
        {"a": bson.Int64(1)}
    ]
    spec = {"a": {"$type": 18}}  # long

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_19(monty_find, mongo_find):
    docs = [
        {"a": bson.Decimal128("1.1")}
    ]
    spec = {"a": {"$type": 19}}  # decimal

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_20(monty_find, mongo_find):
    docs = [
        {"a": bson.MinKey()}
    ]
    spec = {"a": {"$type": -1}}  # minKey

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_21(monty_find, mongo_find):
    docs = [
        {"a": bson.MaxKey()}
    ]
    spec = {"a": {"$type": 127}}  # maxKey

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_22(monty_find, mongo_find):
    docs = [
        {"a": 1}
    ]
    spec = {"a": {"$type": "long"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_23(monty_find, mongo_find):
    docs = [
        {"a": 1.1}
    ]
    spec = {"a": {"$type": "decimal"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_type_24(monty_find, mongo_find):
    docs = [
        {"a": "not_code"}
    ]
    spec = {"a": {"$type": "javascript"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 0
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_type_25(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": bson.Int64(2)},
        {"a": bson.Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "int"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


@skip_if_no_bson
def test_qop_type_26(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": bson.Int64(2)},
        {"a": bson.Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "double"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


@skip_if_no_bson
def test_qop_type_27(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": bson.Int64(2)},
        {"a": bson.Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "long"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


@skip_if_no_bson
def test_qop_type_28(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": bson.Int64(2)},
        {"a": bson.Decimal128("2.2")}
    ]
    spec = {"a": {"$type": "decimal"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
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

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(monty_c) == next(mongo_c)


@skip_if_no_bson
def test_qop_type_30(monty_find, mongo_find):
    docs = [
        {"a": 1},
        {"a": 1.1},
        {"a": bson.Int64(2)},
        {"a": bson.Decimal128("2.2")}
    ]
    spec = {"a": {"$type": ["decimal", 16]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(monty_c) == next(mongo_c)


def test_qop_type_31(monty_find, mongo_find):
    docs = [
        {"a": [1]},
    ]
    spec = {"a": {"$type": ["int"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


def test_qop_type_32(monty_find, mongo_find):
    docs = [
        {"a": [1]},
    ]
    spec = {"a": {"$type": ["array"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)
