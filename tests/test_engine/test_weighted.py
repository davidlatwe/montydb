
import pytest
import re

from montydb.engine.base import Weighted, _cmp_decimal

from datetime import datetime

from bson.py3compat import PY3
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from bson.regex import Regex
from bson.code import Code
from bson.min_key import MinKey
from bson.max_key import MaxKey


def test_weighted_1():
    data = None
    assert Weighted(data) == (1, data)


def test_weighted_2():
    data = 6
    assert Weighted(data) == (2, data)


def test_weighted_3():
    data = 1.1
    assert Weighted(data) == (2, data)


def test_weighted_4():
    data = Int64(8)
    assert Weighted(data) == (2, data)


def test_weighted_5():
    data = Decimal128("5.5")
    assert Weighted(data) == (2, _cmp_decimal(data))


def test_weighted_6():
    data = "string"
    assert Weighted(data) == (3, data)


def test_weighted_7():
    data = Binary(b"001")
    assert Weighted(data) == (6, data)


def test_weighted_8():
    data = b"001"
    if PY3:
        assert Weighted(data) == (6, data)
    else:
        assert Weighted(data) == (3, data)


def test_weighted_9():
    data = {"a": None}
    assert Weighted(data) == (4, ((1, "a", None),))


def test_weighted_10():
    data = {"a": "doc", "b": 5}
    assert Weighted(data) == (4, ((3, "a", "doc"), (2, "b", 5)))


def test_weighted_11():
    data = {"a": [1, 2, 3]}
    assert Weighted(data) == (4, ((5, "a", ((2, 1), (2, 2), (2, 3))),))


def test_weighted_12():
    data = [8, 9]
    assert Weighted(data) == (5, ((2, 8), (2, 9)))


def test_weighted_13():
    data = (8, 9)
    assert Weighted(data) == (5, ((2, 8), (2, 9)))


def test_weighted_14():
    data = ObjectId(b"000000000001")
    assert Weighted(data) == (7, data)


def test_weighted_15():
    data = True
    assert Weighted(data) == (8, data)


def test_weighted_16():
    data = datetime(2018, 5, 13)
    assert Weighted(data) == (9, data)


def test_weighted_17():
    data = Timestamp(0, 1)
    assert Weighted(data) == (10, data)


def test_weighted_18():
    data = Regex("^a")
    assert Weighted(data) == (11, "^a", "")


def test_weighted_19():
    data = Regex("^a", "ix")
    assert Weighted(data) == (11, "^a", "ix")


def test_weighted_20():
    data = re.compile("^a", re.X | re.M)
    flag = "mux" if PY3 else "mx"
    assert Weighted(data) == (11, "^a", flag)


def test_weighted_21():
    data = Code("a")
    assert Weighted(data) == (12, "a", None)


def test_weighted_22():
    data = Code("a", {})
    assert Weighted(data) == (13, "a", ())


def test_weighted_23():
    data = MinKey()
    assert Weighted(data) == (-1, data)


def test_weighted_24():
    data = MaxKey()
    assert Weighted(data) == (127, data)


def test_weighted_25():
    class UnKnownObj:
        pass
    data = UnKnownObj()
    with pytest.raises(TypeError):
        Weighted(data)
