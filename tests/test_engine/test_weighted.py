
import pytest
import re

from montydb.engine.weighted import Weighted, _cmp_decimal
from montydb.types import PY3, init_bson, bson_ as bson
from datetime import datetime

from ..conftest import skip_if_no_bson


@pytest.fixture(scope="module", autouse=True)
def set_bson(use_bson):
    init_bson(use_bson)


def test_weighted_1():
    data = None
    assert Weighted(data) == (1, data)


def test_weighted_2():
    data = 6
    assert Weighted(data) == (2, data)


def test_weighted_3():
    data = 1.1
    assert Weighted(data) == (2, data)


@skip_if_no_bson
def test_weighted_4():
    data = bson.Int64(8)
    assert Weighted(data) == (2, data)


@skip_if_no_bson
def test_weighted_5():
    data = bson.Decimal128("5.5")
    assert Weighted(data) == (2, _cmp_decimal(data))


def test_weighted_6():
    data = "string"
    assert Weighted(data) == (3, data)


@skip_if_no_bson
def test_weighted_7():
    data = bson.Binary(b"001")
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
    assert Weighted(data)[0] == 4
    assert (3, "a", "doc") in Weighted(data)[1]
    assert (2, "b", 5) in Weighted(data)[1]


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
    data = bson.ObjectId(b"000000000001")
    assert Weighted(data) == (7, data)


def test_weighted_15():
    data = True
    assert Weighted(data) == (8, data)


def test_weighted_16():
    data = datetime(2018, 5, 13)
    assert Weighted(data) == (9, data)


@skip_if_no_bson
def test_weighted_17():
    data = bson.Timestamp(0, 1)
    assert Weighted(data) == (10, data)


@skip_if_no_bson
def test_weighted_18():
    data = bson.Regex("^a")
    assert Weighted(data) == (11, "^a", "")


@skip_if_no_bson
def test_weighted_19():
    data = bson.Regex("^a", "ix")
    assert Weighted(data) == (11, "^a", "ix")


def test_weighted_20():
    data = re.compile("^a", re.X | re.M)
    flag = "mux" if PY3 else "mx"
    assert Weighted(data) == (11, "^a", flag)


@skip_if_no_bson
def test_weighted_21():
    data = bson.Code("a")
    assert Weighted(data) == (12, "a", None)


@skip_if_no_bson
def test_weighted_22():
    data = bson.Code("a", {})
    assert Weighted(data) == (13, "a", ())


@skip_if_no_bson
def test_weighted_23():
    data = bson.MinKey()
    assert Weighted(data) == (-1, data)


@skip_if_no_bson
def test_weighted_24():
    data = bson.MaxKey()
    assert Weighted(data) == (127, data)


def test_weighted_25():
    class UnKnownObj:
        pass
    data = UnKnownObj()
    with pytest.raises(TypeError):
        Weighted(data)


@skip_if_no_bson
def test_weighted_26():
    data = _cmp_decimal(bson.Decimal128("0.1"))
    assert Weighted(data) == (2, data)


@skip_if_no_bson
def test_weighted__cmp_decimal_err():
    with pytest.raises(TypeError):
        _cmp_decimal("Not_A_Decimal128")


@skip_if_no_bson
def test_weighted__cmp_decimal_ne():
    assert _cmp_decimal(bson.Decimal128("5.5")) != "NAN"


@skip_if_no_bson
def test_weighted__cmp_decimal_lt_or_gt():
    if PY3:
        with pytest.raises(TypeError):
            _cmp_decimal(bson.Decimal128("5.5")) > "NAN"
    else:
        assert (_cmp_decimal(bson.Decimal128("5.5")) > "NAN") is False


@skip_if_no_bson
def test_weighted__cmp_decimal_le_or_ge():
    if PY3:
        with pytest.raises(TypeError):
            _cmp_decimal(bson.Decimal128("5.5")) >= "NAN"
    else:
        assert (_cmp_decimal(bson.Decimal128("5.5")) >= "NAN") is False
