
import re
import pytest
from bson.regex import Regex
from bson.py3compat import PY3

from pymongo.errors import OperationFailure as MongoOpFail
from montydb.errors import OperationFailure as MontyOpFail


def test_qop_regex_1(monty_find, mongo_find):
    docs = [
        {"a": "apple"}
    ]
    spec = {"a": {"$regex": "^a"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_regex_2(monty_find, mongo_find):
    docs = [
        {"a": "apple"}
    ]
    spec = {"a": {"$regex": "^A", "$options": "i"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()


def test_qop_regex_3(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "abble"}
    ]
    spec = {"a": {"$regex": "^a", "$options": "i", "$nin": ["abble"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_regex_4(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "abble"}
    ]
    spec = {"a": {"$regex": Regex("^a"), "$nin": ["abble"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_regex_5(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "abble"}
    ]
    spec = {"a": {"$regex": re.compile("^a"), "$nin": ["abble"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
    assert next(monty_c) == next(mongo_c)


def test_qop_regex_6(monty_find, mongo_find):
    docs = [
        {"a": "Apple"}
    ]
    spec = {"a": {"$regex": Regex("^a", "i"), "$options": "x"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    count = 0 if PY3 else 1  # In PY3, `$options` will override regex flags
    assert mongo_c.count() == count
    assert monty_c.count() == mongo_c.count()


def test_qop_regex_7(monty_find, mongo_find):
    docs = [
        {"a": "abc123"}
    ]
    pattern = "abc #category code\n123 #item number"
    spec = {"a": {"$regex": pattern, "$options": "x"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    # if pound(#) char exists in $regex string value and not ends with
    # newline(\n), Mongo raise error.
    with pytest.raises(MongoOpFail):
        next(mongo_c)
    with pytest.raises(MontyOpFail):
        next(monty_c)


def test_qop_regex_8(monty_find, mongo_find):
    docs = [
        {"a": "abc123"}
    ]
    pattern = """
        abc #category code
        123 #item number
    """
    spec = {"a": {"$regex": pattern, "$options": "x"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert mongo_c.count() == 1
    assert monty_c.count() == mongo_c.count()
