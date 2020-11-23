
import re
import pytest
from montydb.types import bson_ as bson

from pymongo.errors import OperationFailure as MongoOpFail
from montydb.errors import OperationFailure as MontyOpFail

from ...conftest import skip_if_no_bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_qop_regex_1(monty_find, mongo_find):
    docs = [
        {"a": "apple"}
    ]
    spec = {"a": {"$regex": "^a"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_regex_2(monty_find, mongo_find):
    docs = [
        {"a": "apple"}
    ]
    spec = {"a": {"$regex": "^A", "$options": "i"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


def test_qop_regex_3(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "abble"}
    ]
    spec = {"a": {"$regex": "^a", "$options": "i", "$nin": ["abble"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


@skip_if_no_bson
def test_qop_regex_4(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "abble"}
    ]
    spec = {"a": {"$regex": bson.Regex("^a"), "$nin": ["abble"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


def test_qop_regex_5(monty_find, mongo_find):
    docs = [
        {"a": "apple"},
        {"a": "abble"}
    ]
    spec = {"a": {"$regex": re.compile("^a"), "$nin": ["abble"]}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(monty_c) == next(mongo_c)


@skip_if_no_bson
def test_qop_regex_6(monty_find, mongo_find, mongo_version):
    docs = [
        {"a": "Apple"}
    ]
    spec = {"a": {"$regex": bson.Regex("^a", "i"), "$options": "x"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    if mongo_version[:2] == [4, 2]:
        # options set in both $regex and $options
        with pytest.raises(MongoOpFail):
            next(mongo_c)
        with pytest.raises(MontyOpFail):
            next(monty_c)
        return

    # If not using `SON` or `OrderedDict`, then depend on the dict key order,
    # if the first key is `$regex`, `$options` will override `regex.flags`,
    # vice versa.
    count = 0 if next(iter(spec["a"])) == "$regex" else 1
    assert count_documents(mongo_c, spec) == count
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


# ignored, this is edge case
def _test_qop_regex_7(monty_find, mongo_find, mongo_version):
    docs = [
        {"a": "abc123"}
    ]
    pattern = "abc #category code\n123 #item number"
    spec = {"a": {"$regex": pattern, "$options": "x"}}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    if mongo_version[:2] == [4, 0]:
        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(monty_c) == next(mongo_c)
    else:
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

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)


@skip_if_no_bson
def test_qop_regex_9(monty_find, mongo_find):
    docs = [
        {"a": "apple"}
    ]
    spec = {"a": bson.Regex("^a")}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
