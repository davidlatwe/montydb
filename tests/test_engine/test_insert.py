
import pytest
from montydb import errors
from montydb.types import bson


@pytest.mark.parametrize("doc", [
    {".b": 1},
    {"$b": 1},
    {"a.b": 1},
    {"a": {"$b": 1}},
])
def test_insert_dot_or_dollar_key(monty_find, mongo_find, mongo_version, doc):
    spec = {}

    if mongo_version[:2] < [5, 0]:
        # MongoDB 5.0 began to accept these field names
        if bson.bson_used:
            with pytest.raises(errors.InvalidDocument):
                mongo_find([dict(doc)], spec)

        with pytest.raises(errors.InvalidDocument):
            monty_find([dict(doc)], spec)
        return

    docs = [dict(doc)]
    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert next(mongo_c) == next(monty_c)


def test_insert_bytes(monty_find, mongo_find):
    docs = [
        {"data": b"some bytes"},
    ]
    spec = {}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert next(monty_c) == next(mongo_c)
