
import pytest
from montydb import errors
from montydb.types import bson_ as bson


def test_insert_invalid_doc_1(monty_find, mongo_find):
    docs = [
        {".b": 1},
    ]
    spec = {}

    if bson.bson_used:
        with pytest.raises(errors.InvalidDocument):
            mongo_find(docs, spec)

    with pytest.raises(errors.InvalidDocument):
        monty_find(docs, spec)


def test_insert_invalid_doc_2(monty_find, mongo_find):
    docs = [
        {"$b": 1},
    ]
    spec = {}

    if bson.bson_used:
        with pytest.raises(errors.InvalidDocument):
            mongo_find(docs, spec)

    with pytest.raises(errors.InvalidDocument):
        monty_find(docs, spec)
