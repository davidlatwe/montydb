
import pytest
from montydb.errors import InvalidDocument
from montydb.types import ENABLE_BSON


def test_insert_invalid_doc_1(monty_find, mongo_find):
    docs = [
        {".b": 1},
    ]
    spec = {}

    if ENABLE_BSON:
        with pytest.raises(InvalidDocument):
            mongo_find(docs, spec)

    with pytest.raises(InvalidDocument):
        monty_find(docs, spec)


def test_insert_invalid_doc_2(monty_find, mongo_find):
    docs = [
        {"$b": 1},
    ]
    spec = {}

    if ENABLE_BSON:
        with pytest.raises(InvalidDocument):
            mongo_find(docs, spec)

    with pytest.raises(InvalidDocument):
        monty_find(docs, spec)
