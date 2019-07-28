
import pytest
from bson.errors import InvalidDocument


def test_insert_invalid_doc_1(monty_find, mongo_find):
    docs = [
        {".b": 1},
    ]
    spec = {}

    with pytest.raises(InvalidDocument):
        mongo_find(docs, spec)

    with pytest.raises(InvalidDocument):
        monty_find(docs, spec)


def test_insert_invalid_doc_2(monty_find, mongo_find):
    docs = [
        {"$b": 1},
    ]
    spec = {}

    with pytest.raises(InvalidDocument):
        mongo_find(docs, spec)

    with pytest.raises(InvalidDocument):
        monty_find(docs, spec)
