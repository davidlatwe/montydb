
import pytest
from montydb import errors


@pytest.fixture
def monty_collection(monty_database):
    monty_database.drop_collection("for_invalid_doc_test")
    col = monty_database["for_invalid_doc_test"]
    return col


def test_invalid_doc_1(monty_collection):
    with pytest.raises(errors.InvalidDocument):
        monty_collection.insert_one({"x.y": "z"})


def test_invalid_doc_2(monty_collection):
    with pytest.raises(errors.InvalidDocument):
        monty_collection.insert_one({"$m": "n"})
