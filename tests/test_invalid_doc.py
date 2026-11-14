import pytest
from montydb import errors


@pytest.fixture
def monty_collection(monty_database):
    monty_database.drop_collection("for_invalid_doc_test")
    col = monty_database["for_invalid_doc_test"]
    return col


@pytest.mark.parametrize("doc", [
    {"x.y": "z"},
    {"$m": "n"},
])
def test_dot_or_dollar_key(monty_collection, mongo_version, doc):
    if mongo_version[:2] < [5, 0]:
        with pytest.raises(errors.InvalidDocument):
            monty_collection.insert_one(doc)
        return

    # MongoDB 5.0 began to accept these field names
    monty_collection.insert_one(doc)
    found = monty_collection.find_one({}, {"_id": 0})
    assert found == {k: v for k, v in doc.items() if k != "_id"}
