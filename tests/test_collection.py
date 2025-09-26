import pytest

from montydb.cursor import MontyCursor
from montydb.results import (
    InsertOneResult,
    InsertManyResult,
)
from pymongo.errors import (
    DuplicateKeyError as mongo_dup_key_err,
    BulkWriteError as mongo_bulkw_err,
)
from pymongo import ReturnDocument
from montydb.errors import (
    DuplicateKeyError as monty_dup_key_err,
    BulkWriteError as monty_bulkw_err,
)


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


@pytest.fixture
def monty_collection(monty_database):
    monty_database.drop_collection("test_col")
    return monty_database.test_col


@pytest.fixture
def mongo_collection(mongo_database):
    mongo_database.drop_collection("test_col")
    return mongo_database.test_col


def test_collection_name(monty_collection):
    assert monty_collection.name == "test_col"


def test_collection_eq(monty_collection, monty_database):
    assert monty_collection == monty_database["test_col"]


def test_collection_ne_1(monty_collection, monty_database):
    assert monty_collection.name != monty_database["other_col"]


def test_collection_ne_2(monty_collection, monty_client):
    db_other = monty_client.other_db
    assert monty_collection.name != db_other["test_col"]


def test_collection_full_name(monty_collection):
    assert monty_collection.full_name == "test_db.test_col"


def test_collection_database(monty_collection, monty_database):
    assert monty_collection.database == monty_database


def test_collection_insert_one(monty_collection):
    result = monty_collection.insert_one({"test": "doc"})
    assert isinstance(result, InsertOneResult)


def test_collection_insert_many(monty_collection):
    result = monty_collection.insert_many([{"a": 1}, {"a": 2}])
    assert isinstance(result, InsertManyResult)


def test_collection_find(monty_collection):
    monty_collection.insert_one({"test": "doc"})
    cursor = monty_collection.find({})
    assert isinstance(cursor, MontyCursor)

    db = monty_collection.database
    assert monty_collection.name in db.list_collection_names()


def test_collection_find_in_non_existed_collection(monty_collection):
    cursor = monty_collection.find({})
    assert isinstance(cursor, MontyCursor)

    db = monty_collection.database
    assert monty_collection.name not in db.list_collection_names()


def test_collection_find_one(monty_collection):
    monty_collection.insert_many([{"qty": 5}, {"qty": 10}])
    result = monty_collection.find_one()
    assert result["qty"] == 5


def test_collection_find_one_in_non_existed_collection(monty_collection):
    result = monty_collection.find_one()
    assert result is None


def test_collection_find_one_with_id(monty_collection):
    monty_collection.insert_one({"_id": 1, "qty": 7})
    result = monty_collection.find_one(1)
    assert result["qty"] == 7


def test_collection_get_collection(monty_collection):
    sub_col = monty_collection.sub_col
    assert sub_col.name == "test_col.sub_col"


def test_collection_get_collection_faild(monty_collection):
    with pytest.raises(AttributeError):
        monty_collection._sub_col


def test_collection_insert_one_has_duplicate_key(monty_collection, mongo_collection):
    doc = {"_id": 1, "foo": "bar"}

    mongo_collection.insert_one(doc)
    monty_collection.insert_one(doc)

    with pytest.raises(mongo_dup_key_err) as mongo_err:
        mongo_collection.insert_one(doc)

    with pytest.raises(monty_dup_key_err) as monty_err:
        monty_collection.insert_one(doc)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_collection_insert_many_has_duplicate_key(monty_collection, mongo_collection):
    docs = [{"_id": 1, "foo1": "bar"}, {"_id": 1, "foo2": "bar"}]

    with pytest.raises(mongo_bulkw_err) as mongo_err:
        mongo_collection.insert_many(docs)

    with pytest.raises(monty_bulkw_err) as monty_err:
        monty_collection.insert_many(docs)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
    assert mongo_err.value.details["nInserted"] == monty_err.value.details["nInserted"]

    monty_c = monty_collection.find()
    mongo_c = mongo_collection.find()
    assert count_documents(monty_c) == count_documents(mongo_c)


def test_collection_drop(monty_database):
    collection = monty_database.create_collection("drop_me")
    assert "drop_me" in monty_database.list_collection_names()
    collection.drop()
    assert "drop_me" not in monty_database.list_collection_names()


def test_collection_save(monty_collection, mongo_collection):
    doc = {"some": "doc"}
    new = {"add": "stuff"}
    result = {"some": "doc", "add": "stuff"}

    mongo_collection.insert_one(doc.copy())
    existed_doc = mongo_collection.find_one(doc)
    existed_doc.update(new)
    mongo_collection.replace_one(doc, existed_doc)

    assert mongo_collection.find_one(doc, {"_id": 0}) == result

    monty_collection.insert_one(doc.copy())
    existed_doc = monty_collection.find_one(doc)
    existed_doc.update(new)
    monty_collection.replace_one(doc, existed_doc)

    assert monty_collection.find_one(doc, {"_id": 0}) == result


def test_collection_count(monty_collection, mongo_collection):
    monty_collection.insert_many([{"a": 1}, {"a": 2}])
    mongo_collection.insert_many([{"a": 1}, {"a": 2}])

    monty_c = monty_collection.find()
    mongo_c = mongo_collection.find()
    assert count_documents(monty_c) == count_documents(mongo_c)


def test_collection_count_documents(monty_collection, mongo_collection):
    monty_collection.insert_many([{"a": 1}, {"a": 2}])
    mongo_collection.insert_many([{"a": 1}, {"a": 2}])

    monty_res = monty_collection.count_documents({"a": 1})
    mongo_res = mongo_collection.count_documents({"a": 1})

    assert monty_res == mongo_res


def test_find_one_and_update_basic(monty_collection, mongo_collection):
    doc = {"_id": 1, "a": 10}
    monty_collection.insert_one(doc.copy())
    mongo_collection.insert_one(doc.copy())

    monty_result = monty_collection.find_one_and_update(
        {"_id": 1},
        {"$set": {"a": 20}},
        return_document=False
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"_id": 1},
        {"$set": {"a": 20}},
        return_document=ReturnDocument.BEFORE
    )

    assert monty_result["a"] == 10
    assert monty_collection.find_one({"_id": 1})["a"] == 20

    assert monty_result == mongo_result


def test_find_one_and_update_return_after(monty_collection, mongo_collection):
    doc = {"_id": 1, "a": 10}
    monty_collection.insert_one(doc.copy())
    mongo_collection.insert_one(doc.copy())

    monty_result = monty_collection.find_one_and_update(
        {"_id": 1},
        {"$inc": {"a": 5}},
        return_document=True
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"_id": 1},
        {"$inc": {"a": 5}},
        return_document=ReturnDocument.AFTER
    )

    assert monty_result["a"] == 15
    assert monty_collection.find_one({"_id": 1})["a"] == 15

    assert monty_result == mongo_result


def test_find_one_and_update_with_sort(monty_collection, mongo_collection):
    docs = [
        {"_id": 1, "priority": 3, "a": 10},
        {"_id": 2, "priority": 1, "a": 10},
        {"_id": 3, "priority": 2, "a": 10}
    ]
    for doc in docs:
        monty_collection.insert_one(doc.copy())
        mongo_collection.insert_one(doc.copy())

    # Update document based on sorting priority
    monty_result = monty_collection.find_one_and_update(
        {"a": 10},
        {"$set": {"a": 20}},
        sort=[("priority", 1)],
        return_document=True
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"a": 10},
        {"$set": {"a": 20}},
        sort=[("priority", 1)],
        return_document=ReturnDocument.AFTER
    )

    assert monty_result["_id"] == 2
    assert monty_result["a"] == 20
    assert monty_collection.count_documents({"a": 20}) == 1

    assert monty_result == mongo_result


def test_find_one_and_update_with_projection(monty_collection, mongo_collection):
    doc = {"_id": 1, "a": "X", "b": 30, "c": "Y"}
    monty_collection.insert_one(doc.copy())
    mongo_collection.insert_one(doc.copy())

    monty_result = monty_collection.find_one_and_update(
        {"_id": 1},
        {"$set": {"b": 31}},
        projection={"a": 1, "b": 1, "_id": 0},
        return_document=True
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"_id": 1},
        {"$set": {"b": 31}},
        projection={"a": 1, "b": 1, "_id": 0},
        return_document=ReturnDocument.AFTER
    )

    assert monty_result == {"a": "X", "b": 31}
    assert "city" not in monty_result

    assert monty_result == mongo_result


def test_find_one_and_update_no_match(monty_collection, mongo_collection):
    doc = {"_id": 1, "a": 10}
    monty_collection.insert_one(doc.copy())
    mongo_collection.insert_one(doc.copy())

    monty_result = monty_collection.find_one_and_update(
        {"_id": 999},
        {"$set": {"a": 20}},
        return_document=False
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"_id": 999},
        {"$set": {"a": 20}},
        return_document=ReturnDocument.BEFORE
    )

    assert monty_result is None
    assert monty_collection.find_one({"_id": 1})["a"] == 10

    assert monty_result == mongo_result


def test_find_one_and_update_upsert_no_match(monty_collection, mongo_collection):
    monty_result = monty_collection.find_one_and_update(
        {"a": "X"},
        {"$set": {"b": 25}},
        upsert=True,
        return_document=True
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"a": "X"},
        {"$set": {"b": 25}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )

    assert monty_result["a"] == "X"
    assert monty_result["b"] == 25
    assert "_id" in monty_result

    assert monty_collection.count_documents({"a": "X"}) == 1
    
    # Compare with MongoDB result excluding _id
    monty_no_id = {k: v for k, v in monty_result.items() if k != "_id"}
    mongo_no_id = {k: v for k, v in mongo_result.items() if k != "_id"}
    assert monty_no_id == mongo_no_id


def test_find_one_and_update_upsert_return_before(monty_collection, mongo_collection):
    monty_result = monty_collection.find_one_and_update(
        {"a": "Y"},
        {"$set": {"b": 35}},
        upsert=True,
        return_document=False
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"a": "Y"},
        {"$set": {"b": 35}},
        upsert=True,
        return_document=ReturnDocument.BEFORE
    )

    assert monty_result is None
    assert monty_collection.find_one({"a": "Y"})["b"] == 35

    assert monty_result == mongo_result


def test_find_one_and_update_empty_collection(monty_collection, mongo_collection):
    monty_result = monty_collection.find_one_and_update(
        {},
        {"$set": {"a": 1}},
        return_document=False
    )

    mongo_result = mongo_collection.find_one_and_update(
        {},
        {"$set": {"a": 1}},
        return_document=ReturnDocument.BEFORE
    )

    assert monty_result is None
    assert monty_collection.count_documents({}) == 0

    assert monty_result == mongo_result


def test_find_one_and_update_projection_before_update(monty_collection, mongo_collection):
    doc = {
        "_id": 1,
        "a": "X",
        "b": 1,
        "c": "AA"
    }
    monty_collection.insert_one(doc.copy())
    mongo_collection.insert_one(doc.copy())

    monty_result = monty_collection.find_one_and_update(
        {"_id": 1},
        {"$set": {"b": 20, "c": "BB"}},
        projection={"a": 1, "b": 1, "_id": 0},
        return_document=False
    )

    mongo_result = mongo_collection.find_one_and_update(
        {"_id": 1},
        {"$set": {"b": 20, "c": "BB"}},
        projection={"a": 1, "b": 1, "_id": 0},
        return_document=ReturnDocument.BEFORE
    )

    assert monty_result == {"a": "X", "b": 1}
    # Verify update was applied
    doc = monty_collection.find_one({"_id": 1})
    assert doc["b"] == 20
    assert doc["c"] == "BB"

    assert monty_result == mongo_result
