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
