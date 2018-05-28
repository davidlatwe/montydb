
import pytest

from montydb.cursor import MontyCursor
from montydb.results import (
    InsertOneResult,
    InsertManyResult,
)


@pytest.fixture
def monty_collection(monty_database):
    monty_database.drop_collection("test_col")
    return monty_database.test_col


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
    assert monty_collection.name in db.collection_names()


def test_collection_find_in_non_existed_collection(monty_collection):
    cursor = monty_collection.find({})
    assert isinstance(cursor, MontyCursor)

    db = monty_collection.database
    assert monty_collection.name not in db.collection_names()


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
