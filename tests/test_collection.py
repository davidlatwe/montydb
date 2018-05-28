
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
    cursor = monty_collection.find({})
    assert isinstance(cursor, MontyCursor)


def test_collection_find_one(monty_collection):
    result = monty_collection.find_one()
    assert result is None

    monty_collection.insert_many([{"qty": 5}, {"qty": 10}])
    result = monty_collection.find_one()
    assert result["qty"] == 5
