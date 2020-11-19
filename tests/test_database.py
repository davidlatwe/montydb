
import pytest

from montydb import MontyCollection
from montydb.errors import OperationFailure, CollectionInvalid


def test_database_name(monty_database):
    assert monty_database.name == "test_db"


def test_database_client(monty_database, monty_client):
    assert monty_database.client == monty_client


def test_database_eq(monty_database, monty_client):
    other_database = monty_client.get_database(monty_database.name)
    assert monty_database == other_database


def test_database_eq_other_type(monty_database):
    assert not monty_database == "other_type"


def test_database_ne(monty_database, monty_client):
    other_database = monty_client.get_database("other_db")
    assert monty_database != other_database


def test_database_getitem(monty_database):
    col = monty_database["test_col_get_item"]
    assert isinstance(col, MontyCollection)
    assert col.name == "test_col_get_item"


def test_database_getattr_faild(monty_database):
    with pytest.raises(AttributeError):
        monty_database._bad_name


def test_database_get_collection(monty_database):
    col = monty_database.get_collection("test_col_get")
    assert isinstance(col, MontyCollection)
    assert col.name == "test_col_get"

    with pytest.raises(OperationFailure):
        monty_database.get_collection("$test")

    with pytest.raises(OperationFailure):
        monty_database.get_collection("system.")


def test_database_list_collection_names(monty_database, mongo_database):
    for col in monty_database.list_collection_names():
        monty_database.drop_collection(col)

    for col in mongo_database.list_collection_names():
        mongo_database.drop_collection(col)

    col_names = ["test1", "test2"]

    for col in col_names:
        monty_database.create_collection(col)
        mongo_database.create_collection(col)

    monty_col_names = monty_database.list_collection_names()
    mongo_col_names = mongo_database.list_collection_names()
    # relax collection name order match
    assert len(mongo_col_names) == len(monty_col_names)
    for col in monty_col_names:
        assert col in mongo_col_names


def test_database_create_colllection(monty_database):
    col = monty_database.create_collection("create_collection_test")
    assert isinstance(col, MontyCollection)
    assert col.name == "create_collection_test"


def test_database_drop_colllection(monty_database):
    monty_database.create_collection("drop_me")
    assert "drop_me" in monty_database.list_collection_names()
    monty_database.drop_collection("drop_me")
    assert "drop_me" not in monty_database.list_collection_names()

    db_inst = monty_database.create_collection("drop_col_inst")
    assert "drop_col_inst" in monty_database.list_collection_names()
    monty_database.drop_collection(db_inst)
    assert "drop_col_inst" not in monty_database.list_collection_names()

    with pytest.raises(TypeError):
        monty_database.drop_collection(0)


def test_database_create_existed_colllection(monty_database):
    monty_database.create_collection("collection_double")
    with pytest.raises(CollectionInvalid):
        monty_database.create_collection("collection_double")
