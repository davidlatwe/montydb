
import pytest
import os

import montydb


def _create_db(client, db_name):
    client[db_name].col.insert_one({"test": "fake_data"})


def test_client_address(monty_client, tmp_monty_repo):
    address = os.path.normpath(monty_client.address)
    tmp_repo = os.path.normpath(tmp_monty_repo)
    assert address in [tmp_repo, ":memory:"]


def test_client_getattr(monty_client):
    db = monty_client.test
    assert isinstance(db, montydb.MontyDatabase)
    assert db.name == "test"

    with pytest.raises(AttributeError):
        db = monty_client._test


def test_client_getitem(monty_client):
    db = monty_client["test"]
    assert isinstance(db, montydb.MontyDatabase)
    assert db.name == "test"


def test_client_database_names(monty_client, mongo_client):

    db_names = ["test1", "test2"]

    for db in db_names:
        _create_db(monty_client, db)
        _create_db(mongo_client, db)

    monty_db_names = monty_client.database_names()
    mongo_db_names = mongo_client.database_names()
    mongo_db_names.remove("admin")
    assert monty_db_names == mongo_db_names


def test_client_drop_database(monty_client):
    _create_db(monty_client, "drop_me")
    monty_client.drop_database("drop_me")
    assert "drop_me" not in monty_client.database_names()

    db_inst = monty_client.get_database("drop_db_inst")
    monty_client.drop_database(db_inst)
    assert "drop_db_inst" not in monty_client.database_names()

    with pytest.raises(TypeError):
        monty_client.drop_database(0)


def test_client_get_database(monty_client):
    db = monty_client.get_database("test")
    assert db.name == "test"

    with pytest.raises(montydb.errors.OperationFailure):
        monty_client.get_database("test.db")
