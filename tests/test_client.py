
import pytest
import os
import sys
import platform
import subprocess

import montydb
from montydb.errors import OperationFailure
from montydb.configure import (
    MEMORY_REPOSITORY,
    URI_SCHEME_PREFIX,
    set_storage,
)


def _create_db(client, db_name):
    client[db_name].col.insert_one({"test": "fake_data"})


def test_client_address(monty_client, tmp_monty_repo):
    address = os.path.normpath(monty_client.address)
    tmp_repo = os.path.normpath(tmp_monty_repo)
    assert address in [tmp_repo, MEMORY_REPOSITORY]


def test_client_eq(monty_client):
    other_client = montydb.MontyClient(monty_client.address)
    assert monty_client == other_client


def test_client_eq_other_type(monty_client):
    assert not monty_client == "other_type"


def test_client_ne(monty_client, gettempdir):
    other_client = montydb.MontyClient(gettempdir + "/other_address")
    assert monty_client != other_client


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
    for db in monty_client.database_names():
        monty_client.drop_database(db)

    db_names = ["test1", "test2"]

    for db in db_names:
        _create_db(monty_client, db)
        _create_db(mongo_client, db)

    monty_db_names = monty_client.list_database_names()
    mongo_db_names = mongo_client.list_database_names()
    for mtdb in monty_db_names:
        assert mtdb in mongo_db_names


def test_client_drop_database(monty_client):
    _create_db(monty_client, "drop_me")
    monty_client.drop_database("drop_me")
    assert "drop_me" not in monty_client.list_database_names()

    db_inst = monty_client.get_database("drop_db_inst")
    monty_client.drop_database(db_inst)
    assert "drop_db_inst" not in monty_client.list_database_names()

    with pytest.raises(TypeError):
        monty_client.drop_database(0)


def test_client_get_database(monty_client):
    db = monty_client.get_database("test")
    assert db.name == "test"

    with pytest.raises(montydb.errors.OperationFailure):
        monty_client.get_database("test.db")


def _system_win():
    return "Windows"


def test_client_get_database_on_windows_faild(monty_client, monkeypatch):
    monkeypatch.setattr(platform, "system", _system_win)
    with pytest.raises(OperationFailure):
        monty_client.get_database("$invalid")


def test_client_get_database_non_windows_faild(monty_client, monkeypatch):
    with pytest.raises(OperationFailure):
        monty_client.get_database("$invalid")


def test_client_context(monty_client, gettempdir):
    with montydb.MontyClient(gettempdir + "/address"):
        pass


def test_client_wtimeout_type_error(monty_client):
    with pytest.raises(TypeError):
        montydb.MontyClient(monty_client.address, wtimeout=0.5)


def test_client_server_info(monty_client):
    server_info = monty_client.server_info()

    entries = [
        "version",
        "versionArray",
        "mongoVersion",
        "mongoVersionArray",
        "storageEngine",
        "python",
        "platform",
        "machine",
    ]

    for key in entries:
        server_info.pop(key)

    assert len(server_info) == 0


def test_client_with_montydb_uri(gettempdir):
    URI_SCHEME_PREFIX
    # Faltfile
    tmp_dir = os.path.join(gettempdir, "flatfile")
    uri = URI_SCHEME_PREFIX + tmp_dir

    set_storage(repository=uri, storage="flatfile")
    client = montydb.MontyClient(uri)

    assert client.address == tmp_dir

    # SQLite
    tmp_dir = os.path.join(gettempdir, "sqlite")
    uri = URI_SCHEME_PREFIX + tmp_dir

    set_storage(repository=uri, storage="sqlite")
    client = montydb.MontyClient(uri)

    assert client.address == tmp_dir

    # Memory
    uri = URI_SCHEME_PREFIX + MEMORY_REPOSITORY
    client = montydb.MontyClient(uri)

    assert client.address == MEMORY_REPOSITORY


def test_no_duplicated_docs_in_next_session(monty_client):
    db_name = "test_client"
    col_name = "no_duplicated_docs"

    doc = {"pet": "cat", "name": ["mai-mai", "jo-jo"]}
    find = {"pet": "cat"}
    update = {"$push": {"name": "na-na"}}

    col_1 = monty_client[db_name][col_name]
    col_1.insert_one(doc)
    assert col_1.count_documents(find) == 1

    new_client = montydb.MontyClient(monty_client.address)
    col_2 = new_client[db_name][col_name]
    col_2.update_one(find, update)
    assert col_2.count_documents(find) == 1


def test_client_init_on_existing_storage(gettempdir):
    cmd = [
        sys.executable,
        "-c",
        f"import montydb;montydb.MontyClient({gettempdir!r})",
    ]

    subprocess.check_call(cmd)

    p = subprocess.Popen(cmd, stderr=subprocess.PIPE)
    o, e = p.communicate()
    assert p.returncode == 0, e.decode()
