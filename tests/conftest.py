import pytest
import tempfile
import os
import shutil

import pymongo
import montydb


def pytest_addoption(parser):
    parser.addoption("--storage",
                     action="store",
                     default="memory",
                     help="""
                     Select storage engine:
                        * memory (default)
                        * sqlite
                        * flatfile
                     """)


@pytest.fixture
def storage(request):
    return request.config.getoption("--storage")


@pytest.fixture(scope="session")
def tmp_monty_repo():
    tmp_dir = os.path.join(tempfile.gettempdir(), "monty")
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    return tmp_dir


def purge_all_db(client):
    for db in client.database_names():
        if db == "admin" and isinstance(client, pymongo.MongoClient):
            continue
        client.drop_database(db)


@pytest.fixture
def monty_client(storage, tmp_monty_repo):
    if os.path.isdir(tmp_monty_repo):
        shutil.rmtree(tmp_monty_repo)

    if storage == "memory":
        return montydb.MontyClient(":memory:")
    elif storage == "sqlite":
        config = montydb.storage.SQLITE_CONFIG
        montydb.MontyConfigure(tmp_monty_repo, config)
        client = montydb.MontyClient(tmp_monty_repo)
        purge_all_db(client)
        return client
    elif storage == "flatfile":
        config = montydb.storage.FALTFILE_CONFIG
        montydb.MontyConfigure(tmp_monty_repo, config)
        client = montydb.MontyClient(tmp_monty_repo)
        purge_all_db(client)
        return client
    else:
        pytest.fail("Unknown storage engine: {!r}".format(storage), False)


@pytest.fixture
def mongo_client():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    purge_all_db(client)
    return client


@pytest.fixture
def monty_database(monty_client):
    return monty_client["test_db"]


@pytest.fixture
def mongo_database(mongo_client):
    return mongo_client["test_db"]
