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


@pytest.fixture(scope="session")
def storage(request):
    return request.config.getoption("--storage")


@pytest.fixture(scope="session")
def tmp_monty_repo():
    tmp_dir = os.path.join(tempfile.gettempdir(), "monty")
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    return tmp_dir


@pytest.fixture(scope="session")
def monty_client(storage, tmp_monty_repo):
    if os.path.isdir(tmp_monty_repo):
        shutil.rmtree(tmp_monty_repo)

    if storage == "memory":
        return montydb.MontyClient(":memory:")
    elif storage == "sqlite":
        montydb.set_storage(tmp_monty_repo, storage)
    elif storage == "flatfile":
        montydb.set_storage(tmp_monty_repo, storage)
    else:
        pytest.fail("Unknown storage engine: {!r}".format(storage), False)

    client = montydb.MontyClient(tmp_monty_repo)
    # purge_all_db
    for db in client.database_names():
        client.drop_database(db)
    return client


@pytest.fixture(scope="session")
def mongo_client():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    existed_dbs = client.database_names() + ["admin", "config"]
    yield client
    # db cleanup
    for db in client.database_names():
        if db in existed_dbs:
            continue
        client.drop_database(db)


@pytest.fixture(scope="session")
def monty_database(monty_client):
    return monty_client["test_db"]


@pytest.fixture(scope="session")
def mongo_database(mongo_client):
    return mongo_client["test_db"]
