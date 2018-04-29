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
                     """)


@pytest.fixture
def storage(request):
    return request.config.getoption("--storage")


@pytest.fixture
def tmp_monty_repo():
    return os.path.join(tempfile.gettempdir(), "monty")


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
    else:
        return None


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


def _ls_test_cases():
    def dp(root, f):
        return os.path.join(root, f)

    root = os.path.join(os.path.dirname(__file__), "test_cases")
    for f in os.listdir(root):
        dir_path = dp(root, f)
        if os.path.isdir(dir_path):
            yield dir_path


@pytest.fixture(scope="session")
def test_cases():
    case = {
        "documents": [],
        "filters": [],
        "projections": [],
        "sorts": [],
    }

    cases = {}

    for cdir in _ls_test_cases():
        for key in case:
            fpath = os.path.join(cdir, os.extsep.join([key, "json"]))
            if os.path.isfile(fpath):
                case[key] += montydb.utils.monty_load(fpath)

        case_name = os.path.basename(cdir)
        cases[case_name] = case

    return cases


@pytest.fixture
def monty_collection_stuffed(monty_database, test_cases):
    col = monty_database["test_col"]
    for case in test_cases.values():
        col.insert_many(case["documents"])
    return col


@pytest.fixture
def mongo_collection_stuffed(mongo_database, test_cases):
    col = mongo_database["test_col"]
    for case in test_cases.values():
        col.insert_many(case["documents"])
    return col
