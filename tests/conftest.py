
import os
import time
import pytest
import shutil
import tempfile

import pymongo
import montydb


__cache = {
    "mongo_ver": {},
}


def pytest_addoption(parser):

    parser.addoption("--storage",
                     action="append",
                     help="""
                     MontyDB storage engine to test with.
                     - Valid storage engine name input:
                        * memory (default)
                        * sqlite
                        * flatfile
                        * lightning (lmdb)
                     - This flag can be presented multiple times for testing
                     results with different MontyDB storages.
                     - Testing all storages if not set.
                     """)

    parser.addoption("--mongodb",
                     action="append",
                     help="""
                     MongoDB instance url to test with.
                     - This flag can be presented multiple times for testing
                     results with different MongoDB versions.
                     - Use `localhost:27017` if not given.
                     """)

    parser.addoption("--use-bson",
                     action="store_true",
                     help="""
                     Enable bson in tests.
                     """)


# (NOTE) `bson` should be accessible in test, even use_bson is False

@pytest.fixture(scope="session")
def skip_if_no_bson_(use_bson):
    if not use_bson:
        pytest.skip("BSON module is disabled.")


def skip_if_no_bson(func):
    return pytest.mark.usefixtures("skip_if_no_bson_")(func)


def _gettempdir():
    return tempfile.mkdtemp(prefix=f"montydb.{time.time()}.")


@pytest.fixture
def gettempdir():
    return _gettempdir()


@pytest.fixture(scope="session")
def use_bson(request):
    return request.param


def mongodb_urls(request):
    urls = request.config.getoption("--mongodb")
    if not urls:
        urls = ["mongodb://localhost:27017"]
    return urls


def mongodb_id(url):
    if url in __cache["mongo_ver"]:
        version_info = __cache["mongo_ver"][url]
    else:
        client = pymongo.MongoClient(url)
        version_info = client.server_info()["versionArray"]
        __cache["mongo_ver"][url] = version_info

    return "mongodb-%d.%d" % tuple(version_info[:2])


def montydb_storages(request):
    storages = request.config.getoption("--storage")
    if not storages:
        storages = ["memory", "flatfile", "sqlite", "lightning"]
    return storages


def opt_bson(request):
    return [
        bool(request.config.getoption("--use-bson"))
    ]


def pytest_generate_tests(metafunc):
    if "mongo_client" in metafunc.fixturenames:
        metafunc.parametrize("mongo_client",
                             argvalues=mongodb_urls(metafunc),
                             ids=mongodb_id,
                             indirect=True)

    if "monty_client" in metafunc.fixturenames:
        metafunc.parametrize("monty_client",
                             argvalues=montydb_storages(metafunc),
                             indirect=True)

    if "use_bson" in metafunc.fixturenames:
        # not testing both case, just for the test id.
        metafunc.parametrize("use_bson",
                             argvalues=opt_bson(metafunc),
                             ids=lambda v: "bson-1" if v else "bson-0",
                             indirect=True)


@pytest.fixture(scope="session")
def tmp_monty_repo():
    tmp_dir = os.path.join(_gettempdir(), "monty")
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    return tmp_dir


@pytest.fixture
def tmp_monty_utils_repo(tmp_monty_repo):
    tmp_dir = os.path.join(tmp_monty_repo, "monty.utils")
    os.makedirs(tmp_dir)
    yield tmp_dir
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)


@pytest.fixture(scope="session")
def mongo_client(request):
    client = pymongo.MongoClient(request.param)
    existed_dbs = client.list_database_names() + ["admin", "config"]
    yield client
    # db cleanup
    for db in client.list_database_names():
        if db in existed_dbs:
            continue
        client.drop_database(db)


@pytest.fixture(scope="session")
def mongo_version(mongo_client):
    return mongo_client.server_info()["versionArray"]


@pytest.fixture(scope="session")
def monty_client(request, tmp_monty_repo, mongo_version, use_bson):
    if os.path.isdir(tmp_monty_repo):
        shutil.rmtree(tmp_monty_repo)

    mongo_ver = "%d.%d" % (mongo_version[0], mongo_version[1])

    if request.param == "memory":
        tmp_monty_repo = ":memory:"

    montydb.set_storage(tmp_monty_repo,
                        request.param,
                        mongo_version=mongo_ver,
                        use_bson=use_bson)

    client = montydb.MontyClient(tmp_monty_repo)
    # purge_all_db
    for db in client.list_database_names():
        client.drop_database(db)
    return client


@pytest.fixture(scope="session")
def monty_database(monty_client):
    return monty_client["test_db"]


@pytest.fixture(scope="session")
def mongo_database(mongo_client):
    return mongo_client["test_db"]
