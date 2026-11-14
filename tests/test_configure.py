
import pytest
import os
import tempfile
import shutil

from montydb.configure import (
    set_storage,
    find_storage_cls,
    provide_repository,
    provide_storage,
)
from montydb.errors import ConfigurationError
from montydb.storage.memory import MemoryStorage
from montydb.storage.sqlite import SQLiteStorage
from montydb.storage.flatfile import FlatFileStorage


@pytest.fixture
def tmp_config_repo():
    tmp_dir = os.path.join(tempfile.gettempdir(), "monty_config")
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    return tmp_dir


def test_configure_get_storage_engine(tmp_config_repo):
    # Faltfile
    tmp_dir = os.path.join(tmp_config_repo, "flatfile")
    set_storage(repository=tmp_dir, storage="flatfile")
    storage_cls = provide_storage(repository=tmp_dir)
    storage = storage_cls.launch(tmp_dir)
    assert isinstance(storage, FlatFileStorage)
    # SQLite
    tmp_dir = os.path.join(tmp_config_repo, "sqlite")
    set_storage(repository=tmp_dir, storage="sqlite")
    storage_cls = provide_storage(repository=tmp_dir)
    storage = storage_cls.launch(tmp_dir)
    assert isinstance(storage, SQLiteStorage)
    # Memory
    storage_cls = provide_storage(repository=":memory:")
    storage = storage_cls.launch(":memory:")
    assert isinstance(storage, MemoryStorage)


def test_storage_module_not_found():
    with pytest.raises(ConfigurationError):
        find_storage_cls("lost")


def test_provide_repository_type_err():
    with pytest.raises(TypeError):
        provide_repository(True)


def test_config_int_value(tmp_config_repo):
    expected = 5

    tmp_dir = os.path.join(tmp_config_repo, "flatfile")
    set_storage(repository=tmp_dir,
                storage="flatfile",
                cache_modified=expected)
    storage = provide_storage(repository=tmp_dir).launch(tmp_dir)
    assert storage._config["cache_modified"] == expected

    tmp_dir = os.path.join(tmp_config_repo, "flatfile_str")
    set_storage(repository=tmp_dir,
                storage="flatfile",
                cache_modified=str(expected))
    storage = provide_storage(repository=tmp_dir).launch(tmp_dir)
    assert storage._config["cache_modified"] == expected


def test_config_without_mongo_version(tmp_config_repo):
    # A configuration from before montydb 2.3.10 has no `mongo_version`, and
    # it must keep the MongoDB 4.2 behavior that it had.
    from montydb import configure

    tmp_dir = os.path.join(tmp_config_repo, "no_mongo_version")
    set_storage(repository=tmp_dir, storage="flatfile")
    config_file = os.path.join(tmp_dir, FlatFileStorage.config_fname)
    with open(config_file) as fp:
        lines = [line for line in fp if not line.startswith("mongo_version")]
    with open(config_file, "w") as fp:
        fp.writelines(lines)

    session = configure.session_config()
    try:
        provide_storage(repository=tmp_dir)
        assert configure.session_config()["mongo_version"] == "4.2"
    finally:
        # The version settings are global, so give the other tests theirs back
        configure._session.update(session)
        configure._mongo_compat(session["mongo_version"])
