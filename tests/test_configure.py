
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
