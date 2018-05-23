
import pytest
import os

from montydb import MontyConfigure
from montydb.storage import (
    MemoryStorage,
    SQLiteConfig,
    SQLiteStorage,
    FlatFileConfig,
    FlatFileStorage
)


def test_configure_get_storage_engine(tmp_monty_repo):
    configure = MontyConfigure(":memory:")
    storage = configure._get_storage_engine()
    assert isinstance(storage, MemoryStorage)

    tmp_dir = os.path.join(tmp_monty_repo, "sqlite")
    configure = MontyConfigure(tmp_dir, SQLiteConfig)
    storage = configure._get_storage_engine()
    assert isinstance(storage, SQLiteStorage)

    tmp_dir = os.path.join(tmp_monty_repo, "flatfile")
    configure = MontyConfigure(tmp_dir, FlatFileConfig)
    storage = configure._get_storage_engine()
    assert isinstance(storage, FlatFileStorage)


def test_configure_memory_save():
    configure = MontyConfigure(":memory:")
    assert configure.save() is None


def test_configure_memory_exists():
    configure = MontyConfigure(":memory:")
    assert configure.exists() is None


def test_configure_memory_config_path():
    configure = MontyConfigure(":memory:")
    assert configure.config_path is None


def test_configure_config_err(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    config = configure.config

    with pytest.raises(RuntimeError):
        config.storage.custom_config = "HELLO"

    with pytest.raises(RuntimeError):
        config.storage["custom_config"] = "HELLO"

    with pytest.raises(RuntimeError):
        del config.pragmas["database"]


def test_configure_exists(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    assert configure.exists()
