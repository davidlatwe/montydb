
import pytest
import yaml

from montydb import MontyConfigure
from montydb.storage import MemoryStorage, SQLiteStorage


def test_configure_memory_get_storage():
    configure = MontyConfigure(":memory:")
    storage = configure._get_storage_engine()
    assert isinstance(storage, MemoryStorage)


def test_configure_memory_save():
    configure = MontyConfigure(":memory:")
    with pytest.raises(RuntimeError):
        configure.save()


def test_configure_memory_exists():
    configure = MontyConfigure(":memory:")
    with pytest.raises(RuntimeError):
        configure.exists()


def test_configure_memory_touched():
    configure = MontyConfigure(":memory:")
    with pytest.raises(RuntimeError):
        configure.touched()


def test_configure_sqlite_get_storage(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    storage = configure._get_storage_engine()
    assert isinstance(storage, SQLiteStorage)


def test_configure_sqlite_save_config(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    config = configure.config
    config.storage.pragmas.database.journal_mode = "DELETE"
    configure.save()

    cnf_ = None
    with open(configure._config_path, "r") as stream:
        cnf_ = yaml.load(stream)
    assert cnf_["storage"]["pragmas"]["database"]["journal_mode"] == "DELETE"


def test_configure_sqlite_config_err(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    config = configure.config

    with pytest.raises(IOError):
        config.storage.custom_config = "HELLO"

    with pytest.raises(IOError):
        config.storage["custom_config"] = "HELLO"

    with pytest.raises(IOError):
        del config.storage["pragmas"]


def test_configure_sqlite_exists(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    assert configure.exists()
