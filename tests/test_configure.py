
import pytest

from montydb import MontyConfigure
from montydb.storage import MemoryStorage


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


def test_configure_config_err(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    config = configure.config

    with pytest.raises(IOError):
        config.storage.custom_config = "HELLO"

    with pytest.raises(IOError):
        config.storage["custom_config"] = "HELLO"

    with pytest.raises(IOError):
        del config.pragmas["database"]


def test_configure_exists(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    assert configure.exists()
