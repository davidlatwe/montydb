
import pytest
import os

from montydb import MontyConfigure
from montydb.errors import ConfigurationError
from montydb.storage.memory import MemoryStorage
from montydb.storage.sqlite import SQLiteStorage
from montydb.storage.flatfile import FlatFileStorage
from montydb.storage.base import StorageConfig
from montydb.storage import (
    SQLiteConfig,
    FlatFileConfig,
)


def test_configure_get_storage_engine(tmp_monty_repo):
    configure = MontyConfigure(":memory:")
    storage = configure.load()._get_storage_engine()
    assert isinstance(storage, MemoryStorage)

    tmp_dir = os.path.join(tmp_monty_repo, "sqlite")
    configure = MontyConfigure(tmp_dir).load(SQLiteConfig)
    storage = configure._get_storage_engine()
    assert isinstance(storage, SQLiteStorage)

    tmp_dir = os.path.join(tmp_monty_repo, "flatfile")
    configure = MontyConfigure(tmp_dir).load(FlatFileConfig)
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
    configure = MontyConfigure(tmp_monty_repo).load()
    config = configure.config

    with pytest.raises(ConfigurationError):
        config.storage.custom_config = "HELLO"

    with pytest.raises(ConfigurationError):
        config.storage["custom_config"] = "HELLO"

    with pytest.raises(ConfigurationError):
        del config.storage["engine"]


def test_configure_exists(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    configure.load().save()
    assert configure.exists()


class FakeConfig(StorageConfig):
    config = """
    storage:
      engine: FakeEngine
      config: FakeConfig
      module: {}
    connection:
      fake_attr: false
    """.format(__name__)
    schema = """
    type: object
    properties:
      connection:
        type: object
        properties:
          fake_attr:
            type: boolean
    """


def test_configure_validation_fail(tmp_monty_repo):
    with pytest.raises(ConfigurationError):
        with MontyConfigure(tmp_monty_repo) as cf:
            cf.load(FakeConfig)
            cf.config.connection.fake_attr = None


def test_configure_reload(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    config = configure.load(FakeConfig).config
    configure.save()

    FAKE_CONFIG_TRUE = """
    storage:
      engine: FakeEngine
      config: FakeConfig
      module: {}
    connection:
      fake_attr: true
    """.format(__name__)

    with open(configure.config_path, "w") as fp:
        fp.write(FAKE_CONFIG_TRUE)

    config.reload(repository=tmp_monty_repo)

    assert config.connection.fake_attr


def test_configure_reload_faild(tmp_monty_repo):
    configure = MontyConfigure(tmp_monty_repo)
    config = configure.load(FakeConfig).config
    # no save
    with pytest.raises(ConfigurationError):
        config.reload(repository=tmp_monty_repo)
