
import pytest
import yaml
import os
import tempfile
import shutil
import copy

try:
    from importlib import reload
except ImportError:
    pass

import montydb
from montydb import MontyConfigure
from montydb.errors import ConfigurationError
from montydb.storage.memory import MemoryStorage
from montydb.storage.sqlite import SQLiteStorage
from montydb.storage.flatfile import FlatFileStorage
from montydb.storage.abcs import StorageConfig
from montydb.storage import (
    SQLiteConfig,
    FlatFileConfig,
)


@pytest.fixture
def tmp_config_repo():
    tmp_dir = os.path.join(tempfile.gettempdir(), "monty_config")
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    return tmp_dir


def test_configure_get_storage_engine(tmp_config_repo):
    configure = MontyConfigure(":memory:")
    storage = configure.load()._get_storage_engine()
    assert isinstance(storage, MemoryStorage)

    tmp_dir = os.path.join(tmp_config_repo, "sqlite")
    configure = MontyConfigure(tmp_dir).load(SQLiteConfig)
    storage = configure._get_storage_engine()
    assert isinstance(storage, SQLiteStorage)

    tmp_dir = os.path.join(tmp_config_repo, "flatfile")
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


def test_configure_exists(tmp_config_repo):
    configure = MontyConfigure(tmp_config_repo)
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


def test_configure_config(tmp_config_repo):
    configure = MontyConfigure(tmp_config_repo).load(FakeConfig)
    config = configure.config

    with pytest.raises(ConfigurationError):
        config.storage.custom_config = "HELLO"

    with pytest.raises(ConfigurationError):
        config.storage["custom_config"] = "HELLO"

    with pytest.raises(ConfigurationError):
        config.storage["custom_config"]

    with pytest.raises(ConfigurationError):
        del config.storage["engine"]

    assert len(config) == 2


def test_configure_restriction(tmp_config_repo):
    configure = MontyConfigure(tmp_config_repo).load(FakeConfig)
    config = configure.config

    with pytest.raises(ConfigurationError):
        config.clear()

    with pytest.raises(ConfigurationError):
        config.pop("storage")

    with pytest.raises(ConfigurationError):
        config.popitem()

    with pytest.raises(ConfigurationError):
        config.setdefault("custom_config", [])

    with pytest.raises(ConfigurationError):
        config.update({"custom_config": True})


def test_configure_validation_fail(tmp_config_repo):
    with pytest.raises(ConfigurationError):
        with MontyConfigure(tmp_config_repo) as cf:
            cf.load(FakeConfig)
            cf.config.connection.fake_attr = None


def test_configure_load_without_config_schema(tmp_config_repo):
    EmptyConfig = copy.copy(FakeConfig)
    EmptyConfig.schema = None
    MontyConfigure(tmp_config_repo).load(EmptyConfig)


def test_configure_reload(tmp_config_repo):
    configure = MontyConfigure(tmp_config_repo)
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

    config.reload(repository=tmp_config_repo)

    assert config.connection.fake_attr


def test_configure_reload_faild(tmp_config_repo):
    configure = MontyConfigure(tmp_config_repo)
    config = configure.load().config
    # no save
    with pytest.raises(ConfigurationError):
        config.reload(repository=tmp_config_repo)


def test_configure_reload_in_memory():
    configure = MontyConfigure(":memory:")
    config = configure.load(FakeConfig).config
    # no save
    config.reload(repository=":memory:")


def test_configure_drop(tmp_config_repo):
    with MontyConfigure(tmp_config_repo) as cf:
        cf.load(FakeConfig)
        cf.save()
        cf.drop()
        assert cf.exists() is False


def test_configure_drop_after_del(tmp_config_repo):
    with MontyConfigure(tmp_config_repo) as cf:
        cf.load(FakeConfig)
        cf.save()
        os.remove(cf.config_path)
        cf.drop()
        assert cf.exists() is False


def test_configure_load_wrong_type(tmp_config_repo):
    with pytest.raises(TypeError):
        with MontyConfigure(tmp_config_repo) as cf:
            cf.load({})

    with pytest.raises(TypeError):
        with MontyConfigure(tmp_config_repo) as cf:
            cf.load(FakeConfig())


def test_configure_yaml_import_err(monkeypatch):
    if hasattr(yaml, "CLoader"):
        monkeypatch.delattr(yaml, "CLoader")
        reload(montydb.configure)
