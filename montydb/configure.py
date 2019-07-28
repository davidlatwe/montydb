import os
import importlib
import inspect

from .storage.abcs import AbstractStorage
from .errors import ConfigurationError


MEMORY_STORAGE = "memory"
SQLITE_STORAGE = "sqlite"
FALTFILE_STORAGE = "flatfile"

DEFAULT_STORAGE = FALTFILE_STORAGE

MEMORY_REPOSITORY = ":memory:"


def provide_repository(dirname=None):
    return dirname or os.getcwd()


def find_storage_cls(storage_name):
    """Internal function to find storage engine class

    This function use `importlib.import_module` to find storage module by
    module name. And then it will try to find if there is a class that is
    a subclass of `montydb.storage.abcs.AbstractStorage`.

    Raise `montydb.errors.ConfigurationError` if not found.

    Args:
        storage_name (str): Storage module name

    Returns:
        cls: A subclass of `montydb.storage.abcs.AbstractStorage`

    """
    try:
        monty_storage = "montydb.storage." + storage_name
        module = importlib.import_module(monty_storage)
    except ImportError:
        try:
            module = importlib.import_module(storage_name)
        except ImportError:
            raise ConfigurationError("Storage module '%s' not found."
                                     "" % storage_name)

    for name, cls in inspect.getmembers(module, inspect.isclass):
        if (name != "AbstractStorage" and
                issubclass(cls, AbstractStorage)):

            return cls

    raise ConfigurationError("Storage engine class not found. Should "
                             "be a subclass of `montydb.storage.abcs."
                             "AbstractStorage`.")


_storage_ident_fname = ".monty.storage"


def set_storage(repository=None, storage=None, use_default=True, **kwargs):
    """Setup storage engine for the database repository

    Args:
        repository (str): A dir path for database to live on disk.
                          Default to current working dir.
        storage (str): Storage module name. Default "flatfile".
        use_default (bool): Use default storage config. Default `True`.

    keyword args:
        Other keyword args will be parsed as storage config options.

    """
    storage = storage or DEFAULT_STORAGE

    if storage == MEMORY_STORAGE:
        raise ConfigurationError("Memory storage does not require setup.")

    repository = provide_repository(repository)
    setup = os.path.join(repository, _storage_ident_fname)

    storage_cls = find_storage_cls(storage)

    if not os.path.isdir(repository):
        os.makedirs(repository)

    with open(setup, "w") as fp:
        fp.write(storage)

    if kwargs or use_default:
        storage_cls.save_config(repository, **kwargs)


def provide_storage(repository):
    """Internal function to get storage engine class from config

    Args:
        repository (str): A dir path for database to live on disk.

    """
    if repository == MEMORY_REPOSITORY:
        return find_storage_cls(MEMORY_STORAGE)

    setup = os.path.join(repository, _storage_ident_fname)

    if not os.path.isfile(setup):
        set_storage(repository)

    with open(setup, "r") as fp:
        storage_name = fp.readline().strip()

    return find_storage_cls(storage_name)
