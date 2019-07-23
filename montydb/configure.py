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


def _provide_repository(dirname=None):
    return dirname or os.getcwd()


def find_storage_cls(storage_name):
    try:
        monty_storage = "montydb.storage." + storage_name
        module = importlib.import_module(monty_storage)
    except ModuleNotFoundError:
        try:
            module = importlib.import_module(storage_name)
        except ModuleNotFoundError:
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


def set_storage(repository=None, storage=DEFAULT_STORAGE):
    """
    """
    if repository == MEMORY_REPOSITORY:
        raise ConfigurationError("Memory storage does not require setup.")

    repository = _provide_repository(repository)
    setup = os.path.join(repository, _storage_ident_fname)

    find_storage_cls(storage)

    if not os.path.isdir(repository):
        os.makedirs(repository)

    with open(setup, "w") as fp:
        fp.write(storage)


def provide_storage_for_repository(repository=None):
    """Get storage engine class from config
    """
    if repository == MEMORY_REPOSITORY:
        return find_storage_cls(MEMORY_STORAGE)

    repository = _provide_repository(repository)
    setup = os.path.join(repository, _storage_ident_fname)

    if not os.path.isfile(setup):
        set_storage(repository)

    with open(setup, "r") as fp:
        storage_name = fp.readline().strip()

    return find_storage_cls(storage_name)
