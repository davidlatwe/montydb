import os
import contextlib
import importlib
import inspect

from bson.py3compat import string_type

from .storage.abcs import AbstractStorage
from .errors import ConfigurationError


MEMORY_STORAGE = "memory"
SQLITE_STORAGE = "sqlite"
FALTFILE_STORAGE = "flatfile"

DEFAULT_STORAGE = FALTFILE_STORAGE

MEMORY_REPOSITORY = ":memory:"

URI_SCHEME_PREFIX = "montydb://"


_pinned_repository = {"_": None}


def remove_uri_scheme_prefix(uri_or_dir):
    """Internal function to remove URI scheme prefix from repository path

    Args:
        uri_or_dir (str): Folder path or montydb URI

    Returns:
        str: A repository path without URI scheme prefix

    """
    if uri_or_dir.startswith(URI_SCHEME_PREFIX):
        dirname = uri_or_dir[len(URI_SCHEME_PREFIX):]
    else:
        dirname = uri_or_dir

    return dirname


def provide_repository(dirname=None):
    """Internal function to acquire repository path

    This will pick one repository path in the order of:
    `dirname` -> current pinned repository -> current working dir

    Args:
        dirname (str): Folder path, default None

    Returns:
        str: A repository path acquired from current environment

    """
    if dirname is None or dirname == "":
        return current_repo() or os.getcwd()
    elif isinstance(dirname, string_type):
        return remove_uri_scheme_prefix(dirname)
    else:
        raise TypeError("Repository path should be a string.")


def pin_repo(repository):
    """Pin a db repository for all operations afterward

    Example:
        >>> from montydb import pin_repo, set_storage, MontyClient
        >>> pin_repo("/foo/bar")
        >>> # The following operations will use '/foo/bar'
        >>> set_storage(storage="sqlite")
        >>> client = MontyClient()

    Args:
        repository (str): Database repository path

    """
    _pinned_repository["_"] = provide_repository(repository)


def current_repo():
    """Returns current pinned repository

    Returns:
        str: Database repository path

    """
    return _pinned_repository["_"]


@contextlib.contextmanager
def open_repo(repository=None):
    """Open a repository context

    This will change current working dir to the `repository` or the current
    pinned one during the context. But if the `repository` is ":memory:",
    the current working dir will NOT be changed.

    Args:
        repository (str): Database repository path, default None

    """
    repository = provide_repository(repository)
    crepo = current_repo()
    if repository == MEMORY_REPOSITORY:
        try:
            # Context
            pin_repo(repository)
            yield

        finally:
            pin_repo(crepo)

    else:
        cwd = os.getcwd()
        try:
            # Context
            pin_repo(repository)
            os.chdir(repository)
            yield

        finally:
            pin_repo(crepo)
            os.chdir(cwd)


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

    Returns:
        A Subclass of `montydb.storage.abcs.AbstractStorage`

    """
    if repository == MEMORY_REPOSITORY:
        return find_storage_cls(MEMORY_STORAGE)

    setup = os.path.join(repository, _storage_ident_fname)

    if not os.path.isfile(setup):
        set_storage(repository)

    with open(setup, "r") as fp:
        storage_name = fp.readline().strip()

    return find_storage_cls(storage_name)
