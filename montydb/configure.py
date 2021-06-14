import os
import contextlib
import importlib
import inspect

from .storage import AbstractStorage, memory
from .errors import ConfigurationError
from .types import string_types


MEMORY_STORAGE = "memory"
SQLITE_STORAGE = "sqlite"
FALTFILE_STORAGE = "flatfile"

DEFAULT_STORAGE = FALTFILE_STORAGE

MEMORY_REPOSITORY = ":memory:"

URI_SCHEME_PREFIX = "montydb://"

MONGO_COMPAT_VERSIONS = ("3.6", "4.0", "4.2", "4.4")  # 4.4 is experimenting


_pinned_repository = {"_": None}
_session = {}
_session_default = {
    "mongo_version": "4.2",
    "use_bson": None,
}
# TODO:
#   The mongo version compating may fail if calling `set_storage()` multiple
#   times with different version.
#   To get this right, may need a factory to spawn CRUD logic object for
#   specific version and hook with client object.
#   Also mind the `MontyClient.server_info`, 'mongoVersion' entry should have
#   the right compat version.


def session_config():
    return _session.copy()


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
    elif isinstance(dirname, string_types):
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
            raise ConfigurationError("Storage module '%s' not found." % storage_name)

    for name, cls in inspect.getmembers(module, inspect.isclass):
        if name != "AbstractStorage" and issubclass(cls, AbstractStorage):

            return cls

    raise ConfigurationError(
        "Storage engine class not found. Should "
        "be a subclass of `montydb.storage.abcs."
        "AbstractStorage`."
    )


_storage_ident_fname = ".monty.storage"


def set_storage(
    repository=None, storage=None, mongo_version=None, use_bson=None, **kwargs
):
    """Setup storage engine for the database repository

    Args:
        repository (str, optional): A dir path for database to live on disk.
            Default to current working dir.
        storage (str, optional): Storage module name. Default "flatfile".
        mongo_version (str, optional): Which mongodb version's behavior should
            montydb try to match with. Default "4.2", other versions are "3.6",
            "4.0".
        use_bson (bool, optional): Use bson module. Default `None`.

    keyword args:
        Other keyword args will be parsed as storage config options.

    """
    from .types import bson

    storage = storage or DEFAULT_STORAGE

    if mongo_version and mongo_version not in MONGO_COMPAT_VERSIONS:
        raise ConfigurationError(
            "Unknown mongodb version: %s, currently supported versions are: %s"
            % (mongo_version, ", ".join(MONGO_COMPAT_VERSIONS))
        )

    use_bson = bson.bson_used if use_bson is None else use_bson
    mongo_version = mongo_version or _session.get("mongo_version")

    for key, value in {"use_bson": use_bson, "mongo_version": mongo_version}.items():
        if value is None:
            value = _session_default[key]
        _session[key] = value

    _bson_init(_session["use_bson"])
    _mongo_compat(_session["mongo_version"])

    kwargs.update(_session)

    storage_cls = find_storage_cls(storage)

    if storage == MEMORY_STORAGE:
        repository = MEMORY_REPOSITORY
    else:
        repository = provide_repository(repository)
        setup = os.path.join(repository, _storage_ident_fname)

        if not os.path.isdir(repository):
            os.makedirs(repository)

        with open(setup, "w") as fp:
            fp.write(storage)

    storage_cls.save_config(repository, **kwargs)


def provide_storage(repository):
    """Internal function to get storage engine class from config

    Args:
        repository (str): A dir path for database to live on disk.

    Returns:
        A Subclass of `montydb.storage.abcs.AbstractStorage`

    """
    if repository == MEMORY_REPOSITORY:
        storage_name = MEMORY_STORAGE
        if not memory.is_memory_storage_set():
            set_storage(repository, storage_name)

    else:
        setup = os.path.join(repository, _storage_ident_fname)
        if not os.path.isfile(setup):
            set_storage(repository)

        with open(setup, "r") as fp:
            storage_name = fp.readline().strip()

    return find_storage_cls(storage_name)


def _bson_init(use_bson):
    from .types import bson

    if bson.bson_used is None:
        bson.init(use_bson)

    elif bson.bson_used and not use_bson:
        raise ConfigurationError(
            "montydb has been config to use BSON and "
            "cannot be changed in current session."
        )

    elif not bson.bson_used and use_bson:
        raise ConfigurationError(
            "montydb has been config to opt-out BSON and "
            "cannot be changed in current session."
        )

    else:
        # bson.bson_used == use_bson
        pass


def _mongo_compat(version):
    from .engine import queries

    if version.startswith("3"):
        v3 = getattr(queries, "_is_comparable_ver3")
        setattr(queries, "_is_comparable", v3)

    if version.startswith("4"):
        v4 = getattr(queries, "_is_comparable_ver4")
        setattr(queries, "_is_comparable", v4)

    if version == "4.2":
        setattr(queries, "_regex_options_check", getattr(queries, "_regex_options_v42"))
    else:
        setattr(queries, "_regex_options_check", getattr(queries, "_regex_options_"))
