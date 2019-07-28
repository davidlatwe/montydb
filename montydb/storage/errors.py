
class StorageError(Exception):
    """Base class for all storage exceptions."""


class StorageDuplicateKeyError(StorageError):
    """Raise when an insert or update fails due to a duplicate key error."""
