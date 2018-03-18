from bson.errors import *


class MontyError(Exception):
    """Base class for all Montydb exceptions."""


class ConfigurationError(MontyError):
    """Raised when something is incorrectly configured.
    """


class OperationFailure(MontyError):
    """Raised when a database operation fails.
    """


class InvalidOperation(MontyError):
    """Raised when a client attempts to perform an invalid operation."""


class InvalidName(MontyError):
    """Raised when an invalid name is used.
    """


class CollectionInvalid(MontyError):
    """Raised when collection validation fails.
    """


class DocumentTooLarge(InvalidDocument):
    """Raised when an encoded document is too large.
    """
