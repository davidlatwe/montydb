from abc import ABCMeta, abstractmethod

from ..engine.helpers import with_metaclass


class AbstractStorage(with_metaclass(ABCMeta, object)):
    """
    """

    def __init__(self, repository=None, storage_config=None):
        self._is_opened = False
        self._repository = repository
        self._parse_config(storage_config)
        self._is_opened = True

    def __getattribute__(self, name):
        if not (name == "_is_opened" or
                object.__getattribute__(self, "_is_opened")):
            # Run re-open if not checking open status nor is opened.
            object.__getattribute__(self, "_re_open")()

        return object.__getattribute__(self, name)

    def _re_open(self):
        """Auto re-open"""
        self._is_opened = True

    def close(self):
        """Could do some clean up"""
        self._is_opened = False

    @property
    def repository(self):
        return self._repository

    @abstractmethod
    def __repr__(self):
        return NotImplemented

    @abstractmethod
    def _parse_config(self, config):
        return NotImplemented

    @abstractmethod
    def write_with_concern(self):
        return NotImplemented
