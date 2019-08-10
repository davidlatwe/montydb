
VERSION_MAJOR = 1
VERSION_MINOR = 2
VERSION_PATCH = 1

version_info = (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
version = "%i.%i.%i" % version_info
__version__ = version


MONGO_VER_MAJOR = 3
MONGO_VER_MINOR = 6
MONGO_VER_PATCH = 4

mongo_version_info = (MONGO_VER_MAJOR, MONGO_VER_MINOR, MONGO_VER_PATCH)
mongo_version = "%i.%i.%i" % mongo_version_info

__all__ = (
    "version",
    "version_info",
    "mongo_version",
    "mongo_version_info",
    "__version__",
)
