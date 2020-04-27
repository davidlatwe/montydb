import os


MONGO_COMPAT_36 = bool(os.getenv("MONGO_COMPAT_36", False))
ENABLE_BSON = bool(os.getenv("ENABLE_BSON", False))
