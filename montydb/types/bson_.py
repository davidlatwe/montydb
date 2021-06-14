
import warnings
from .bson import init, bson_used  # noqa
from .bson import *  # noqa


warnings.warn(
    "montydb.types.bson_ is deprecated; import montydb.types.bson instead",
    DeprecationWarning
)
