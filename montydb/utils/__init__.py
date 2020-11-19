
try:
    import bson
except ImportError:
    class _mock(object):
        def __init__(self, *args, **kwargs):
            raise ImportError("No module named 'bson'")

    montyimport = _mock
    montyexport = _mock
    montyrestore = _mock
    montydump = _mock
    MongoQueryRecorder = _mock
    MontyList = _mock

else:

    from .io import (
        montyimport,
        montyexport,
        montyrestore,
        montydump,
        MongoQueryRecorder,
    )
    from .mt_list import MontyList


__all__ = [
    "montyimport",
    "montyexport",
    "montyrestore",
    "montydump",
    "MongoQueryRecorder",

    "MontyList",
]
