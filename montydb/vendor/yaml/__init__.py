import sys

PY3 = sys.version_info[0] == 3

if PY3:
    from .yaml3 import *
else:
    from .yaml2 import *
