
import os

from bson import decode_all, BSON
from bson.py3compat import string_type
from bson.json_util import (
    loads as _loads,
    dumps as _dumps,
    RELAXED_JSON_OPTIONS as _default_json_opts,
)

from ..collection import MontyCollection
from ..errors import DuplicateKeyError


def _validate_monty_collection(obj):
    if not isinstance(obj, MontyCollection):
        raise TypeError("Not a `MontyCollection` instance.")


def montyimport(collection, file, mode="insert", json_options=None):

    _validate_monty_collection(collection)

    opt = json_options or _default_json_opts

    with open(file, "r") as fp:
        lines = [line.strip() for line in fp.readlines()]
        serialized = "[{}]".format(", ".join(lines))

    documents = _loads(serialized, json_options=opt)

    if mode == "insert":
        for doc in documents:
            try:
                collection.insert_one(doc)
            except DuplicateKeyError:
                print("Duplicate id: %s" % doc["_id"])

    elif mode == "upsert":
        for doc in documents:
            collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    elif mode == "merge":
        for doc in documents:
            update = {"$setOnInsert": {k: v} for k, v in doc.items()}
            collection.update_one({"_id": doc["_id"]}, update, upsert=True)


def montyexport(collection, out, fileds=None, query=None, json_options=None):

    _validate_monty_collection(collection)

    opt = json_options or _default_json_opts
    fileds = fileds or []

    if not os.path.isdir(os.path.dirname(out)):
        os.makedirs(os.path.dirname(out))

    if isinstance(fileds, string_type):
        fileds = [fileds]

    projection = {field: True for field in fileds}

    with open(out, "w") as fp:
        for doc in collection.find(query, projection=projection):
            serialized = _dumps(doc, json_options=opt)
            fp.write(serialized + "\n")


def montyrestore(collection, dumpfile):
    """Loads a binary database dump into a MontyCollection instance

    Should be able to accept the dump created by `mongodump`.

    Args:
        collection (MontyCollection): Collection object to load into
        dumpfile (str): File path to load from

    """
    _validate_monty_collection(collection)

    with open(dumpfile, "rb") as fp:
        raw = fp.read()

    for doc in decode_all(raw):
        try:
            collection.insert_one(doc)
        except DuplicateKeyError:
            pass


def montydump(collection, dumpfile):
    """Creates a binary export from a MontyCollection instance

    The export should be able to be accepted by `mongorestore`.

    Args:
        collection (MontyCollection): Collection object to export from
        dumpfile (str): File path to export to

    """
    _validate_monty_collection(collection)

    raw = b""
    for doc in collection.find():
        raw += BSON.encode(doc)

    with open(dumpfile, "wb") as fp:
        fp.write(raw)
