
import os

from bson import decode_all, BSON
from bson.py3compat import string_type
from bson.json_util import (
    loads as _loads,
    dumps as _dumps,
    RELAXED_JSON_OPTIONS as _default_json_opts,
)
from ..client import MontyClient
from ..errors import DuplicateKeyError


def _collection(database, collection):
    client = MontyClient()
    return client[database][collection]


def montyimport(database,
                collection,
                file,
                mode="insert",
                json_options=None):
    """Imports content from an Extended JSON file into a MontyCollection instance

    Example:
        >>> from montydb import open_repo, utils
        >>> with open_repo("foo/bar"):
        >>>     utils.montyimport("db", "col", "/data/dump.json")
        >>>

    Args:
        database (str): Database name
        collection (str): Collection name to import to
        file (str): Input file path
        mode (str): Specifies how the import process should handle existing
                    documents in the database that match documents in the
                    import file.
                    Options: ["insert", "upsert", "merge"]
                    Default: "insert"
        json_options (JSONOptions): A JSONOptions instance used to modify
                                    the decoding of MongoDB Extended JSON
                                    types. Default None.

    """
    collection = _collection(database, collection)
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


def montyexport(database,
                collection,
                out,
                fileds=None,
                query=None,
                json_options=None):
    """Produces a JSON export of data stored in a MontyDB instance

    Example:
        >>> from montydb import open_repo, utils
        >>> with open_repo("foo/bar"):
        >>>     utils.montyexport("db", "col", "/data/dump.json")
        >>>

    Args:
        database (str): Database name
        collection (str): Collection name to export from
        out (str): Output file path
        fields (str, list): Specifies a field name string or a list fields
                            to include in the export.
        query (dict): Provides a query document to return matching documents
                      in the export.
        json_options (JSONOptions): A JSONOptions instance used to modify
                                    the decoding of MongoDB Extended JSON
                                    types. Default None.

    """
    collection = _collection(database, collection)
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


def montyrestore(database, collection, dumpfile):
    """Loads a binary database dump into a MontyCollection instance

    Should be able to accept the dump created by `mongodump`.

    Example:
        >>> from montydb import open_repo, utils
        >>> with open_repo("foo/bar"):
        >>>     utils.montyrestore("db", "col", "/data/dump.bson")
        >>>

    Args:
        database (str): Database name
        collection (str): Collection name to load into
        dumpfile (str): File path to load from

    """
    collection = _collection(database, collection)

    with open(dumpfile, "rb") as fp:
        raw = fp.read()

    try:
        collection.insert_many(decode_all(raw))
    except DuplicateKeyError:
        pass


def montydump(database, collection, dumpfile):
    """Creates a binary export from a MontyCollection instance

    The export should be able to be accepted by `mongorestore`.

    Example:
        >>> from montydb import open_repo, utils
        >>> with open_repo("foo/bar"):
        >>>     utils.montydump("db", "col", "/data/dump.bson")
        >>>

    Args:
        database (str): Database name
        collection (str): Collection name to export from
        dumpfile (str): File path to export to

    """
    collection = _collection(database, collection)

    raw = b""
    for doc in collection.find():
        raw += BSON.encode(doc)

    with open(dumpfile, "wb") as fp:
        fp.write(raw)
