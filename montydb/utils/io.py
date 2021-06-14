
import os
import time
from collections import defaultdict, OrderedDict
from datetime import datetime

from ..types import string_types, init_bson, bson as bson_
from ..client import MontyClient
from ..errors import DuplicateKeyError


def _collection(database, collection):
    client = MontyClient()
    return client[database][collection]


def montyimport(database,
                collection,
                file,
                mode="insert",
                json_options=None,
                use_bson=False):
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
    init_bson(use_bson)
    collection = _collection(database, collection)

    with open(file, "r") as fp:
        lines = [line.strip() for line in fp.readlines()]
        serialized = "[{}]".format(", ".join(lines))

    documents = bson_.json_loads(serialized, json_options=json_options)

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
                fields=None,
                query=None,
                json_options=None,
                use_bson=False):
    """Produces a JSON export of data stored in a MontyCollection instance

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
    init_bson(use_bson)
    collection = _collection(database, collection)
    fields = fields or []

    out = os.path.abspath(out)
    if not os.path.isdir(os.path.dirname(out)):
        os.makedirs(os.path.dirname(out))

    if isinstance(fields, string_types):
        fields = [fields]

    projection = {field: True for field in fields} or None

    with open(out, "w") as fp:
        for doc in collection.find(query, projection=projection):
            serialized = bson_.json_dumps(doc, json_options=json_options)
            fp.write(serialized + "\n")


def montyrestore(database, collection, dumpfile):
    """Loads a binary database dump into a MontyCollection instance

    Should be able to accept the dump created by `mongodump`.
    bson required.

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
    from bson import decode_all

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
    bson required.

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
    from bson import BSON

    collection = _collection(database, collection)

    dumpfile = os.path.abspath(dumpfile)
    if not os.path.isdir(os.path.dirname(dumpfile)):
        os.makedirs(os.path.dirname(dumpfile))

    raw = b""
    for doc in collection.find():
        raw += BSON.encode(doc)

    with open(dumpfile, "wb") as fp:
        fp.write(raw)


class MongoQueryRecorder(object):
    """Record MongoDB query results in a period of time

    :Important: Requires to access database profiler.

    This works via filtering the database profile data and reproduce the
    queries of `find` and `distinct` commands.

    bson required.

    Example:
        >>> from pymongo import MongoClient
        >>> from montydb.utils import MongoQueryRecorder
        >>> client = MongoClient()
        >>> recorder = MongoQueryRecorder(client["mydb"])
        >>> recorder.start()
        >>> # Make some queries or run the App...
        >>> recorder.stop()
        >>> recorder.extract()
        {<collection_1>: [<doc_1>, <doc_2>, ...], ...}

    Args:
        mongodb (pymongo.database.Database): An instance of mongo database
        namespace (str or regex, optional): A MongoDB namespace string/regex.
        user (str, optional): Name of authenticated user to record with.

    """

    def __init__(self, mongodb, namespace=None, user=None):
        self._mongodb = mongodb
        self._namespace = namespace or {"$regex": mongodb.name + r"\..*"}
        self._user = user

        self._epoch = datetime(1970, 1, 1)
        self._rec_stime = None
        self._rec_etime = None

    def __repr__(self):
        return ("MongoQueryRecorder(mongodb=%s, namespace=%s, user=%s)"
                "" % (self._mongodb.name, self._namespace, self._user))

    def reset_profile(self, level=0):
        """Drop and reset database profile

        Args:
            level (int): Database profile level, default 0.

        """
        self._mongodb.command({"profile": 0})
        self._mongodb.system.profile.drop()
        if level:
            self._mongodb.command({"profile": level})

    def current_level(self):
        """Return current database's profile level"""
        return self._mongodb.command({"profile": -1})["was"]

    def start(self):
        """Start recording and set database profile level to 2"""
        self._mongodb.command({"profile": 2})
        self._rec_stime = datetime.utcnow()
        time.sleep(0.1)  # Wait for db

    def stop(self):
        """Stop recording and set database profile level to 0"""
        time.sleep(0.1)  # Wait for db
        self._rec_etime = datetime.utcnow()
        self._mongodb.command({"profile": 0})

    def extract(self):
        """Collect documents via previous queries

        Via filtering the `[database].system.profile`, parsing previous
        commands to reproduce the query results.

        NOTE: Depend on the `namespace`, the result may across multiple
              collections.

        Returns:
            dict: A dict of {collection: list of documents}

        """
        from bson.codec_options import CodecOptions

        filter = {
            "$or": [
                {
                    "op": "query",
                    "command.find": {"$exists": True},
                    "nreturned": {"$gte": 1}
                },
                {
                    "op": "command",
                    "command.distinct": {"$exists": True}
                },
            ],
            "ns": self._namespace,
            "ts": {"$gte": self._rec_stime, "$lte": self._rec_etime},
        }

        if self._user is not None:
            filter.update({"user": self._user})

        projection = {
            "op": 1,

            "command.find": 1,
            "command.filter": 1,
            "command.sort": 1,
            "command.limit": 1,

            "command.distinct": 1,
            "command.key": 1,
            "command.query": 1
        }

        profile = self._mongodb.system.profile
        code_opt = CodecOptions(document_class=OrderedDict)
        profile = profile.with_options(codec_options=code_opt)

        history = defaultdict(list)
        for log in profile.find(filter, projection=projection):
            op = log.pop("op")
            if log not in history[op]:
                history[op].append(log)

        documents = defaultdict(dict)

        # Query - find
        for cmd in (log["command"] for log in history["query"]):
            col = cmd["find"]
            filter = cmd["filter"]
            limit = cmd.get("limit", 0)
            sort = None

            if limit:
                sort = list()
                for k, v in cmd.get("sort", dict()):
                    sort.append((k, v))

            for doc in self._mongodb[col].find(filter, sort=sort, limit=limit):
                id = doc["_id"]
                if id not in documents[col]:
                    documents[col][id] = doc

        # Command - distinct
        for cmd in (log["command"] for log in history["command"]):
            col = cmd["distinct"]
            key = cmd["key"]
            query = cmd.get("query")

            for value in self._mongodb[col].distinct(key, query):
                doc = self._mongodb[col].find_one({key: value})
                id = doc["_id"]
                if id not in documents[col]:
                    documents[col][id] = doc

        # Done
        return {col: list(docs.values()) for col, docs in documents.items()}
