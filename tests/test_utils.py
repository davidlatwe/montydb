
import os
import base64
import pytest

from montydb import open_repo
from montydb.utils import (
    montyimport,
    montyexport,
    montyrestore,
    montydump,
    MongoQueryRecorder,

    MontyList,
)

from .conftest import skip_if_no_bson

from bson import BSON, json_util
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.int64 import Int64
from bson.decimal128 import Decimal128
from bson.binary import Binary
from bson.regex import Regex
from bson.code import Code


DOCUMENTS = [
    {"_id": 0, "a": [False, True]},
    {"_id": 1, "a": None},
    {"_id": 2, "a": "appple"},
    {"_id": 3, "a": [{"s": 5.5}]},
    {"_id": 4, "a": {"s": [Int64(9)]}},
    {"_id": 5, "a": Decimal128("4.5")},
    {"_id": 6, "a": Binary(b"0")},
    {"_id": 7, "a": Regex("^b")},
    {"_id": 8, "a": Code("x", {"m": 0})},
    {"_id": 9, "a": MinKey()},
    {"_id": 10, "a": MaxKey()},
    {"_id": 11, "a": Timestamp(0, 1)},
    {"_id": 12, "a": ObjectId(b"000000000000")},
]

SERIALIZED = """
{"_id": 0, "a": [false, true]}
{"_id": 1, "a": null}
{"_id": 2, "a": "appple"}
{"_id": 3, "a": [{"s": 5.5}]}
{"_id": 4, "a": {"s": [9]}}
{"_id": 5, "a": {"$numberDecimal": "4.5"}}
{"_id": 6, "a": {"$binary": {"base64": "MA==", "subType": "00"}}}
{"_id": 7, "a": {"$regularExpression": {"pattern": "^b", "options": ""}}}
{"_id": 8, "a": {"$code": "x", "$scope": {"m": 0}}}
{"_id": 9, "a": {"$minKey": 1}}
{"_id": 10, "a": {"$maxKey": 1}}
{"_id": 11, "a": {"$timestamp": {"t": 0, "i": 1}}}
{"_id": 12, "a": {"$oid": "303030303030303030303030"}}
""".strip()


BINARY = b"""
HgAAABBfaWQAAAAAAARhAA0AAAAIMAAACDEAAQAAEQAAABBfaWQAAQAAAAphAAAcAAAAEF9pZAAC
AAAAAmEABwAAAGFwcHBsZQAAKQAAABBfaWQAAwAAAARhABgAAAADMAAQAAAAAXMAAAAAAAAAFkAA
AAApAAAAEF9pZAAEAAAAA2EAGAAAAARzABAAAAASMAAJAAAAAAAAAAAAACEAAAAQX2lkAAUAAAAT
YQAtAAAAAAAAAAAAAAAAAD4wABcAAAAQX2lkAAYAAAAFYQABAAAAADAAFQAAABBfaWQABwAAAAth
AF5iAAAAJwAAABBfaWQACAAAAA9hABYAAAACAAAAeAAMAAAAEG0AAAAAAAAAEQAAABBfaWQACQAA
AP9hAAARAAAAEF9pZAAKAAAAf2EAABkAAAAQX2lkAAsAAAARYQABAAAAAAAAAAAdAAAAEF9pZAAM
AAAAB2EAMDAwMDAwMDAwMDAwAA==
""".replace(b"\n", b"")


JSON_DUMP = "dumped.json"
BSON_DUMP = "dumped.bson"


@skip_if_no_bson
def test_utils_montyimport(monty_client, tmp_monty_utils_repo):
    database = "dump_db_JSON"
    collection = "dump_col_JSON"

    with open_repo(tmp_monty_utils_repo):
        with open(JSON_DUMP, "w") as dump:
            dump.write(SERIALIZED)

        montyimport(database, collection, JSON_DUMP)

        col = monty_client[database][collection]
        for i, doc in enumerate(col.find(sort=[("_id", 1)])):
            assert doc == BSON.encode(DOCUMENTS[i]).decode()

        os.remove(JSON_DUMP)


@skip_if_no_bson
def test_utils_montyexport(monty_client, tmp_monty_utils_repo):
    database = "dump_db_JSON"
    collection = "dump_col_JSON"

    # TODO: should not rely on other test
    test_utils_montyimport(monty_client, tmp_monty_utils_repo)

    with open_repo(tmp_monty_utils_repo):
        montyexport(database, collection, JSON_DUMP)

        loaded_examples = list()
        loaded_exported = list()

        with open(JSON_DUMP, "r") as dump:
            data = dump.read().strip()
            for d, s in zip(data.split("\n"), SERIALIZED.split("\n")):
                loaded_exported.append(json_util.loads(d))
                loaded_examples.append(json_util.loads(s))

        sort = (lambda l: sorted(l, key=lambda i: i["_id"]))
        for d, s in zip(sort(loaded_exported), sort(loaded_examples)):
            assert d == s

        os.remove(JSON_DUMP)


@skip_if_no_bson
def test_utils_montyrestore(monty_client, tmp_monty_utils_repo):
    database = "dump_db_BSON"
    collection = "dump_col_BSON"

    with open_repo(tmp_monty_utils_repo):
        with open(BSON_DUMP, "wb") as dump:
            dump.write(base64.b64decode(BINARY))

        montyrestore(database, collection, BSON_DUMP)

        col = monty_client[database][collection]
        for i, doc in enumerate(col.find(sort=[("_id", 1)])):
            assert doc == BSON.encode(DOCUMENTS[i]).decode()

        os.remove(BSON_DUMP)


@skip_if_no_bson
def test_utils_montydump(monty_client, tmp_monty_utils_repo):
    database = "dump_db_BSON"
    collection = "dump_col_BSON"

    if monty_client.server_info()["storageEngine"] == "lightning":
        pytest.skip("LMDB's document natural order is lexicographic, not easy "
                    "to match with MongoDB's natural order but safe to skip.")

    # TODO: should not rely on other test
    test_utils_montyrestore(monty_client, tmp_monty_utils_repo)

    with open_repo(tmp_monty_utils_repo):
        montydump(database, collection, BSON_DUMP)

        with open(BSON_DUMP, "rb") as dump:
            raw = dump.read()
            assert base64.b64encode(raw) == BINARY


# Ignored, seems not working after mongo-4.x
def _test_MongoQueryRecorder(mongo_client):
    mongo_client.drop_database("recordTarget")  # ensure clean db
    db = mongo_client["recordTarget"]

    _docs_ = [
        {"_id": 1, "a": 1},
        {"_id": 2, "a": 2},
        {"_id": 3, "a": 3},
        {"_id": 4, "a": 4},
        {"_id": 5, "a": 5},
        {"_id": 6, "b": 1},
        {"_id": 7, "b": 2},
        {"_id": 8, "b": 3},
        {"_id": 9, "b": 4},
    ]

    # Put some docs
    col = db["testCol"]
    col.insert_many(_docs_)

    # Start
    recorder = MongoQueryRecorder(db)
    recorder.start()
    assert recorder.current_level() == 2

    # Make some queries
    # id: 2, 3, 4
    list(col.find({"a": {"$gte": 2, "$lte": 4}}))
    # id: 8, 9
    col.distinct("b", filter={"b": {"$gte": 3}})

    # End
    recorder.stop()
    assert recorder.current_level() == 0

    results = recorder.extract()

    assert len(results) == 1
    assert "testCol" in results

    ids = {doc["_id"] for doc in results["testCol"]}
    assert ids == {2, 3, 4, 8, 9}


def test_MontyList_find():
    mt = MontyList([1, 4, 3, {"a": 5}, {"a": 4}])
    mt_find = mt.find({"a": {"$exists": 1}})
    assert len(mt_find) == 2

    mt = MontyList([1, 4, 3, {"a": 5, "b": 1}, {"a": 4, "b": 0}])
    mt_find = mt.find({"a": {"$exists": 1}}, {"a": 0}, [("a", 1)])
    assert mt_find[0] == {"b": 0}
    assert mt_find[1] == {"b": 1}


def test_MontyList_sort():
    mt = MontyList([1, 4, 3, {"a": 5}, {"a": 4}])
    mt.sort("a", 1)
    assert mt == [1, 4, 3, {"a": 4}, {"a": 5}]


def test_MontyList_iter():
    mt = MontyList([1, 2, 3])
    assert next(mt) == 1
    assert next(mt) == 2
    assert next(mt) == 3
    with pytest.raises(StopIteration):
        next(mt)
    mt.rewind()
    assert next(mt) == 1


def test_MontyList_compare():
    mt = MontyList([1, 2, 3])
    assert mt == [1, 2, 3]
    assert mt != [1, 2, 0]
    assert mt > [1, 2, 0]
    assert mt < [1, 2, {"a": 0}]
    assert mt >= [1, 2, 0]
    assert mt <= [1, 2, {"a": 0}]

    assert mt > {"a": 0}
    assert mt > None
    assert mt > "string type"
    assert mt < True
    assert mt < MontyList([5])


def test_MontyList_getitem_err():
    mt = MontyList([1, 2, 3])
    with pytest.raises(TypeError):
        mt["ind"]
