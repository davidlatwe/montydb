
import pytest
from montydb import errors
from montydb.types import bson


@pytest.mark.parametrize("doc", [
    {".b": 1},
    {"$b": 1},
    {"a.b": 1},
    {"a": {"$b": 1}},
])
def test_insert_dot_or_dollar_key(monty_find, mongo_find, mongo_version, doc):
    spec = {}

    if mongo_version[:2] < [5, 0]:
        # Before MongoDB 5.0, pymongo 3 rejected these field names on the
        # client side. pymongo 4 does not, so only montydb is tested here.
        with pytest.raises(errors.InvalidDocument):
            monty_find([dict(doc)], spec)
        return

    docs = [dict(doc)]
    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert next(mongo_c) == next(monty_c)


def test_insert_bytes(monty_find, mongo_find):
    docs = [
        {"data": b"some bytes"},
    ]
    spec = {}

    monty_c = monty_find(docs, spec)
    mongo_c = mongo_find(docs, spec)

    assert next(monty_c) == next(mongo_c)


@pytest.mark.parametrize("doc", [
    {"a": {"$date": 1700000000000}},
    {"a": {"$oid": "x"}},
    {"a": [{"$binary": {"base64": "", "subType": "00"}}]},
    {"a": {"$regex": "^x", "$options": ""}},
])
def test_insert_type_marker_key(monty_find, mongo_find, mongo_version, doc):
    if mongo_version[:2] < [5, 0]:
        pytest.skip("MongoDB before 5.0 rejects all '$'-prefixed field names")

    expected = next(mongo_find([dict(doc)], {}))

    if not bson.bson_used:
        # Without bson, montydb reads these keys back as values of other
        # types, so it must not store them.
        with pytest.raises(errors.InvalidDocument):
            monty_find([dict(doc)], {})
        return

    assert next(monty_find([dict(doc)], {})) == expected


def _error_of(action):
    with pytest.raises(Exception) as excinfo:
        action()
    return excinfo.value


@pytest.mark.parametrize("_id", [
    {"$bad": 1},
    {"a": {"$b": 1}},
    {"a": [{"$b": 1}]},
])
def test_insert_dollar_field_in_id(monty_database, mongo_database, mongo_version,
                                   _id):
    if mongo_version[:2] < [5, 0]:
        pytest.skip("MongoDB before 5.0 rejects all '$'-prefixed field names")

    actions = [
        lambda col: col.insert_one({"_id": _id}),
    ]
    # MongoDB 6.0 and 7.0 accept a nested '$' field in an upserted `_id`,
    # although their insert rejects it. montydb keeps the strict rule.
    if "$bad" in _id or mongo_version[:2] not in ([6, 0], [7, 0]):
        actions.append(
            lambda col: col.replace_one({"x": 1}, {"_id": _id}, upsert=True)
        )

    for db in (monty_database, mongo_database):
        db.drop_collection("test")
        col = db["test"]

        for action in actions:
            error = _error_of(lambda: action(col))
            assert (type(error).__name__, error.code) == ("WriteError", 52)

        error = _error_of(
            lambda: col.insert_many([{"_id": 0}, {"_id": _id}, {"_id": 2}])
        )
        assert type(error).__name__ == "BulkWriteError"
        assert error.details["nInserted"] == 1
        write_errors = error.details["writeErrors"]
        assert [(e["index"], e["code"]) for e in write_errors] == [(1, 52)]
        assert list(col.find({})) == [{"_id": 0}]


@pytest.mark.parametrize("_id", [
    {"a.b": 1},
    {"$ref": "c", "$id": 1},
])
def test_insert_allowed_field_in_id(monty_database, mongo_database, mongo_version,
                                    _id):
    if mongo_version[:2] < [5, 0]:
        pytest.skip("MongoDB before 5.0 rejects '.' and '$' in field names")

    found = []
    for db in (monty_database, mongo_database):
        db.drop_collection("test")
        db["test"].insert_one({"_id": dict(_id)})
        found.append(list(db["test"].find({})))

    if bson.bson_used or "$ref" not in _id:
        assert found[0] == found[1]
    else:
        # pymongo reads a DBRef document back as a `DBRef` object
        assert found[0] == [{"_id": _id}]
