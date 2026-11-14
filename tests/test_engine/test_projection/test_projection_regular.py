
import pytest

from pymongo.errors import OperationFailure as mongo_op_fail
from montydb.errors import OperationFailure as monty_op_fail
from montydb.types import bson


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_projection_regular_1(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": 1, "c": True}, {"b": 3, "c": False}]}
    ]
    spec = {"a.b": {"$gt": 2}}
    proj = {"a.c": 0}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_regular_2(monty_proj, mongo_proj):
    docs = [
        {"a": 85, "b": [{"x": 1, "y": 5}, {"x": 5, "y": 12}]},
        {"a": 60, "b": [{"x": 4, "y": 8}, {"x": 0, "y": 6}]},
        {"a": 90, "b": [{"x": 2, "y": 12}, {"x": 3, "y": 7}]},
    ]
    spec = {"a": {"$gt": 80}, "b.x": {"$gt": 4}}
    proj = {"a": 1, "b.x": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_regular_3(monty_proj, mongo_proj):
    docs = [
        {"a": [{"x": [1]}, {"x": [5]}]},
        {"a": [{"x": [4]}, {"x": [0]}]},
        {"a": [{"x": [2]}, {"x": [3]}]},
    ]
    spec = {"a.x": {"$gt": 4}}
    proj = {"_id": 0}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_regular_err_mix_include_exclude(monty_proj, mongo_proj):
    docs = [
        {"a": "foo", "b": "bar"},
    ]
    spec = {}
    proj = {"a": 0, "b": 1}

    with pytest.raises(mongo_op_fail):
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail):
        next(monty_proj(docs, spec, proj))


def test_projection_path_collision_1(monty_proj, mongo_proj, mongo_version):
    if mongo_version[:2] < [4, 4]:
        return

    docs = [
        {"size": {"h": 10, "w": 5, "uom": "cm"}}
    ]
    spec = {"size.h": 10}
    proj = {"size": 1, "size.uom": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    with pytest.raises(mongo_op_fail):
        next(mongo_c)
        # OperationFailure: "Path collision at size.uom remaining portion uom"
    with pytest.raises(monty_op_fail):
        next(monty_c)


def test_projection_path_collision_2(monty_proj, mongo_proj, mongo_version):
    if mongo_version[:2] < [4, 4]:
        return

    docs = [
        {"size": {"h": 10, "w": 5, "uom": "cm"}}
    ]
    spec = {"size.h": 10}
    proj = {"size.uom": 1, "size": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)

    with pytest.raises(mongo_op_fail):
        next(mongo_c)
        # OperationFailure: "Path collision at size"
    with pytest.raises(monty_op_fail):
        next(monty_c)


def test_projection_path_collision_3(monty_proj, mongo_proj, mongo_version):
    if mongo_version[:2] < [4, 4]:
        return

    docs = [
        {"a": [{"b": [0, 1, {"c": 5}]}, {"b": [3, 2, {"x": 5}]}]},
    ]
    spec = {"a.b.1": 1}
    proj = {"a.b.$": 1, "a.b": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    with pytest.raises(mongo_op_fail):
        next(mongo_c)
        # OperationFailure: "Path collision at a.b"
    with pytest.raises(monty_op_fail):
        next(monty_c)


def _outcome(proj_fn, op_fail, docs, spec, proj):
    try:
        return "docs", list(proj_fn(docs, spec, proj))
    except op_fail as e:
        return "error", e.code


@pytest.mark.parametrize("proj", [
    {"_id": 0, "_id.x": 1},
    {"_id.x": 1, "_id": 0},
    {"$.a": 1},
    {"a.": 1},
    {"$.a": {"$slice": 1}},
    {"a.": {"$slice": 1}},
])
def test_projection_invalid_path(monty_proj, mongo_proj, mongo_version, proj):
    if mongo_version[:2] < [4, 4]:
        pytest.skip("MongoDB 4.4 began to check these paths")

    docs = [{"a": [1, 2]}]
    expected = _outcome(mongo_proj, mongo_op_fail, docs, {}, proj)
    assert expected[0] == "error"
    assert _outcome(monty_proj, monty_op_fail, docs, {}, proj) == expected


@pytest.mark.parametrize("proj", [
    {"a.x": 1, "a.$id": 1},
    {"a.$id": 1, "a.x": 1},
])
def test_projection_dbref_field(monty_proj, mongo_proj, mongo_version, proj):
    if mongo_version[:2] < [5, 0]:
        pytest.skip("MongoDB 5.0 began to accept '$'-prefixed field names")
    if bson.bson_used:
        pytest.skip("With bson, montydb cannot project into a stored DBRef yet")

    docs = [{"a": {"x": 1, "$id": 2, "$ref": "c"}}]
    expected = _outcome(mongo_proj, mongo_op_fail, docs, {}, proj)
    assert _outcome(monty_proj, monty_op_fail, docs, {}, proj) == expected
