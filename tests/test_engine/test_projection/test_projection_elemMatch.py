
import pytest

from pymongo.errors import OperationFailure as mongo_op_fail
from montydb.errors import OperationFailure as monty_op_fail

def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_projection_elemMatch_unsupported_option(monty_proj, mongo_proj):
    docs = [
        {"x": {"a": [{"b": 1}, {"b": 3}]}},
    ]
    spec = {}
    proj = {"x": {"a": {"$elemMatch": {"b": 3}}}}

    with pytest.raises(mongo_op_fail) as mongo_err:
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail) as monty_err:
        next(monty_proj(docs, spec, proj))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_projection_elemMatch_nested_field(monty_proj, mongo_proj):
    docs = [
        {"x": {"a": [{"b": 1}, {"b": 3}]}},
    ]
    spec = {}
    proj = {"x.a": {"$elemMatch": {"b": 3}}}

    with pytest.raises(mongo_op_fail) as mongo_err:
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail) as monty_err:
        next(monty_proj(docs, spec, proj))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_projection_elemMatch_1(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": 1}, {"b": 3}]}
    ]
    spec = {}
    proj = {"a": {"$elemMatch": {"b": 3}}}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_elemMatch_2(monty_proj, mongo_proj):
    docs = [
        {"a": 85, "b": [{"x": 1, "y": 5}, {"x": 5, "y": 12}]},
        {"a": 60, "b": [{"x": 4, "y": 8}, {"x": 0, "y": 6}]},
        {"a": 90, "b": [{"x": 2, "y": 12}, {"x": 3, "y": 7}]},
    ]
    spec = {"a": {"$gt": 80}}
    proj = {"b": {"$elemMatch": {"x": {"$in": [0, 2, 3]},
                                 "y": {"$mod": [2, 0]}}}}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_projection_elemMatch_3(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1, 2, 5, 6]}
    ]
    spec = {}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a": {"$elemMatch": {"$eq": 2}}}
    run(proj)


def test_projection_elemMatch_4(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": 1}, {"b": 3}], "x": 100}
    ]
    spec = {}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a": {"$elemMatch": {"b": 3}}, "x": 1}
    run(proj)

    proj = {"a": {"$elemMatch": {"b": 3}}, "x": 0}
    run(proj)


def test_projection_elemMatch_mix_with_slice_1(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1, 2, 5, 6]}
    ]
    spec = {}
    proj = {"a": {"$elemMatch": {"$eq": 2}, "$slice": [1, 4]}}

    with pytest.raises(mongo_op_fail) as mongo_err:
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail) as monty_err:
        next(monty_proj(docs, spec, proj))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
