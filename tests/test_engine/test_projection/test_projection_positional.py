
import pytest

from pymongo.errors import OperationFailure as mongo_op_fail
from montydb.errors import OperationFailure as monty_op_fail


def count_documents(cursor, spec=None):
    return cursor.collection.count_documents(spec or {})


def test_projection_positional_1(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": 1}, {"b": 3}]}
    ]
    spec = {"a.b": {"$gt": 2}}
    proj = {"a.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_2(monty_proj, mongo_proj):
    docs = [
        {"a": 85, "b": [{"x": 1, "y": 5}, {"x": 5, "y": 12}]},
        {"a": 60, "b": [{"x": 4, "y": 8}, {"x": 0, "y": 6}]},
        {"a": 90, "b": [{"x": 2, "y": 12}, {"x": 3, "y": 7}]},
    ]
    proj = {"b.$": 1}

    def run(spec):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    spec = {"a": {"$gt": 80}, "b.x": {"$gt": 4}}
    run(spec)

    spec = {"b.x": {"$gt": 4}, "a": {"$gt": 80}}
    run(spec)


def test_projection_positional_3(monty_proj, mongo_proj):
    docs = [
        {"a": [{"x": [1]}, {"x": [5]}]},
        {"a": [{"x": [4]}, {"x": [0]}]},
        {"a": [{"x": [2]}, {"x": [3]}]},
    ]
    spec = {"a.x": {"$gt": 4}}
    proj = {"a.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_4(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [1, 2, 3]}}
    ]
    spec = {"a.b": 2}
    proj = {"a.b.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_5(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [1, 2, 3], "c": [4, 5, 6]}},
        {"a": {"b": [1, 2, 3], "c": [4]}},
    ]
    spec = {"a.b": 2}
    proj = {"a.c.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 2
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    for i in range(2):
        assert next(mongo_c) == next(monty_c)


def test_projection_positional_6(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [{"c": [1]}, {"c": [2]}, {"c": [3]}]}},
    ]
    spec = {"a.b.c": 2}
    proj = {"a.b.c.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_7(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [{"c": [1, 5]}, {"c": 2}, {"c": [3]}]}},
    ]
    spec = {"a.b.c": 2}
    proj = {"a.b.c.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_8(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]},
    ]
    spec = {"a.b.1": {"$eq": 6}}
    proj = {"a.b.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_9(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": 2}, {"b": [3]}]},
    ]
    spec = {"a.b.1": 5}
    proj = {"a.b.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_10(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [{"c": 5}, {"c": 10}], "x": [{"c": 5}, {"c": 10}]}},
    ]
    spec = {"a.x.c": 5}
    proj = {"a.b.x.$": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_11(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [0, 1, 2]}, {"b": [3, 2, 4]}]},
    ]

    def run(spec, proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    spec = {"a.b.2": 4}
    proj = {"a.b.$": 1}
    run(spec, proj)

    spec = {"a.b": 2}
    proj = {"a.b.$": 1}
    run(spec, proj)

    spec = {"a.b": 2}
    proj = {"a.$.b": 1}
    run(spec, proj)

    spec = {"a.b": 2}
    proj = {"$.a.b": 1}
    run(spec, proj)

    spec = {"a.b": 2}
    proj = {"a": 1, "$.a.b": 1}
    run(spec, proj)

    for ie in range(2):
        spec = {"a.b": 2}
        proj = {"a": ie}
        run(spec, proj)

    spec = {"a.b": 2}
    proj = {"a.b.0.$": 1}
    run(spec, proj)

    spec = {"a.b": 2}
    proj = {"a.b.0.1.$": 1}
    run(spec, proj)

    spec = {"a.b": 2}
    proj = {"a.b.0.1.x.$": 1}
    run(spec, proj)

    for ie in range(2):
        spec = {"a.b": 2}
        proj = {"a.b.0.1.x": ie}
        run(spec, proj)

    for ie in range(2):
        spec = {}
        proj = {"a.0.b.x": ie}
        run(spec, proj)

        proj = {"a.b.1": ie}
        run(spec, proj)

        proj = {"a.b": ie}
        run(spec, proj)


def test_projection_positional_12(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [{"c": 1}, {"x": 1}]}, {"b": [{"c": 1}, {"x": 1}]}]},

        {"a": [{"b": [{"c": [0, 1]}, {"5": 8}, "hello", {"x": 1}, 8]},
               {"b": [{"c": {"1": 8}}, "world", {"x": 1}, 0]}]}
    ]
    spec = {}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 2
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        for i in range(2):
            assert next(mongo_c) == next(monty_c)

    for ie in range(2):
        proj = {"a.b.5": ie}
        run(proj)

        proj = {"a.b.c.1": ie}
        run(proj)

        proj = {"a.x": ie}
        run(proj)

        proj = {"a.0.b": ie}
        run(proj)

        proj = {"a.b.s": ie}
        run(proj)

        proj = {"a.b.c.": ie}  # Redundant dot
        run(proj)

        proj = {"a.b.c": ie}
        run(proj)


def test_projection_positional_13(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": 2}, {"b": [3, 10, 4]}],
         "c": [{"b": [1]}, {"b": 2}, {"b": [3, 5]}]},
    ]
    spec = {"a.b.0": 3, "c.b.1": 5}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a.b.$": 1}
    run(proj)

    proj = {"a.$.b": 1}
    run(proj)


def test_projection_positional_14(monty_proj, mongo_proj):
    docs = [
        {"a": 5, "b": {"c": 5, "g": 0}, "x": [1, 2]}
    ]
    spec = {}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    for ie in range(2):
        proj = {"a": ie, "x.1": ie, "b.g": ie}
        run(proj)


def test_projection_positional_15(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [0, 1, {"c": 5}]}, {"b": [3, 2, {"x": 5}]}]},
    ]
    spec = {"a.b.1": 1}
    proj = {"a.b.$": 1, "a.b.x": 1}

    monty_c = monty_proj(docs, spec, proj)
    mongo_c = mongo_proj(docs, spec, proj)

    assert count_documents(mongo_c, spec) == 1
    assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
    assert next(mongo_c) == next(monty_c)


def test_projection_positional_err_2(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [0, 1, 2]}, {"b": [3, 2, 4]}],
         "b": [{"b": [0, 1, 2]}, {"b": [3, 2, 4]}]}
    ]
    spec = {"b.0.b": 2}
    proj = {"a.b.$": 1}

    with pytest.raises(mongo_op_fail) as mongo_err:
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail) as monty_err:
        next(monty_proj(docs, spec, proj))

    # ignore comparing error code\n# # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_projection_positional_err_96_1(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [0, 1, 2]}, {"b": [3, 2, 4]}]}
    ]
    spec = {"a.0.b": 2}
    proj = {"a.b.$": 1}

    with pytest.raises(mongo_op_fail) as mongo_err:
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail) as monty_err:
        next(monty_proj(docs, spec, proj))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_projection_positional_err_96_2(monty_proj, mongo_proj):
    docs = [
        {"a": [{"b": [0, 1, 2]}, {"b": [3, 2, 4]}]}
    ]
    spec = {"a.0.b": 2}
    proj = {"a.$": 1}

    with pytest.raises(mongo_op_fail) as mongo_err:
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail) as monty_err:
        next(monty_proj(docs, spec, proj))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_projection_positional_err_96_3(monty_proj, mongo_proj):
    docs = [
        {"a": [0, 1]}
    ]
    spec = {"a.1": 1}
    proj = {"a.$": 1}

    with pytest.raises(mongo_op_fail) as mongo_err:
        next(mongo_proj(docs, spec, proj))

    with pytest.raises(monty_op_fail) as monty_err:
        next(monty_proj(docs, spec, proj))

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_projection_positional_16(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": {"c": [1, 2, 3]}, "d": [1]}}
    ]
    spec = {"a.b.c": 2}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a.b.$": 1}
    run(proj)

    proj = {"a.d.$": 1}
    run(proj)


def test_projection_positional_17(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [0, 1]}},
    ]
    spec = {"a.b.1": 1}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a.b.$": 1}
    run(proj)

    proj = {"a.b.1": 1}
    run(proj)


def test_projection_positional_18(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": [[1], 1]}},
    ]
    spec = {"a.b.1": 1}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a.b.$": 1}
    run(proj)

    proj = {"a.b.1": 1}
    run(proj)


def test_projection_positional_19(monty_proj, mongo_proj):
    docs = [
        {"a": {"b": {"c": [0, 1]}}},
    ]
    spec = {"a.b.c.1": 1}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a.b.c.$": 1}
    run(proj)

    proj = {"a.b.c.1": 1}
    run(proj)


def test_projection_positional_20(monty_proj, mongo_proj):
    docs = [
        {"a": [{"1": 1}, 1]},
    ]
    spec = {"a.1": 1}

    def run(proj):
        monty_c = monty_proj(docs, spec, proj)
        mongo_c = mongo_proj(docs, spec, proj)

        assert count_documents(mongo_c, spec) == 1
        assert count_documents(monty_c, spec) == count_documents(mongo_c, spec)
        assert next(mongo_c) == next(monty_c)

    proj = {"a.$": 1}
    run(proj)
