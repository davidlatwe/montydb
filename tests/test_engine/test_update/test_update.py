
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err


def test_update_err_1(monty_update, mongo_update):
    # testing update path conflict
    docs = [
        {"a": {"b": 4}}
    ]
    spec = {"$inc": {"a.b": 1}, "$min": {"a.b": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_1(monty_update, mongo_update):
    # testing useless array_filter
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem": {"c": {"$gt": 0}, "b": {"$gt": 4}}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}


def test_update_positional_filtered_err_2(monty_update, mongo_update):
    # testing array_filter duplicated top-level field error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}}, {"elem.c": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_3(monty_update, mongo_update):
    # testing array_filter multiple top-level field error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}}, {"other": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_4(monty_update, mongo_update):
    # testing array_filter not used error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}}, {"notuse": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_5(monty_update, mongo_update):
    # testing array_filter not used error
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 5, "c": 1}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1}}
    array_filters = [{"elem.b": {"$gt": 4}},
                     {"notuse": {"$gt": 4}},
                     {"notuse2": {"$gt": 4}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_6(monty_update, mongo_update):
    # testing bad identifier
    docs = [
        {"a": [{"b": {"c": 4, "d": 0}}, {"b": {"c": 4, "d": 1}}]}
    ]
    spec = {"$inc": {"a.$[elem.b].c": 1}}
    array_filters = [{"elem.b.d": {"$gt": 0}}]

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_err_7(monty_update, mongo_update):
    # testing no identifier
    docs = [
        {"a": [{"b": {"c": 4, "d": 0}}, {"b": {"c": 4, "d": 1}}]}
    ]
    spec = {"$inc": {"a.$[elem].b.c": 1}}
    array_filters = []

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_near_conflict(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1, "a.$[almost].b": 2}}
    array_filters = [{"elem.c": {"$gt": 0}},  # update element 0
                     {"almost": {"b": 4, "c": 0}}]  # update element 1

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 5, "c": 1}, {"b": 6, "c": 0}]}


def test_update_positional_filtered_multi(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 0, "c": [{"d": 80}, {"d": 95}]},
               {"b": 1, "c": [{"d": 90}, {"d": 85}]}]}
    ]
    spec = {"$inc": {"a.$[elem].c.$[deep].d": 1000}}
    array_filters = [{"elem.b": {"$gt": 0}},
                     {"deep.d": {"$lt": 90}}]

    monty_c = monty_update(docs, spec, array_filters=array_filters)
    mongo_c = mongo_update(docs, spec, array_filters=array_filters)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 0, "c": [{"d": 80}, {"d": 95}]},
                                   {"b": 1, "c": [{"d": 90}, {"d": 1085}]}]}


def test_update_positional_filtered_has_conflict_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 4, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$[elem].b": 1, "a.$[conflict].b": 2}}
    array_filters = [{"elem.c": {"$gt": 0}},  # update element 0
                     {"conflict": {"b": 4, "c": 1}}]  # update element 0, too.

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_positional_filtered_has_conflict_2(monty_update, mongo_update):
    docs = [
        {"a": [{"c": 5, "b": 1}, {"c": 8, "b": 0}]}
    ]
    spec = {"$set": {"a.$[elem]": 5, "a.$[conflict]": 5}}
    array_filters = [{"elem.c": 5},  # update element 0
                     {"conflict.b": 1}]  # update element 0, too.

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, array_filters=array_filters)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, array_filters=array_filters)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_id(monty_update, mongo_update):
    docs = [
        {"a": 6},
    ]
    spec = {"$inc": {"_id": 3, "b": 9}}
    find = {"a": 6}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find)

    assert mongo_err.value.code == 66
    assert mongo_err.value.code == monty_err.value.code


def test_update_id2(monty_update, mongo_update):
    docs = [
        {"a": {"b": {"_id": 0}}},
    ]
    spec = {"$inc": {"a.b._id": 3}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"b": {"_id": 3}}}


def test_update_id3(monty_update, mongo_update):
    docs = []
    spec = {"$set": {"_id": "some-id", "foo": "baby"}}
    find = {"_id": "some-id"}

    monty_c = monty_update(docs, spec, find, upsert=True)
    mongo_c = mongo_update(docs, spec, find, upsert=True)

    assert next(mongo_c) == next(monty_c)


def test_update_id4(monty_update, mongo_update):
    docs = []
    spec = {"$set": {"_id": "some-id", "foo": "baby"}}
    find = {"_id": "other-id"}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find, upsert=True)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find, upsert=True)

    assert mongo_err.value.code == 66
    assert mongo_err.value.code == monty_err.value.code


def test_update_id5(monty_update, mongo_update):
    docs = [{"_id": "other-id", "foo": "bar"}]
    spec = {"$set": {"_id": "some-id", "foo": "baby"}}
    find = {"foo": "bar"}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find, upsert=True)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find, upsert=True)

    assert mongo_err.value.code == 66
    assert mongo_err.value.code == monty_err.value.code


def test_update_id6(monty_update, mongo_update):
    docs = [{"_id": "some-id", "foo": "bar"}]
    spec = {"$set": {"_id": "some-id", "foo": "baby"}}
    find = {"foo": "bar"}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find, upsert=True)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find, upsert=True)

    assert mongo_err.value.code == 66
    assert mongo_err.value.code == monty_err.value.code


def test_update_positional(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$.b": 1}}
    find = {"a.b": 4}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": 3, "c": 1}, {"b": 5, "c": 0}]}


def test_update_positional_without_query_condition(monty_update, mongo_update):
    docs = [
        {"a": [{"b": 3, "c": 1}, {"b": 4, "c": 0}]}
    ]
    spec = {"$inc": {"a.$.b": 1}}
    find = {}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_array_faild_1(monty_update, mongo_update):
    docs = [
        {"a": [{"1": 4}]}
    ]
    spec = {"$inc": {"a.1.$[]": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_array_faild_2(monty_update, mongo_update):
    docs = [
        {"a": [{"1": 4}, {"1": 5}]}
    ]
    spec = {"$inc": {"a.1.$[]": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_array_faild_3(monty_update, mongo_update):
    docs = [
        {"a": {"1": 4}}
    ]
    spec = {"$inc": {"a.1.$[]": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_with_dollar_prefixed_field_1(monty_update, mongo_update):
    docs = [
        {"a": {"1": 4}}
    ]
    spec = {"$inc": {"a.$a": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_with_dollar_prefixed_field_2(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$set": {"b": {"$ey": [5]}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_with_dollar_prefixed_field_3(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$set": {"a": {"b": [{"$a": 5}]}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_with_dot_prefixed_nest_field(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$set": {"b": {".ey": [5]}}}  # This will pass

    mongo_update(docs, spec)
    monty_update(docs, spec)


def test_update_complex_position_1(monty_update, mongo_update):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]}
    ]
    spec = {"$inc": {"a.$[].$": 10}}
    find = {"a.b.1": {"$gt": 5}}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": [1, 5], "2": 10},
                                   {"b": [2, 4], "2": 10},
                                   {"b": [3, 6], "2": 10}]}


def test_update_complex_position_1_1(monty_update, mongo_update):
    docs = [
        {"a": {"1": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]}}
    ]
    spec = {"$inc": {"a.1.$[].$": 10}}
    find = {"a.1.b.1": {"$gt": 5}}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": {"1": [{"b": [1, 5], "2": 10},
                                         {"b": [2, 4], "2": 10},
                                         {"b": [3, 6], "2": 10}]}}


def test_update_complex_position_2(monty_update, mongo_update):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]}
    ]
    spec = {"$inc": {"a.$[].b.$": 10}}
    find = {"a.b.1": {"$eq": 6}}

    monty_c = monty_update(docs, spec, find)
    mongo_c = mongo_update(docs, spec, find)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [{"b": [1, 5, 10]},
                                   {"b": [2, 4, 10]},
                                   {"b": [3, 6, 10]}]}


def test_update_complex_position_3(monty_update, mongo_update):
    docs = [
        {"a": [{"b": [1, 5]}, {"b": [2, 4]}, {"b": [3, 6]}]}
    ]
    spec = {"$inc": {"a.$[].$[]": 10}}
    find = {"a.b.1": {"$gt": 5}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec, find)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec, find)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_empty_array(monty_update, mongo_update):
    docs = [
        {"a": []}
    ]
    spec = {"$inc": {"a.0": 10}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    assert next(mongo_c) == next(monty_c)
    monty_c.rewind()
    assert next(monty_c) == {"a": [10]}


def test_update_with_bad_spec(monty_update, mongo_update):
    docs = [
        {"a": 1}
    ]
    spec = {"$inc": 5}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_rename_conflict_1(monty_update, mongo_update):
    docs = [
        {"a": {"b": "some"}, "c": {"d": []}}
    ]
    spec = {"$set": {"c.d": {}}, "$rename": {"a.b": "c.d.b"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_rename_conflict_2(monty_update, mongo_update):
    docs = [
        {"a": {"b": "some"}, "d": {"e": 5}}
    ]
    spec = {"$rename": {"a.b": "c.b.f", "d.e": "c.b"}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_with_empty_field_1(monty_update, mongo_update):
    docs = [
        {"": 1}
    ]
    spec = {"$inc": {"": 5}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_with_empty_field_2(monty_update, mongo_update):
    docs = [
        {"": 1}
    ]
    spec = {"$inc": {".": 5}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
