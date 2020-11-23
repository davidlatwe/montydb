
import pytest

from pymongo.errors import WriteError as mongo_write_err
from montydb.errors import WriteError as monty_write_err
from ...conftest import skip_if_no_bson


def test_update_currentDate_1(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$currentDate": {"a": True}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    mg_date = next(mongo_c)["a"]
    mt_date = next(monty_c)["a"]

    assert mg_date.date() == mt_date.date()
    assert mg_date.hour == mt_date.hour
    assert mg_date.minute == mt_date.minute


def test_update_currentDate_2(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$currentDate": {"a": False}}  # still set date

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    mg_date = next(mongo_c)["a"]
    mt_date = next(monty_c)["a"]

    assert mg_date.date() == mt_date.date()
    assert mg_date.hour == mt_date.hour
    assert mg_date.minute == mt_date.minute


def test_update_currentDate_3(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$currentDate": {"a": {"$type": "date"}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    mg_date = next(mongo_c)["a"]
    mt_date = next(monty_c)["a"]

    assert mg_date.date() == mt_date.date()
    assert mg_date.hour == mt_date.hour
    assert mg_date.minute == mt_date.minute


@skip_if_no_bson
def test_update_currentDate_4(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$currentDate": {"a": {"$type": "timestamp"}}}

    monty_c = monty_update(docs, spec)
    mongo_c = mongo_update(docs, spec)

    mg_tstamp = next(mongo_c)["a"]
    mt_tstamp = next(monty_c)["a"]

    assert mg_tstamp.time - mt_tstamp.time < 10
    assert mg_tstamp.inc == mt_tstamp.inc


def test_update_currentDate_5(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$currentDate": {"a": 1}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_currentDate_6(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$currentDate": {"a": {"not_op": True}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code


def test_update_currentDate_7(monty_update, mongo_update):
    docs = [
        {"a": None}
    ]
    spec = {"$currentDate": {"a": {"$type": "not date nor timestamp"}}}

    with pytest.raises(mongo_write_err) as mongo_err:
        mongo_update(docs, spec)

    with pytest.raises(monty_write_err) as monty_err:
        monty_update(docs, spec)

    # ignore comparing error code
    # assert mongo_err.value.code == monty_err.value.code
