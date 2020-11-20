
import pytest

from montydb.errors import OperationFailure
from montydb.cursor import MontyCursor


@pytest.fixture
def monty_collection(monty_database):
    monty_database.drop_collection("for_cursor_test")
    col = monty_database.for_cursor_test
    col.insert_many([{"doc": i} for i in range(20)])
    return col


def test_cursor_address(monty_collection):
    cur = monty_collection.find({})
    assert cur.address == monty_collection.database.client.address


def test_cursor_collection(monty_collection):
    cur = monty_collection.find({})
    assert cur.collection == monty_collection


def test_cursor_filter_type_error(monty_collection):
    with pytest.raises(TypeError):
        monty_collection.find([])


def test_cursor_skip(monty_collection):
    cur = monty_collection.find({}, skip=5)
    assert next(cur)["doc"] == 5


def test_cursor_skip_not_int_1(monty_collection):
    with pytest.raises(TypeError):
        monty_collection.find({}, skip="5")


def test_cursor_skip_not_int_2(monty_collection):
    with pytest.raises(TypeError):
        monty_collection.find({}).skip("5")


def test_cursor_skip_neg_int_1(monty_collection):
    cur = monty_collection.find({}, skip=-5)
    with pytest.raises(OperationFailure):
        next(cur)


def test_cursor_skip_neg_int_2(monty_collection):
    with pytest.raises(ValueError):
        monty_collection.find({}).skip(-5)


def test_cursor_limit(monty_collection):
    assert monty_collection.count_documents({}, limit=5) == 5


def test_cursor_limit_not_int(monty_collection):
    with pytest.raises(TypeError):
        monty_collection.find({}, limit="5")


def test_cursor_cursor_type_value_err(monty_collection):
    with pytest.raises(ValueError):
        monty_collection.find({}, cursor_type=1)


def test_cursor_projection_is_empty_dict(monty_collection):
    cur = monty_collection.find({"doc": 0}, projection={})
    doc = next(cur)
    assert len(doc) == 1 and "_id" in doc


def test_cursor_get_item_with_int(monty_collection):
    cur = monty_collection.find({})
    assert cur[0]["doc"] == 0


def test_cursor_get_item_with_neg_int(monty_collection):
    cur = monty_collection.find({})
    with pytest.raises(IndexError):
        cur[-1]


def test_cursor_get_item_with_slice(monty_collection):
    cur = monty_collection.find({})
    cur = cur[1:5]
    assert len(list(cur)) == 4
    for i, doc in enumerate(cur, 1):
        assert doc["doc"] == i


def test_cursor_get_item_with_slice_and_step(monty_collection):
    cur = monty_collection.find({})
    with pytest.raises(IndexError):
        cur[1:5:2]


def test_cursor_get_item_with_slice_and_neg_start(monty_collection):
    cur = monty_collection.find({})
    with pytest.raises(IndexError):
        cur[-1:5]


def test_cursor_get_item_with_slice_and_stop_lt_start(monty_collection):
    cur = monty_collection.find({})
    with pytest.raises(IndexError):
        cur[5:1]


def test_cursor_get_item_with_slice_and_stop_eq_start(monty_collection):
    cur = monty_collection.find({})
    cur = cur[1:1]
    with pytest.raises(StopIteration):
        next(cur)


def test_cursor_get_item_with_slice_none(monty_collection):
    cur = monty_collection.find({})
    cur = cur[None:None]
    assert len(list(cur)) == 20


def test_cursor_get_item_but_out_of_range(monty_collection):
    cur = monty_collection.find({})
    with pytest.raises(IndexError):
        cur[100]


def test_cursor_get_item_with_type_error(monty_collection):
    cur = monty_collection.find({})
    with pytest.raises(TypeError):
        cur["not_num"]


def test_cursor_context(monty_collection):
    with monty_collection.find({}) as cur:
        assert isinstance(cur, MontyCursor)


def test_cursor_rewind(monty_collection):
    with monty_collection.find({}) as cur:
        assert next(cur)["doc"] == 0
        assert next(cur)["doc"] == 1
        cur.rewind()
        assert next(cur)["doc"] == 0
