import pytest
from pymongo.results import BulkWriteResult as PyMongoBulkWriteResult

from montydb.results import BulkWriteResult


@pytest.mark.parametrize("bulk_api_result", [
    {},
    {
        "nUpserted": 2,
        "upserted": [{"index": 0, "_id": "a"}, {"index": 3, "_id": "b"}],
    },
])
def test_bulk_write_result_upserted_ids(bulk_api_result):
    monty_result = BulkWriteResult(bulk_api_result)
    mongo_result = PyMongoBulkWriteResult(bulk_api_result, True)

    assert monty_result.upserted_ids == mongo_result.upserted_ids


def test_bulk_write_result_upserted_ids_empty_is_none():
    # pymongo returns None, not an empty dict, for an empty bulk API result
    assert BulkWriteResult({}).upserted_ids is None
