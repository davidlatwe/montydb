
import pytest

import pymongo
import montydb


MongoOperationFailure = pymongo.errors.OperationFailure
MontyOperationFailure = montydb.errors.OperationFailure


def test_filtering(test_cases,
                   monty_collection_stuffed,
                   mongo_collection_stuffed):
    for name, case in test_cases.items():
        for i_, filter_ in enumerate(case["filters"]):
            mongo_cursor = mongo_collection_stuffed.find(filter_)
            monty_cursor = monty_collection_stuffed.find(filter_)

            mongo_res = []
            monty_res = []

            case_addr = "{} - filters.json in line: {}".format(name, (i_ + 1))

            try:
                for res in mongo_cursor:
                    mongo_res.append(res)
                for res in monty_cursor:
                    monty_res.append(res)
            except Exception as e:
                msg = """
                Should NOT raising any Error here...
                Faild case: {}
                {}: {}
                """.format(case_addr, str(e.__class__), str(e))
                assert False, msg

            msg = """
            Faild case: {}
            Mongo and Monty Cursor data count not match.
            """.format(case_addr)
            assert len(mongo_res) == len(monty_res), msg

            for i, res in enumerate(mongo_res):
                msg = """
                Faild case: {}
                Mongo and Monty documents order or content not match.
                """.format(case_addr)
                assert res == monty_res[i], msg
