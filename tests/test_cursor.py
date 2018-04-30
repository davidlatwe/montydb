
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
            case_addr = "{} - filters.json in line: {}".format(name, (i_ + 1))

            mongo_res = []
            mongo_err = ""
            mongo_err_cls = ""
            mongo_cursor = mongo_collection_stuffed.find(filter_)
            try:
                for res in mongo_cursor:
                    mongo_res.append(res)
            except Exception as e:
                mongo_err = e.message if hasattr(e, 'message') else str(e)
                mongo_err_cls = str(e.__class__)

            monty_res = []
            monty_err = ""
            monty_err_cls = ""
            monty_cursor = monty_collection_stuffed.find(filter_)
            try:
                for res in monty_cursor:
                    monty_res.append(res)
            except Exception as e:
                monty_err = e.message if hasattr(e, 'message') else str(e)
                monty_err_cls = str(e.__class__)

            msg = """
            Mongo and Monty Error not match.
            Mongo: {}: {}
            Monty: {}: {}
            Faild case: {}
            Filter: {}
            """.format(mongo_err_cls,
                       mongo_err,
                       monty_err_cls,
                       monty_err,
                       case_addr,
                       filter_)
            assert monty_err == mongo_err, msg

            msg = """
            Mongo and Monty Cursor data count not match.
            Faild case: {}
            Filter: {}
            """.format(case_addr, filter_)
            assert len(mongo_res) == len(monty_res), msg

            for i, res in enumerate(mongo_res):
                msg = """
                Mongo and Monty documents order or content not match.
                Faild case: {}
                Filter: {}
                """.format(case_addr, filter_)
                assert res == monty_res[i], msg
