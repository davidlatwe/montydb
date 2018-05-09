
import pytest


def insert_and_find(db, docs, spec):
    db.drop_collection("test")
    col = db["test"]
    for i, d in enumerate(docs):
        d['_id'] = i
    col.insert_many(docs)
    return col.find(spec)


@pytest.fixture
def monty_find(monty_database):
    def _insert_and_find(docs, spec, db=monty_database):
        return insert_and_find(db, docs, spec)
    return _insert_and_find


@pytest.fixture
def mongo_find(mongo_database):
    def _insert_and_find(docs, spec, db=mongo_database):
        return insert_and_find(db, docs, spec)
    return _insert_and_find
