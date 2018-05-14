
# Monty, Mongo Tinified
### A serverless Mongo-like database backed with SQLite in Python

[![Build Status](https://travis-ci.org/davidlatwe/MontyDB.svg?branch=master)](https://travis-ci.org/davidlatwe/MontyDB)
[![Coverage Status](https://coveralls.io/repos/github/davidlatwe/MontyDB/badge.svg)](https://coveralls.io/github/davidlatwe/MontyDB)

:construction: **Not Ready For Prime Time** :construction:

###### Inspired by [TinyDB](https://github.com/msiemens/tinydb) and the extension [TinyMongo](https://github.com/schapman1974/tinymongo).

---

### What MontyDB is ...
* A serverless version of MongoDB *(trying to be)*
* Backed with SQLite *(and `:memory:` storage)*
* Using Mongo query language, against to `MongoDB 3.6`
* Working on both Python 2 and 3

### Goal
* To be an alternative option for projects which using MongoDB
* Improve my personal skill

### Example Code
```python
>>> from montydb import MontyClient
>>> address = "/path/to/db/dir"  # Or ":memory:" for InMemory mode
>>> client = MontyClient(address)
>>> col = client.db.test
>>> col.insert_one({"stock": "A", "qty": 5})
<montydb.results.InsertOneResult object at 0x000001B3CE3D0A08>
>>> cur = col.find({"stock": {"$exists": 1}})
>>> next(cur)
{'_id': ObjectId('5ad34e537e8dd45d9c61a456'), 'stock': 'A', 'qty': 5}
```

### Requirements
* `pip install pyyaml`
* `pip install pymongo` (for `bson`)

### Status
- Most workable functions had tested with *MongoDB 3.6* to ensure the behave.
- Implemented basic `client` and `database` interface.
- On `collection` level, currently only able to perform `insert` and `find`.
- Basic query and projection ops has implemented.
- Cursor able to `sort`.
