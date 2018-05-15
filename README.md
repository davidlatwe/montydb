
# Monty, Mongo Tinified
### A serverless Mongo-like database backed with SQLite in Python

[![Build Status](https://travis-ci.org/davidlatwe/MontyDB.svg?branch=master)](https://travis-ci.org/davidlatwe/MontyDB)
[![Coverage Status](https://coveralls.io/repos/github/davidlatwe/MontyDB/badge.svg)](https://coveralls.io/github/davidlatwe/MontyDB)

:construction: **Not Ready For Prime Time** :construction:

###### Inspired by [TinyDB](https://github.com/msiemens/tinydb) and the extension [TinyMongo](https://github.com/schapman1974/tinymongo).

---

### What MontyDB is ...
* A serverless version of MongoDB *(trying to be)*
* Backed with SQLite
* Using Mongo query language, against to `MongoDB 3.6`
* Support **Python 2.7, 3.4, 3.5, 3.6**

### Goal
* To be an alternative option for projects which using MongoDB.
* Switch in between without changing document operation code. (If common ops is all you need)
* Improve my personal skill :p

### Requirements
* `pip install pyyaml`
* `pip install pymongo` (for `bson`)

### Example Code
```python
>>> from montydb import MontyClient
>>> client = MontyClient("/path/to/db-dir")  # Or ":memory:" for InMemory mode
>>> col = client.db.test
>>> col.insert_one({"stock": "A", "qty": 5})
# <montydb.results.InsertOneResult object at 0x000001B3CE3D0A08>

>>> cur = col.find({"stock": "A", "qty": {"$gt": 4}})
>>> next(cur)
# {'_id': ObjectId('5ad34e537e8dd45d9c61a456'), 'stock': 'A', 'qty': 5}
```

### Status
- Most workable functions had tested with *MongoDB 3.6* to ensure the behave.
- Implemented basic `client` and `database` interface.
- On `collection` level, currently only able to perform `insert` and `find`.
- Basic query and projection ops has implemented.
- Cursor able to `sort`.
