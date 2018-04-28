
# Monty, Mongo Tinified
### A serverless Mongo-like database backed with SQLite in Python

**Not ready for prime time**

Inspired by [TinyDB](https://github.com/msiemens/tinydb) and the extension [TinyMongo](https://github.com/schapman1974/tinymongo).

### MontyDB is
* A serverless version of MongoDB *(trying to be)*
* Backed with SQLite *(default storage)*
* Using Mongo query language
* Working on both Python 2 and 3
* Against to MongoDB 3.6

### Example Code
```python
>>> from montydb import MontyClient
>>> address = "/path/to/db/dir"  # Or ":memory:" for InMemory mode
>>> client = MontyClient(address)
>>> col = client.db.test
>>> col.insert_one({"warehouse": "A", "qty": 5})
<montydb.results.InsertOneResult object at 0x000001B3CE3D0A08>
>>> cur = col.find({"warehouse": {"$exists": 1}})
>>> next(cur)
{'_id': ObjectId('5ad34e537e8dd45d9c61a456'), 'warehouse': 'A', 'qty': 5}
```

## Query
Able to `find()` with filter and `sort()` result.
Support embedded document.
The query results are the same as using Mongo Python driver.
Currently implemented ops:
* $eq
* $ne
* $gt
* $gte
* $lt
* $lte
* $in
* $nin
* $all
* $elemMatch
* $size
* $exists
