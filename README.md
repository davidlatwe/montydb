
# Monty, Mongo Tinified
### A serverless Mongo-like database backed with SQLite in Python

**Not ready for prime time**

Inspired by [TinyDB](https://github.com/msiemens/tinydb) and the extension [TinyMongo](https://github.com/schapman1974/tinymongo).

### MontyDB is
* A serverless version of MongoDB *(trying to be)*
* Backed with SQLite *(default storage)*
* Using Mongo query language
* Working on both Python 2 and 3

### Example Code
```python
>>> from montydb import MontyClient
>>> client = MontyClient("/path/to/db/dir")
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
The query results are the same as PyMongo.
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
