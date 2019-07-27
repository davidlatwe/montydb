
<img src="https://raw.githubusercontent.com/davidlatwe/MontyDB/master/artwork/logo.png" alt="drawing" width="600"/>

#### Monty, Mongo tinified. A literally serverless, Mongo-like database in Python

[![Build Status](https://travis-ci.org/davidlatwe/MontyDB.svg?branch=master)](https://travis-ci.org/davidlatwe/MontyDB)
<a href='https://coveralls.io/github/davidlatwe/MontyDB?branch=master'><img src='https://coveralls.io/repos/github/davidlatwe/MontyDB/badge.svg?branch=master&kill_cache=1' alt='Coverage Status' /></a>
[![Version](http://img.shields.io/pypi/v/MontyDB.svg?style=flat)](https://pypi.python.org/pypi/MontyDB)
[![Maintainability](https://api.codeclimate.com/v1/badges/1adb14266d05ef3c9b17/maintainability)](https://codeclimate.com/github/davidlatwe/MontyDB/maintainability)

:construction: **Not Ready For Prime Time** :construction:

###### Inspired by [TinyDB](https://github.com/msiemens/tinydb) and the extension [TinyMongo](https://github.com/schapman1974/tinymongo).

---

### MontyDB is:
* A serverless version of MongoDB, against to **MongoDB 3.6.4**
* Document oriented, of course
* Storage engine pluggable
* Write in pure Python, works on **Python 2.7, 3.4, 3.5, 3.6**

### Install
`pip install montydb`

  ##### Requirements
  - *`pymongo` (for `bson`)*

### Example Code
```python
>>> from montydb import MontyClient
>>> col = MontyClient(":memory:").db.test
>>> col.insert_many([{"stock": "A", "qty": 6}, {"stock": "A", "qty": 2}])

>>> cur = col.find({"stock": "A", "qty": {"$gt": 4}})
>>> next(cur)
{'_id': ObjectId('5ad34e537e8dd45d9c61a456'), 'stock': 'A', 'qty': 6}
```

### Develop Status
See [Projects' TODO](https://github.com/davidlatwe/MontyDB/projects/1)

### Storage Engine Configurations

The configuration process only required on repository creation or modification.

**Currently, one repository can only assign one storage engine.**

  - **Memory**
  
  Memory storage does not need nor have any configuration, nothing saved to disk.
  
  ```python
  >>> from montydb import MontyClient
  >>> client = MontyClient(":memory:")
  ```

  - **FlatFile**
  
  FlatFile is the default on-disk storage engine.
  
  ```python
  >>> from montydb import MontyClient
  >>> client = MontyClient("/db/repo")
  ```

  FlatFile config:

  ```yaml
  [flatfile]
  cache_modified: 0  # how many document CRUD cached before flush to disk.
  ```

  - **SQLite**
  
  SQLite is NOT the default on-disk storage, need configuration first before get client.
  
  ```python
  >>> from montydb import set_storage, MontyClient
  >>> set_storage("/db/repo", storage="sqlite")
  >>> client = MontyClient("/db/repo")
  ```

  SQLite config:

  ```yaml
  [sqlite]
  journal_mode: WAL
  ```

  SQLite write concern:

  ```python
  >>> client = MontyClient("/db/repo",
  >>>                      synchronous=1,
  >>>                      automatic_index=False,
  >>>                      busy_timeout=5000)
  ```

### Utilities

* #### `monty_dump`

  Write documents to disk, able to load by `monty_load` or `mongoimport`
  ```python
  >>> from montydb.utils import monty_dump
  >>> documents = [{"a": 1}, {"doc": "some doc"}]
  >>> monty_dump("/path/dump.json", documents)
  ```

* ####  `monty_load`

  Read documents from disk, able to read from `monty_dump` or `mongoexport`
  ```python
  >>> from montydb.utils import monty_load
  >>> monty_load("/path/dump.json")
  [{"a": 1}, {"doc": "some doc"}]
  ```

* ####  `MontyList`

  Experimental, a subclass of `list`, combined the common CRUD methods from Mongo's Collection and Cursor.

  ```python
  >>> from montydb.utils import MontyList
  >>> mtl = MontyList([1, 2, {"a": 1}, {"a": 5}, {"a": 8}])
  >>> mtl.find({"a": {"$gt": 3}})
  MontyList([{'a': 5}, {'a': 8}])
  ```
  You can dump it with `monty_dump` or read from `monty_load`
  ```python
  >>> monty_dump("/path/dump.json", mtl)
  >>> MontyList(monty_load("/path/dump.json"))
  MontyList([1, 2, {'a': 1}, {'a': 5}, {'a': 8}])
  ```

---

<p align="center">
  <img src="https://raw.githubusercontent.com/davidlatwe/MontyDB/master/artwork/icon.png" alt="drawing" width="60"/>
</p>
