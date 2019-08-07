
<img src="artwork/logo.png" alt="drawing" width="600"/>

#### Monty, Mongo tinified. MongoDB implementated in Python !

[![Build Status](https://travis-ci.org/davidlatwe/MontyDB.svg?branch=master)](https://travis-ci.org/davidlatwe/MontyDB)
<a href='https://coveralls.io/github/davidlatwe/MontyDB?branch=master'><img src='https://coveralls.io/repos/github/davidlatwe/MontyDB/badge.svg?branch=master&kill_cache=1' alt='Coverage Status' /></a>
[![Version](http://img.shields.io/pypi/v/MontyDB.svg?style=flat)](https://pypi.python.org/pypi/MontyDB)
[![Maintainability](https://api.codeclimate.com/v1/badges/1adb14266d05ef3c9b17/maintainability)](https://codeclimate.com/github/davidlatwe/MontyDB/maintainability)

###### Inspired by [TinyDB](https://github.com/msiemens/tinydb) and it's extension [TinyMongo](https://github.com/schapman1974/tinymongo).

---

### MontyDB is:
* A tiny version of MongoDB, against to **MongoDB 3.6.4** *(4.0 soon)*
* Written in pure Python, testing on **Python 2.7, 3.6, 3.7, PyPy, PyPy3.5**
* Literally serverless.
* Similar to [mongomock](https://github.com/mongomock/mongomock), but a bit more than that.

> All those implemented functions and operators, should behaved just like you were working with MongoDB. Even raising error for same cause.

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

### Development
* Adopting [Gitflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) branching model.
* Adopting Test-driven development.
* You may visit [Projects' TODO](https://github.com/davidlatwe/montydb/projects/1) to see what's going on.
* You may visit [This Issue](https://github.com/davidlatwe/montydb/issues/14) to see what's been implemented and what's not.


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

### MontyDB URI

You could prefix the repository path with montydb URI scheme.

```python
  >>> client = MontyClient("montydb:///db/repo")
```

### Utilities

* #### `montyimport`

  Imports content from an Extended JSON file into a MontyCollection instance.
  The JSON file could be generated from `montyexport` or `mongoexport`.

  ```python
  >>> from montydb import open_repo, utils
  >>> with open_repo("foo/bar"):
  >>>     utils.montyimport("db", "col", "/path/dump.json")
  >>>
  ```

* ####  `montyexport`

  Produces a JSON export of data stored in a MontyCollection instance.
  The JSON file could be loaded by `montyimport` or `mongoimport`.

  ```python
  >>> from montydb import open_repo, utils
  >>> with open_repo("foo/bar"):
  >>>     utils.montyexport("db", "col", "/data/dump.json")
  >>>
  ```

* #### `montyrestore`

  Loads a binary database dump into a MontyCollection instance.
  The BSON file could be generated from `montydump` or `mongodump`.

  ```python
  >>> from montydb import open_repo, utils
  >>> with open_repo("foo/bar"):
  >>>     utils.montyrestore("db", "col", "/path/dump.bson")
  >>>
  ```

* ####  `montydump`

  Creates a binary export from a MontyCollection instance.
  The BSON file could be loaded by `montyrestore` or `mongorestore`.

  ```python
  >>> from montydb import open_repo, utils
  >>> with open_repo("foo/bar"):
  >>>     utils.montydump("db", "col", "/data/dump.bson")
  >>>
  ```

* #### `MongoQueryRecorder`

  Record MongoDB query results in a period of time.
  *Requires to access databse profiler.*

  This works via filtering the database profile data and reproduce the queries of `find` and `distinct` commands.

  ```python
  >>> from pymongo import MongoClient
  >>> from montydb.utils import MongoQueryRecorder
  >>> client = MongoClient()
  >>> recorder = MongoQueryRecorder(client["mydb"])
  >>> recorder.start()
  >>> # Make some queries or run the App...
  >>> recorder.stop()
  >>> recorder.extract()
  {<collection_1>: [<doc_1>, <doc_2>, ...], ...}
  ```

* ####  `MontyList`

  Experimental, a subclass of `list`, combined the common CRUD methods from Mongo's Collection and Cursor.

  ```python
  >>> from montydb.utils import MontyList
  >>> mtl = MontyList([1, 2, {"a": 1}, {"a": 5}, {"a": 8}])
  >>> mtl.find({"a": {"$gt": 3}})
  MontyList([{'a': 5}, {'a': 8}])
  ```

### Why I did this ?

Mainly for personal skill practicing and fun.
I work in VFX industry, some of my production needs (mostly edge-case) requires to run in a limited environment (e.g. outsourced render farms), which may have problem to run or connect a MongoDB instance. And I found this project really helps.


---

<p align="center">
  <img src="artwork/icon.png" alt="drawing" width="60"/>
</p>
