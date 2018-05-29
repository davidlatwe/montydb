
# Monty, Mongo Tinified
### A serverless Mongo-like database backed with SQLite in Python

[![Build Status](http://img.shields.io/travis/davidlatwe/MontyDB/master.svg?style=flat)](https://travis-ci.org/davidlatwe/MontyDB)
[![Coverage Status](https://img.shields.io/coveralls/github/davidlatwe/MontyDB/master.svg?style=flat)](https://coveralls.io/github/davidlatwe/MontyDB?branch=master)
[![Version](http://img.shields.io/pypi/v/MontyDB.svg?style=flat)](https://pypi.python.org/pypi/MontyDB)
![Tested_with_MongoDB-v3.6.4](https://img.shields.io/badge/tested_with_MongoDB-v3.6.4-blue.svg)
[![Maintainability](https://api.codeclimate.com/v1/badges/1adb14266d05ef3c9b17/maintainability)](https://codeclimate.com/github/davidlatwe/MontyDB/maintainability)

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

### Install
`pip install montydb`

### Requirements
* `pyyaml`
* `jsonschema`
* `pymongo` (for `bson`)

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

### Storage Engine Configurations

To select or config a storage engine, we need to use `MontyConfigure` class. It will load a storage engine's default config and offer you a chance to tweak those settings then auto save a `conf.yaml` file in the database repository, except *Memory storage*.

The configuration is only needed when repository creation or settings modification, `MontyClient` will pick up `conf.yaml` if exists.

**Currently, one database repository can only assign single storage engine.**

  - **Memory**
  
  Memory storage does not need nor have any configuration, nothing saved to disk.
  
  ```python
  >>> from montydb import MontyClient
  >>> client = MontyClient(":memory:")  # Just use it.
  ```

  - **SQLite**
  
  SQLite is the default on-disk storage engine, you can skip the configuration if the default setting is okay.
  
  ```python
  >>> from montydb import MontyClient
  >>> client = MontyClient("/db/repo")  # Just use it.
  ```

  Here is the SQLite default settings, they are infact SQLite pragmas:

  ```yaml
  connection:
    journal_mode: WAL
  write_concern:
    synchronous: 1
    automatic_index: OFF
    busy_timeout: 5000
  ```

  If you are not happy with the default, use `MontyConfigure` before you get client.

  ```python
  >>> from montydb import MontyClient, MontyConfigure, storage
  >>> with MontyConfigure("/db/repo") as cf:  # Auto save config when exit
  ...     cf.load(storage.SQLiteConfig)       # Load sqlite config
  ...     cf.config.connection.journal_mode = "DELETE"
  ...     cf.config.write_concern.busy_timeout = 8000
  ...
  >>> client = MontyClient("/db/repo")  # Running tweaked sqlite storage now
  ```

  - **FlatFile**
  
  FlatFile storage is not default storage engine, you need to do repository configuration first before create client object.
  
  ```python
  >>> from montydb import MontyClient, MontyConfigure, storage
  >>> with MontyConfigure("/db/repo") as cf:  # Auto save config when exit
  ...     cf.load(storage.FlatFileConfig)     # Load flatfile config
  ...
  >>> client = MontyClient("/db/repo")  # Running on flatfile storage now
  ```

  Here is the FlatFile default setting:

  ```yaml
  connection:
    cache_modified: 0
  ```

  `cache_modified` is an integer of how many document CRUD cached before flush to disk.

  ```python
  >>> with MontyConfigure("/db/repo") as cf:  # Auto save config when exit
  ...     cf.load(storage.FlatFileConfig)     # Load flatfile config
  ...     cf.config.connection.cache_modified = 1000
  ...
  >>> client = MontyClient("/db/repo")  # Running tweaked flatfile storage now
  ```

  > NOTICE
  >
  > If you already load and save a storage config, next config load will be ignored and load the previous saved on-disk config instead. You have to delete `conf.yaml` manually if you want to change storage engine.
  > For example:

  ```python
  >>> with MontyConfigure("/db/repo") as cf:
  ...     cf.load(storage.FlatFileConfig)
  ...
  >>> with MontyConfigure("/db/repo") as cf:
  ...     cf.load(storage.SQLiteConfig)
  ...     st = cf.config.storage
  ...     assert st.engine == "FlatFileStorage"  # True
  ```
 
 > NOTICE
 > 
 > `MontyClient` will reload `conf.yaml` at next operation after `client.close()`

  ```python
  >>> from montydb import MontyClient, MontyConfigure, storage
  >>> with MontyConfigure("/db/repo") as cf:
  ...     cf.load(storage.FlatFileConfig)
  ...
  >>> # Create with flatfile default config
  >>> client = MontyClient("/db/repo")
  >>> col = client.my_db.my_col
  >>> # Write to disk immediately due to default 0 cache
  >>> col.insert_one({"doc": 0})
  >>> # Edit `conf.yaml` directly and change cache_modified to 3,
  >>> # or use `MontyConfigure` to do that.
  >>> # Close client when done.
  >>> client.close()
  >>> 
  >>> col.insert_one({"doc": 1})  # client auto re-open and reload config
  >>> col.insert_one({"doc": 2})
  >>> col.insert_one({"doc": 3})
  >>> col.insert_one({"doc": 4})  # flush !
  ```


**After storage engine configuration, you should feel like using MongoDB's Python driver, unless it's not implemented.**

### Develop Status
See [Projects' TODO](https://github.com/davidlatwe/MontyDB/projects/1)
