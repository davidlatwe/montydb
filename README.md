
# Monty, Mongo Tinified
### A serverless Mongo-like database backed with SQLite in Python

[![Build Status](https://travis-ci.org/davidlatwe/MontyDB.svg?branch=master)](https://travis-ci.org/davidlatwe/MontyDB)
[![Coverage Status](https://coveralls.io/repos/github/davidlatwe/MontyDB/badge.svg?branch=master)](https://coveralls.io/github/davidlatwe/MontyDB?branch=master)
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
  - *`pyyaml`*
  - *`jsonschema`*
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

### Storage Engine Configurations

The configuration process only required on repository creation or modification.

**Currently, one repository can only assign one storage engine.**

  - **Memory**
  
  Memory storage does not need nor have any configuration, nothing saved to disk.
  
  ```python
  >>> client = MontyClient(":memory:")
  ```

  - **SQLite**
  
  SQLite is the default on-disk storage engine, you can skip the configuration if the default setting is okay.
  
  ```python
  >>> client = MontyClient("/db/repo")
  ```

  SQLite default settings, they are infact SQLite pragmas:

  ```yaml
  connection:
    journal_mode: WAL
  write_concern:
    synchronous: 1
    automatic_index: OFF
    busy_timeout: 5000
  ```

  If you are not happy with the default, use `MontyConfigure` before get client.

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
  
  Not default storage engine, need configuration first before get client.
  
  ```python
  >>> from montydb import MontyClient, MontyConfigure, storage
  >>> with MontyConfigure("/db/repo") as cf:  # Auto save config when exit
  ...     cf.load(storage.FlatFileConfig)     # Load flatfile config
  ...
  >>> client = MontyClient("/db/repo")  # Running on flatfile storage now
  ```

   FlatFile default settings:

  ```yaml
  connection:
    cache_modified: 0  # how many document CRUD cached before flush to disk.
  ```

  #### Change storage engine

  `MontyConfigure` will ignore `load()` if `conf.yaml` exists, you need to `drop()` first before changing the storage engine. The documents will remain on disk and `conf.yaml` will be deleted.

  ```python
   >>> with MontyConfigure("/db/repo") as cf:
  ...     cf.drop()
  ...     cf.load(storage.WhateverConfig)
  ```

  #### Reload configuration

   `MontyClient` will reload `conf.yaml` at the operation right after `client.close()`.

  ```python
  >>> client.close()
  >>> col.insert_one({"doc": 1})  # client auto re-open and reload config
  ```


**After storage engine configuration, you should feel like using MongoDB's Python driver, unless it's not implemented.**

### Develop Status
See [Projects' TODO](https://github.com/davidlatwe/MontyDB/projects/1)
