
<img src="artwork/logo.png" alt="drawing" width="600"/>

[![Python package](https://github.com/davidlatwe/montydb/actions/workflows/python-package.yml/badge.svg)](https://github.com/davidlatwe/montydb/actions/workflows/python-package.yml)
[![Version](http://img.shields.io/pypi/v/montydb.svg?style=flat)](https://pypi.python.org/pypi/montydb)
[![PyPi downloads](https://img.shields.io/pypi/dm/montydb)](https://pypistats.org/packages/montydb)

> Monty, Mongo tinified. MongoDB implemented in Python!

_Inspired by [TinyDB](https://github.com/msiemens/tinydb) and it's extension [TinyMongo](https://github.com/schapman1974/tinymongo)_


## What is it?

A pure Python-implemented database that looks and works like [MongoDB](https://www.mongodb.com/).

```python
>>> from montydb import MontyClient

>>> col = MontyClient(":memory:").db.test
>>> col.insert_many( [{"stock": "A", "qty": 6}, {"stock": "A", "qty": 2}] )
>>> cur = col.find( {"stock": "A", "qty": {"$gt": 4}} )
>>> next(cur)
{'_id': ObjectId('5ad34e537e8dd45d9c61a456'), 'stock': 'A', 'qty': 6}
```

Most of the CRUD operators have been implemented. You can visit [issue #14](https://github.com/davidlatwe/montydb/issues/14) to see the full list.

This project is tested against:

- MongoDB: 3.6, 4.0, 4.2 (4.4 on the way💦)
- Python: 3.7, 3.8, 3.9, 3.10, 3.11


## Install

```sh
pip install montydb
```

- optional, to use *real* `bson` in operation (`pymongo` will be installed)
    _For minimum requirements, `montydb` ships with it's own fork of `ObjectId` in `montydb.types`, so you may ignore this option if `ObjectId` is all you need from `bson`_

    ```sh
    pip install montydb[bson]
    ```
- optional, to use lightning memory-mapped db as storage engine
    ```sh
    pip install montydb[lmdb]
    ```


## Storage

🦄 Available storage engines:

* in-memory
* flat-file
* sqlite
* lmdb (lightning memory-mapped db)

Depending on which one you use, you may have to configure the storage engine before you start.

> ⚠️
>
> The configuration process only required on repository creation or modification. And, one repository (the parent level of databases) can only assign one storage engine.

To configure a storage, see flat-file storage for example:

```python
from montydb import set_storage, MontyClient


set_storage(
    # general settings
    
    repository="/db/repo",  # dir path for database to live on disk, default is {cwd}
    storage="flatfile",     # storage name, default "flatfile"
    mongo_version="4.0",    # try matching behavior with this mongodb version
    use_bson=False,         # default None, and will import pymongo's bson if None or True

    # any other kwargs are storage engine settings.
    
    cache_modified=10,       # the only setting that flat-file have
)

# ready to go
```

Once that done, there should be a file named `monty.storage.cfg` saved in your db repository path. It would be `/db/repo` for the above examples.


## Configuration

Now let's moving on to each storage engine's config settings.

### 🌟 In-Memory
  
`memory` storage does not need nor have any configuration, nothing saved to disk.

```python
from montydb import MontyClient


client = MontyClient(":memory:")

# ready to go
```

### 🔰 Flat-File
  
`flatfile` is the default on-disk storage engine.

```python
from montydb import set_storage, MontyClient


set_storage("/db/repo", cache_modified=5)  # optional step
client = MontyClient("/db/repo")  # use current working dir if no path given

# ready to go
```

FlatFile config:

```ini
[flatfile]
cache_modified: 0  # how many document CRUD cached before flush to disk.
```

### 💎 SQLite
  
`sqlite` is NOT the default on-disk storage, need configuration first before getting client.

> Pre-existing sqlite storage file which saved by `montydb<=1.3.0` is not read/writeable after `montydb==2.0.0`.

```python
from montydb import set_storage, MontyClient


set_storage("/db/repo", storage="sqlite")  # required, to set sqlite as engine
client = MontyClient("/db/repo")

# ready to go
```

SQLite config:

```ini
[sqlite]
journal_mode = WAL
check_same_thread =   # Leave it empty as False, or any value will be True
```
Or,

```python
repo = "path_to/repo"
set_storage(
    repository=repo,
    storage="sqlite",
    use_bson=True,
    # sqlite pragma
    journal_mode="WAL",
    # sqlite connection option
    check_same_thread=False,
)
client = MontyClient(repo)
...
```

SQLite write concern:

```python
client = MontyClient("/db/repo",
                     synchronous=1,
                     automatic_index=False,
                     busy_timeout=5000)
```

### 🚀 LMDB (Lightning Memory-Mapped Database)

`lightning` is NOT the default on-disk storage, need configuration first before get client.

> Newly implemented.

```python
from montydb import set_storage, MontyClient


set_storage("/db/repo", storage="lightning")  # required, to set lightning as engine
client = MontyClient("/db/repo")

# ready to go
```

LMDB config:

```ini
[lightning]
map_size: 10485760  # Maximum size database may grow to.
```

## URI

Optionally, You could prefix the repository path with montydb URI scheme.

```python
client = MontyClient("montydb:///db/repo")
```

## Utilities

> Pymongo `bson` may required.

* #### `montyimport`

  Imports content from an Extended JSON file into a MontyCollection instance.
  The JSON file could be generated from `montyexport` or `mongoexport`.

  ```python
  from montydb import open_repo, utils
  

  with open_repo("foo/bar"):
      utils.montyimport("db", "col", "/path/dump.json")
  
  ```

* #### `montyexport`

  Produces a JSON export of data stored in a MontyCollection instance.
  The JSON file could be loaded by `montyimport` or `mongoimport`.

  ```python
  from montydb import open_repo, utils
  

  with open_repo("foo/bar"):
      utils.montyexport("db", "col", "/data/dump.json")
  
  ```

* #### `montyrestore`

  Loads a binary database dump into a MontyCollection instance.
  The BSON file could be generated from `montydump` or `mongodump`.

  ```python
  from montydb import open_repo, utils
  

  with open_repo("foo/bar"):
      utils.montyrestore("db", "col", "/path/dump.bson")
  
  ```

* ####  `montydump`

  Creates a binary export from a MontyCollection instance.
  The BSON file could be loaded by `montyrestore` or `mongorestore`.

  ```python
  from montydb import open_repo, utils
  

  with open_repo("foo/bar"):
      utils.montydump("db", "col", "/data/dump.bson")
  
  ```

* #### `MongoQueryRecorder`

  Record MongoDB query results in a period of time.
  *Requires to access database profiler.*

  This works via filtering the database profile data and reproduce the queries of `find` and `distinct` commands.

  ```python
  from pymongo import MongoClient
  from montydb.utils import MongoQueryRecorder
  
  client = MongoClient()
  recorder = MongoQueryRecorder(client["mydb"])
  recorder.start()
  
  # Make some queries or run the App...
  recorder.stop()
  recorder.extract()
  {<collection_1>: [<doc_1>, <doc_2>, ...], ...}
  
  ```

* ####  `MontyList`

  Experimental, a subclass of `list`, combined the common CRUD methods from Mongo's Collection and Cursor.

  ```python
  from montydb.utils import MontyList
  
  mtl = MontyList([1, 2, {"a": 1}, {"a": 5}, {"a": 8}])
  mtl.find({"a": {"$gt": 3}})
  MontyList([{'a': 5}, {'a': 8}])
  
  ```

## Development

montydb uses [Poetry](https://python-poetry.org/) to make it easy manage dependencies and set up the development environment. 

### Initial setup

After cloning the repository, you need to run the following commands to set up the development environment:

```bash
make install
```

This will create a virtual environment and download the required dependencies.

### updating dependencies

To keep dependencies updated after git operations such as local updates or merging changes into local dev branch

```bash
make update
```
### Makefile

A makefile is used to simplify common operations such as updating, testing, and deploying etc.

```bash
make or make help

install                        install all dependencies locally
update                         update project dependencies locally (run after git update)
ci                             Run all checks (codespell, lint, bandit, test)
test                           Run tests
lint                           Run linting with flake8
codespell                      Find typos with codespell
bandit                         Run static security analysis with bandit
build                          Build project using poetry
clean                          Clean project
```

### Run mongo docker image
Most of our tests compare montydb CRUD results against real mongodb instance, therefore we must have a running
mongodb before testing.

For example, if we want to test against mongo 4.4:
```shell
docker run --name monty-4.4 -p 30044:27017 -d mongo:4.4
```

### Tests
```shell
poetry run pytest --storage {storage engin name} --mongodb {mongo instance url} [--use-bson]
```
Example:
```shell
poetry run pytest --storage memory --mongodb localhost:30044 --use-bson
```

## Why did I make this?

Mainly for personal skill practicing and fun.

I work in the VFX industry and some of my production needs (mostly edge-case) requires to run in a limited environment (e.g. outsourced render farms), which may have problem to run or connect a MongoDB instance. And I found this project really helps.

---

<p align=center>
    <a href="https://jb.gg/OpenSource"><i>This project is supported by JetBrains</i></a>
</p>

<p align="center">
    <img src="artwork/icon.png" alt="drawing" width="100"/>
  &nbsp;&nbsp;
    <img src="artwork/jetbrains.png" alt="drawing" width="100"/>
</p>
