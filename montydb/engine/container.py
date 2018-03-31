import os
import bson

from sqlite3 import Binary


"""class RepositoryEngine(object):

    def __init__(self, repository):
        if repository is None:
            repository = os.getcwd()
        # make
        if not os.path.isdir(repository):
            os.makedirs(repository)

        self.repository = repository
        self.opened = True
        self.conn = SQLazyConn()

    def database_path(self, database_name):
        return os.path.join(self.repository, database_name)

    def database_list(self):
        return [
            db_dir.decode(FS_ENC) for db_dir in os.listdir(self.repository)
            if os.path.isdir(self.database_path(db_dir))
        ]

    def drop_database(self, database_name):
        db_dir = self.database_path(database_name)
        if os.path.isdir(db_dir):
            shutil.rmtree(db_dir)
"""


class DatabaseEngine(object):

    COL_EXT = ".collection"
    COL_TBL = "documents"

    def __init__(self, repo_engine, db_name):
        self._db_path = repo_engine.database_path(db_name)

        self.repo_engine = repo_engine

    def database_exists(self):
        return os.path.isdir(self._db_path)

    def database_init(self):
        if not os.path.isdir(self._db_path):
            os.makedirs(self._db_path)

    def collection_path(self, collection_name):
        return os.path.join(self._db_path, collection_name) + self.COL_EXT

    def collection_exists(self, collection_name):
        return os.path.isfile(self.collection_path(collection_name))

    def create_collection(self, collection_name):
        """
        """
        if not self.database_exists():
            self.database_init()

        INIT_COLLECTION = u"""
            CREATE TABLE [{}](
                _id text NOT NULL,
                doc blob NOT NULL,
                PRIMARY KEY(_id)
            );
        """.format(self.COL_TBL)
        self.repo_engine.conn.write(
            self.collection_path(collection_name),
            INIT_COLLECTION)

    def collection_list(self):
        """
        Return a name list of collections.
        """
        if not self.database_exists():
            return []
        return [os.path.splitext(name)[0]
                for name in os.listdir(self._db_path)]

    def drop_collection(self, collection_name):
        """
        Drop all tables that related to the collection
        """
        if self.collection_exists(collection_name):
            os.remove(self.collection_path(collection_name))


class CollectionEngine(object):

    def __init__(self, db_engine, collection_name):
        self._collection_name = collection_name
        self._collection_path = db_engine.collection_path(collection_name)

        self.db_engine = db_engine

    def _whip(func):
        def init_if_not_exists(self, *args, **kwargs):
            if not self.db_engine.collection_exists(self._collection_name):
                self.db_engine.create_collection(self._collection_name)
            return func(self, *args, **kwargs)
        return init_if_not_exists

    def _exists(self):
        return self.db_engine.collection_exists(self._collection_name)

    def _bdoc(self, doc):
        return Binary(bson.BSON.encode(doc))

    def _ddoc(self, bdoc):
        return bson.BSON.decode(bson.BSON(bdoc))

    @_whip
    def insert(self, write_concern, doc, bypass_doc_val):
        if "_id" not in doc:
            doc["_id"] = bson.ObjectId()

        sql = u"""
            INSERT INTO [{}](_id, doc) VALUES (?, ?)
        """.format(self.db_engine.COL_TBL)
        self.db_engine.repo_engine.conn.write(
            self._collection_path,
            sql,
            (str(doc["_id"]), self._bdoc(doc)),
            write_concern
        )
        return doc["_id"]

    @_whip
    def insert_many(self, write_concern, docs, ordered, bypass_doc_val):
        for doc in docs:
            if "_id" not in doc:
                doc["_id"] = bson.ObjectId()

        sql = u"""
            INSERT INTO [{}](_id, doc) VALUES (?, ?)
        """.format(self.db_engine.COL_TBL)
        self.db_engine.repo_engine.conn.write_many(
            self._collection_path,
            sql,
            [(str(doc["_id"]), self._bdoc(doc)) for doc in docs],
            write_concern
        )
        return [doc["_id"] for doc in docs]

    def delete(self):
        pass
