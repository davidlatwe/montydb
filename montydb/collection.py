import collections

from .cursor import MontyCursor
from .base import BaseObject
from .results import (BulkWriteResult,
                      DeleteResult,
                      InsertOneResult,
                      InsertManyResult,
                      UpdateResult)


class MontyCollection(BaseObject):

    def __init__(self, database, name, create=False,
                 codec_options=None, write_concern=None, **kwargs):
        """
        """
        super(MontyCollection, self).__init__(
            codec_options or database.codec_options,
            write_concern or database.write_concern)

        self._database = database
        self._name = name

    def __repr__(self):
        return "MontyCollection({!r}, {!r})".format(self._database, self._name)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self._database == other.database and
                    self._name == other.name)
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __getattr__(self, name):
        if name.startswith('_'):
            full_name = ".".join((self._name, name))
            raise AttributeError(
                "MontyCollection has no attribute {0!r}. To access the {1}"
                " collection, use database[{1!r}].".format(name, full_name))
        return self.__getitem__(name)

    def __getitem__(self, key):
        return self._database.get_collection(".".join((self._name, key)))

    @property
    def full_name(self):
        """
        """
        return u".".join((self._database.name, self._name))

    @property
    def name(self):
        """
        """
        return self._name

    @property
    def database(self):
        """
        """
        return self._database

    def with_options(self, codec_options=None, write_concern=None):
        raise NotImplementedError("Not implemented.")

    def bulk_write(self, requests, ordered=True):
        raise NotImplementedError("Not implemented.")

    def insert_one(self,
                   document,
                   bypass_document_validation=False, *args, **kwargs):
        """
        """
        if bypass_document_validation:
            pass

        return InsertOneResult(
            self.database.client._storage.insert_one(
                self.database.name,
                self._name,
                self.write_concern,
                self.codec_options,
                document
            ))

    def insert_many(self,
                    documents,
                    ordered=True,
                    bypass_document_validation=False, *args, **kwargs):
        """
        """
        if bypass_document_validation:
            pass

        return InsertManyResult(
            self.database.client._storage.insert_many(
                self.database.name,
                self._name,
                self.write_concern,
                self.codec_options,
                documents,
                ordered
            ))

    def replace_one(self,
                    filter,
                    replacement,
                    upsert=False,
                    bypass_document_validation=False, *args, **kwargs):
        """
        """
        if bypass_document_validation:
            pass

        return UpdateResult(
            self.database.client._storage._update_retryable(
                self.database.name,
                self._name,
                self.write_concern,
                self.codec_options,
                filter,
                replacement,
                upsert
            ))

    def update_one(self,
                   filter,
                   update,
                   upsert=False,
                   bypass_document_validation=False,
                   array_filters=None, *args, **kwargs):
        """
        """
        if bypass_document_validation:
            pass

        return UpdateResult(
            self.database.client._storage.update(
                self.database.name,
                self._name,
                self.write_concern,
                self.codec_options,
                filter,
                update,
                upsert,
                check_keys=False,
                array_filters=array_filters,
            ))

    def update_many(self,
                    filter,
                    update,
                    upsert=False,
                    bypass_document_validation=False,
                    array_filters=None, *args, **kwargs):
        """
        """

        if bypass_document_validation:
            pass

        return UpdateResult(
            self.database.client._storage.update(
                self.database.name,
                self._name,
                self.write_concern,
                self.codec_options,
                filter,
                update,
                upsert,
                check_keys=False,
                multi=True,
                array_filters=array_filters,
            ))

    def delete_one(self, filter):
        pass

    def delete_many(self, filter):
        pass

    def aggregate(self, pipeline, session=None, **kwargs):
        # return CommandCursor
        raise NotImplementedError("Not implemented.")

    def aggregate_raw_batches(self, pipeline, **kwargs):
        # return RawBatchCursor
        raise NotImplementedError("Not implemented.")

    def watch(self,
              pipeline=None,
              full_document='default',
              resume_after=None,
              max_await_time_ms=None,
              batch_size=None,
              collation=None,
              session=None):
        # return a changeStreamCursor
        raise NotImplementedError("Not implemented.")

    def find(self, *args, **kwargs):
        # return a cursor
        return MontyCursor(self, *args, **kwargs)

    def find_raw_batches(self, *args, **kwargs):
        # return RawBatchCursor
        raise NotImplementedError("Not implemented.")

    def find_one(self, filter=None, *args, **kwargs):
        """
        """
        if (filter is not None and not
                isinstance(filter, collections.Mapping)):
            filter = {"_id": filter}

        cursor = self.find(filter, *args, **kwargs)
        for result in cursor.limit(-1):
            return result
        return None

    def find_one_and_delete(self, filter, projection=None, sort=None):
        raise NotImplementedError("Not implemented.")

    def find_one_and_replace(self,
                             filter,
                             replacement,
                             projection=None,
                             sort=None, *args, **kwargs):
        raise NotImplementedError("Not implemented.")

    def find_one_and_update(self,
                            filter,
                            update,
                            projection=None,
                            sort=None, *args, **kwargs):
        raise NotImplementedError("Not implemented.")

    def count(self, filter=None):
        raise NotImplementedError("Not implemented.")

    def distinct(self, key, filter=None):
        raise NotImplementedError("Not implemented.")

    def create_index(self, keys):
        raise NotImplementedError("Not implemented.")

    def create_indexes(self, indexes):
        raise NotImplementedError("Not implemented.")

    def drop_index(self, index_or_name):
        raise NotImplementedError("Not implemented.")

    def drop_indexes(self):
        raise NotImplementedError("Not implemented.")

    def reindex(self):
        raise NotImplementedError("Not implemented.")

    def list_indexes(self):
        # return a commandCursor
        raise NotImplementedError("Not implemented.")

    def index_information(self):
        raise NotImplementedError("Not implemented.")

    def drop(self):
        raise NotImplementedError("Not implemented.")

    def rename(self, new_name, session=None, **kwargs):
        raise NotImplementedError("Not implemented.")

    def options(self, session=None):
        raise NotImplementedError("Not implemented.")

    def map_reduce(self,
                   map,
                   reduce,
                   out,
                   full_response=False,
                   session=None, **kwargs):
        raise NotImplementedError("Not implemented.")

    def inline_map_reduce(self,
                          map,
                          reduce,
                          full_response=False,
                          session=None, **kwargs):
        raise NotImplementedError("Not implemented.")

    def parallel_scan(self, num_cursors, session=None, **kwargs):
        raise NotImplementedError("Not implemented.")
