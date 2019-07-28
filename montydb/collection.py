import collections
from copy import deepcopy
from bson import ObjectId
from bson.py3compat import string_type

from .base import (
    BaseObject,
    validate_is_mapping,
    validate_ok_for_update,
    validate_ok_for_replace,
    validate_list_or_none,
    validate_boolean,
    command_coder,
)

from .cursor import MontyCursor
from .engine.core import FieldWalker, Weighted
from .engine.queries import QueryFilter
from .engine.update import Updator
from .engine.helpers import is_duckument_type, Counter

from .storage.errors import StorageDuplicateKeyError
from .errors import (
    DuplicateKeyError,
    BulkWriteError,
)

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

        self._storage = database.client._storage

        self._database = database
        self._name = name
        self._components = (database, self)

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

        if "_id" not in document:
            document["_id"] = ObjectId()

        try:
            result = self._storage.write_one(self, document)
        except StorageDuplicateKeyError:
            message = ("E11000 duplicate key error collection: %s index: "
                       "_id_ dup key: { : \"%s\" }" % (self.full_name,
                                                       str(document["_id"])))
            details = {"index": 0, "code": 11000, "errmsg": message}
            raise DuplicateKeyError(message, code=11000, details=details)

        return InsertOneResult(result)

    def insert_many(self,
                    documents,
                    ordered=True,
                    bypass_document_validation=False, *args, **kwargs):
        """
        """
        if not isinstance(documents, collections.Iterable) or not documents:
            raise TypeError("documents must be a non-empty list")

        if bypass_document_validation:
            pass

        def set_id(doc):
            if "_id" not in doc:
                doc["_id"] = ObjectId()
            # Keep _id in track for error message
            return doc["_id"]

        counter = Counter(iter(documents), job_on_each=set_id)

        try:
            result = self._storage.write_many(self, counter, ordered)
        except StorageDuplicateKeyError:
            message = ("E11000 duplicate key error collection: %s index: "
                       "_id_ dup key: { : \"%s\" }" % (self.full_name,
                                                       str(counter.data)))
            index = counter.count - 1
            result = {
                "writeErrors": [{"index": index,
                                 "code": 11000,
                                 "errmsg": message,
                                 "op": documents[index]}],
                "writeConcernErrors": [],
                "nInserted": index,
                "nUpserted": 0,
                "nMatched": 0,
                "nModified": 0,
                "nRemoved": 0,
                "upserted": [],
            }
            raise BulkWriteError(result)

        return InsertManyResult(result)

    def replace_one(self,
                    filter,
                    replacement,
                    upsert=False,
                    bypass_document_validation=False, *args, **kwargs):
        """
        """
        validate_is_mapping("filter", filter)
        validate_ok_for_replace(replacement)
        validate_boolean("upsert", upsert)

        filter, = command_coder(filter, codec_op=self._database.codec_options)

        if bypass_document_validation:
            pass

        raw_result = {"n": 0, "nModified": 0}
        # updator = Updator(replacement)
        try:
            fw = next(self._internal_scan_query(filter))
        except StopIteration:
            if upsert:
                if "_id" not in replacement:
                    replacement["_id"] = ObjectId()
                raw_result["upserted"] = replacement["_id"]
                raw_result["n"] = 1
                self._storage.write_one(self, replacement, check_keys=False)
        else:
            raw_result["n"] = 1
            if fw.doc != replacement:
                replacement["_id"] = fw.doc["_id"]
                self._storage.update_one(self, replacement)
                raw_result["nModified"] = 1

        return UpdateResult(raw_result)

    def _internal_scan_query(self, query_spec):
        """An interanl document generator for update"""
        queryfilter = QueryFilter(query_spec)
        documents = self._storage.query(MontyCursor(self), 0)
        first_matched = None
        for doc in documents:
            if queryfilter(doc):
                first_matched = queryfilter.fieldwalker
                break

        if first_matched:
            yield first_matched  # for try statement to test update or insert
            yield first_matched  # start update, yield again
            # continue iter documents(generator)
            for doc in documents:
                if queryfilter(doc):
                    yield queryfilter.fieldwalker

    def _internal_upsert(self, query_spec, updator, raw_result):
        """Internal document upsert"""
        doc_cls = self._database.codec_options.document_class

        def _remove_dollar_key(doc):
            if is_duckument_type(doc):
                new_doc = doc_cls()
                fields = list(doc.keys())
                for field in fields:
                    if field[:1] == "$" or "." in field:
                        continue
                    new_doc[field] = _remove_dollar_key(doc[field])
                return new_doc
            else:
                return doc

        document = _remove_dollar_key(deepcopy(query_spec))
        if "_id" not in document:
            document["_id"] = ObjectId()
        raw_result["upserted"] = document["_id"]
        raw_result["n"] = 1

        fieldwalker = FieldWalker(document)
        updator(fieldwalker, do_insert=True)
        self._storage.write_one(self, fieldwalker.doc)

    def update_one(self,
                   filter,
                   update,
                   upsert=False,
                   bypass_document_validation=False,
                   array_filters=None, *args, **kwargs):
        """
        """
        validate_is_mapping("filter", filter)
        validate_ok_for_update(update)
        validate_list_or_none('array_filters', array_filters)
        validate_boolean('upsert', upsert)

        filter, update = command_coder(filter, update,
                                       codec_op=self._database.codec_options)
        if array_filters:
            array_filters = command_coder(
                *array_filters, codec_op=self._database.codec_options)

        if bypass_document_validation:
            pass

        raw_result = {"n": 0, "nModified": 0}
        updator = Updator(update, array_filters)
        try:
            fw = next(self._internal_scan_query(filter))
        except StopIteration:
            if upsert:
                self._internal_upsert(filter, updator, raw_result)
        else:
            raw_result["n"] = 1
            if updator(fw):
                self._storage.update_one(self, fw.doc)
                raw_result["nModified"] = 1

        return UpdateResult(raw_result)

    def update_many(self,
                    filter,
                    update,
                    upsert=False,
                    bypass_document_validation=False,
                    array_filters=None, *args, **kwargs):
        """
        """
        validate_is_mapping("filter", filter)
        validate_ok_for_update(update)
        validate_list_or_none('array_filters', array_filters)
        validate_boolean('upsert', upsert)

        filter, update = command_coder(filter, update,
                                       codec_op=self._database.codec_options)
        if array_filters:
            array_filters = command_coder(
                *array_filters, codec_op=self._database.codec_options)

        if bypass_document_validation:
            pass

        raw_result = {"n": 0, "nModified": 0}
        updator = Updator(update, array_filters)
        scanner = self._internal_scan_query(filter)
        try:
            next(scanner)
        except StopIteration:
            if upsert:
                self._internal_upsert(filter, updator, raw_result)
        else:
            def update_docs():
                n, m = 0, 0
                for fieldwalker in scanner:
                    n += 1
                    if updator(fieldwalker):
                        m += 1
                        yield fieldwalker.doc
                raw_result["n"] = n
                raw_result["nModified"] = m

            self._storage.update_many(self, update_docs())

        return UpdateResult(raw_result)

    def delete_one(self, filter):
        raw_result = {"n": 0}

        queryfilter = QueryFilter(filter)
        storage = self._storage
        documents = storage.query(MontyCursor(self), 0)

        for doc in documents:
            if queryfilter(doc):
                storage.delete_one(self, doc["_id"])
                raw_result["n"] = 1
                break

        return DeleteResult(raw_result)

    def delete_many(self, filter):
        raw_result = {"n": 0}

        queryfilter = QueryFilter(filter)
        storage = self._storage
        documents = storage.query(MontyCursor(self), 0)

        doc_ids = set()
        for doc in documents:
            if queryfilter(doc):
                doc_ids.add(doc["_id"])
                raw_result["n"] += 1

        storage.delete_many(self, doc_ids)

        return DeleteResult(raw_result)

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

    def distinct(self, key, filter=None, **kwargs):
        """
        """
        if not isinstance(key, string_type):
            raise TypeError("key must be an "
                            "instance of %s" % (string_type.__name__,))

        result = list()

        def get_value(doc):
            fieldwalker = FieldWalker(doc)
            fieldvalues = fieldwalker.go(key).get().value
            res = list()
            for v in fieldvalues.values:
                weighted = Weighted(v)
                if weighted not in result:
                    res.append(weighted)
            return sorted(res)

        documents = self._storage.query(MontyCursor(self), 0)

        if filter:
            queryfilter = QueryFilter(filter)
            for doc in documents:
                if queryfilter(doc):
                    result += get_value(doc)
        else:
            for doc in documents:
                result += get_value(doc)

        return [weighted.value for weighted in result]

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
        self._database.drop_collection(self._name)

    def save(self, to_save, *args, **kwargs):
        # DEPRECATED
        if "_id" in to_save:
            self.replace_one({"_id": to_save["_id"]},
                             to_save,
                             upsert=True,
                             *args, **kwargs)
        else:
            self.insert_one(to_save, *args, **kwargs)

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
