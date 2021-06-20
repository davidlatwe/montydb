import warnings
from copy import deepcopy

from .base import (
    BaseObject,
    validate_is_mapping,
    validate_ok_for_update,
    validate_ok_for_replace,
    validate_list_or_none,
    validate_boolean,
)

from .cursor import MontyCursor
from .engine.field_walker import FieldWalker
from .engine.weighted import Weighted
from .engine.queries import QueryFilter
from .engine.update import Updator
from .types import (
    abc,
    bson,
    string_types,
    is_duckument_type,
    Counter,
    on_err_close,
)
from .storage import StorageDuplicateKeyError
from .errors import (
    DuplicateKeyError,
    BulkWriteError,
    WriteError,
)

from .results import (
    DeleteResult,
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
)


NotImplementeds = {
    "aggregate",
    "aggregate_raw_batches",
    "with_options",
    "bulk_write",
    "watch",
    "find_raw_batches",
    "find_one_and_delete",
    "find_one_and_replace",
    "find_one_and_update",
    "create_index",
    "create_indexes",
    "drop_index",
    "drop_indexes",
    "reindex",
    "list_indexes",
    "index_information",
    "rename",
    "options",
    "map_reduce",
    "inline_map_reduce",
    "parallel_scan",
}


class MontyCollection(BaseObject):
    def __init__(
        self,
        database,
        name,
        create=False,
        codec_options=None,
        write_concern=None,
        **kwargs
    ):
        """ """
        super(MontyCollection, self).__init__(
            codec_options or database.codec_options,
            write_concern or database.write_concern,
        )

        self._storage = database.client._storage

        self._database = database
        self._name = name
        self._components = (database, self)

    def __repr__(self):
        return "MontyCollection({!r}, {!r})".format(self._database, self._name)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._database == other.database and self._name == other.name
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __getattr__(self, name):
        if name in NotImplementeds:
            raise NotImplementedError(
                "'MontyCollection.%s' is NOT implemented !" % name
            )
        if name.startswith("_"):
            full_name = ".".join((self._name, name))
            raise AttributeError(
                "MontyCollection has no attribute {0!r}. To access the {1}"
                " collection, use database[{1!r}].".format(name, full_name)
            )
        return self.__getitem__(name)

    def __getitem__(self, key):
        return self._database.get_collection(".".join((self._name, key)))

    @property
    def full_name(self):
        """ """
        return u".".join((self._database.name, self._name))

    @property
    def name(self):
        """ """
        return self._name

    @property
    def database(self):
        """ """
        return self._database

    def insert_one(self, document, bypass_document_validation=False, *args, **kwargs):
        """ """
        if bypass_document_validation:
            pass

        if "_id" not in document:
            document["_id"] = bson.ObjectId()

        try:
            result = self._storage.write_one(self, document)
        except StorageDuplicateKeyError:
            message = (
                "E11000 duplicate key error collection: %s index: "
                '_id_ dup key: { : "%s" }' % (self.full_name, str(document["_id"]))
            )
            details = {"index": 0, "code": 11000, "errmsg": message}
            raise DuplicateKeyError(message, code=11000, details=details)

        return InsertOneResult(result)

    def insert_many(
        self, documents, ordered=True, bypass_document_validation=False, *args, **kwargs
    ):
        """ """
        if not isinstance(documents, abc.Iterable) or not documents:
            raise TypeError("documents must be a non-empty list")

        if bypass_document_validation:
            pass

        def set_id(doc):
            if "_id" not in doc:
                doc["_id"] = bson.ObjectId()
            # Keep _id in track for error message
            return doc["_id"]

        counter = Counter(iter(documents), job_on_each=set_id)

        try:
            result = self._storage.write_many(self, counter, ordered)
        except StorageDuplicateKeyError:
            message = (
                "E11000 duplicate key error collection: %s index: "
                '_id_ dup key: { : "%s" }' % (self.full_name, str(counter.data))
            )
            index = counter.count - 1
            result = {
                "writeErrors": [
                    {
                        "index": index,
                        "code": 11000,
                        "errmsg": message,
                        "op": documents[index],
                    }
                ],
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

    def replace_one(
        self,
        filter,
        replacement,
        upsert=False,
        bypass_document_validation=False,
        *args,
        **kwargs
    ):
        """ """
        validate_is_mapping("filter", filter)
        validate_ok_for_replace(replacement)
        validate_boolean("upsert", upsert)

        if bypass_document_validation:
            pass

        raw_result = {"n": 0, "nModified": 0}
        # updator = Updator(replacement)
        try:
            fw = next(self._internal_scan_query(filter))
        except StopIteration:
            if upsert:
                if "_id" not in replacement:
                    replacement["_id"] = bson.ObjectId()
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
        """An internal document generator for update"""
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
            document["_id"] = bson.ObjectId()
        raw_result["upserted"] = document["_id"]
        raw_result["n"] = 1

        fieldwalker = FieldWalker(document)
        updator(fieldwalker, do_insert=True)
        self._storage.write_one(self, fieldwalker.doc)

    def _no_id_update(self, updator, filter=None):
        id_operator = updator.operations.get("_id")
        doc_id = (filter or {}).get("_id")
        if id_operator and id_operator._keep() != doc_id:
            msg = (
                "Performing an update on the path '_id' would "
                "modify the immutable field '_id'"
            )
            raise WriteError(msg, code=66)

    def update_one(
        self,
        filter,
        update,
        upsert=False,
        bypass_document_validation=False,
        array_filters=None,
        *args,
        **kwargs
    ):
        """ """
        validate_is_mapping("filter", filter)
        validate_ok_for_update(update)
        validate_list_or_none("array_filters", array_filters)
        validate_boolean("upsert", upsert)

        if bypass_document_validation:
            pass

        raw_result = {"n": 0, "nModified": 0}
        updator = Updator(update, array_filters)
        self._no_id_update(updator, filter)
        try:
            fw = next(self._internal_scan_query(filter))
        except StopIteration:
            if upsert:
                self._internal_upsert(filter, updator, raw_result)
        else:
            self._no_id_update(updator)

            raw_result["n"] = 1
            if updator(fw):
                self._storage.update_one(self, fw.doc)
                raw_result["nModified"] = 1

        return UpdateResult(raw_result)

    def update_many(
        self,
        filter,
        update,
        upsert=False,
        bypass_document_validation=False,
        array_filters=None,
        *args,
        **kwargs
    ):
        """ """
        validate_is_mapping("filter", filter)
        validate_ok_for_update(update)
        validate_list_or_none("array_filters", array_filters)
        validate_boolean("upsert", upsert)

        if bypass_document_validation:
            pass

        raw_result = {"n": 0, "nModified": 0}
        updator = Updator(update, array_filters)
        scanner = self._internal_scan_query(filter)
        self._no_id_update(updator, filter)
        try:
            next(scanner)
        except StopIteration:
            if upsert:
                self._internal_upsert(filter, updator, raw_result)
        else:
            self._no_id_update(updator)

            @on_err_close(scanner)
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

    def find(self, *args, **kwargs):
        # return a cursor
        return MontyCursor(self, *args, **kwargs)

    def find_one(self, filter=None, *args, **kwargs):
        """ """
        if filter is not None and not isinstance(filter, abc.Mapping):
            filter = {"_id": filter}

        cursor = self.find(filter, *args, **kwargs)
        for result in cursor.limit(-1):
            return result
        return None

    def count(self, filter=None, **kwargs):
        warnings.warn(
            "count is deprecated. Use Collection.count_documents instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.count_documents(filter, **kwargs)

    def count_documents(self, filter, **kwargs):
        cursor = MontyCursor(self, filter=filter, **kwargs)
        return len(list(cursor))

    def distinct(self, key, filter=None, **kwargs):
        """ """
        if not isinstance(key, string_types):
            raise TypeError(
                "key must be an instance of %s" % (string_types.__name__,)
            )

        result = list()

        def get_value(doc):
            fieldwalker = FieldWalker(doc)
            fieldvalues = fieldwalker.go(key).get().value
            res = list()
            for v in fieldvalues.iter_flat():
                weighted = Weighted(v)
                if weighted not in result:
                    res.append(weighted)
            return res

        documents = self._storage.query(MontyCursor(self), 0)

        if filter:
            queryfilter = QueryFilter(filter)
            for doc in documents:
                if queryfilter(doc):
                    result += get_value(doc)
        else:
            for doc in documents:
                result += get_value(doc)

        return [weighted.value for weighted in sorted(result)]

    def drop(self):
        self._database.drop_collection(self._name)

    def save(self, to_save, *args, **kwargs):
        # DEPRECATED
        if "_id" in to_save:
            self.replace_one(
                {"_id": to_save["_id"]}, to_save, upsert=True, *args, **kwargs
            )
        else:
            self.insert_one(to_save, *args, **kwargs)
