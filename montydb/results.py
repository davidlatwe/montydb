# Copyright 2015-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Modifications copyright (C) 2018 davidlatwe
#
# Modifide some doc string, change module name form `pymongo` to `montydb`.
# Removed `_raise_if_unacknowledged` since `acknowledged` will always return
# True.


class _WriteResult(object):
    """Base class for write result classes."""

    __slots__ = ("__acknowledged",)

    def __init__(self):
        self.__acknowledged = True

    @property
    def acknowledged(self):
        """
        """
        return self.__acknowledged


class InsertOneResult(_WriteResult):
    """The return type for :meth:`~montydb.collection.Collection.insert_one`.
    """

    __slots__ = ("__inserted_id", "__acknowledged")

    def __init__(self, inserted_id, *args):
        self.__inserted_id = inserted_id
        super(InsertOneResult, self).__init__()

    def __repr__(self):
        return "InsertOneResult(%s)" % repr(self.__inserted_id)

    @property
    def inserted_id(self):
        """The inserted document's _id."""
        return self.__inserted_id


class InsertManyResult(_WriteResult):
    """The return type for :meth:`~montydb.collection.Collection.insert_many`.
    """

    __slots__ = ("__inserted_ids", "__acknowledged")

    def __init__(self, inserted_ids, *args):
        self.__inserted_ids = inserted_ids
        super(InsertManyResult, self).__init__()

    def __repr__(self):
        return "InsertManyResult(\n  {}\n)".format(
            ",\n  ".join(repr(i) for i in self.__inserted_ids))

    @property
    def inserted_ids(self):
        """A list of _ids of the inserted documents, in the order provided.

        .. note:: If ``False`` is passed for the `ordered` parameter to
          :meth:`~montydb.collection.Collection.insert_many` the server
          may have inserted the documents in a different order than what
          is presented here.
        """
        return self.__inserted_ids


class UpdateResult(_WriteResult):
    """The return type for :meth:`~montydb.collection.Collection.update_one`,
    :meth:`~montydb.collection.Collection.update_many`, and
    :meth:`~montydb.collection.Collection.replace_one`.
    """

    __slots__ = ("__raw_result", "__acknowledged")

    def __init__(self, raw_result, *args):
        self.__raw_result = raw_result
        super(UpdateResult, self).__init__()

    @property
    def raw_result(self):
        """The raw result document returned by the server."""
        return self.__raw_result

    @property
    def matched_count(self):
        """The number of documents matched for this update."""
        if self.upserted_id is not None:
            return 0
        return self.__raw_result.get("n", 0)

    @property
    def modified_count(self):
        """The number of documents modified.

        .. note:: modified_count is only reported by MongoDB 2.6 and later.
          When connected to an earlier server version, or in certain mixed
          version sharding configurations, this attribute will be set to
          ``None``.
        """
        return self.__raw_result.get("nModified")

    @property
    def upserted_id(self):
        """The _id of the inserted document if an upsert took place. Otherwise
        ``None``.
        """
        return self.__raw_result.get("upserted")


class DeleteResult(_WriteResult):
    """The return type for :meth:`~montydb.collection.Collection.delete_one`
    and :meth:`~montydb.collection.Collection.delete_many`"""

    __slots__ = ("__raw_result", "__acknowledged")

    def __init__(self, raw_result, *args):
        self.__raw_result = raw_result
        super(DeleteResult, self).__init__()

    @property
    def raw_result(self):
        """The raw result document returned by the server."""
        return self.__raw_result

    @property
    def deleted_count(self):
        """The number of documents deleted."""
        return self.__raw_result.get("n", 0)


class BulkWriteResult(_WriteResult):
    """An object wrapper for bulk API write results."""

    __slots__ = ("__bulk_api_result", "__acknowledged")

    def __init__(self, bulk_api_result, *args):
        """Create a BulkWriteResult instance.

        :Parameters:
          - `bulk_api_result`: A result dict from the bulk API
        """
        self.__bulk_api_result = bulk_api_result
        super(BulkWriteResult, self).__init__()

    @property
    def bulk_api_result(self):
        """The raw bulk API result."""
        return self.__bulk_api_result

    @property
    def inserted_count(self):
        """The number of documents inserted."""
        return self.__bulk_api_result.get("nInserted")

    @property
    def matched_count(self):
        """The number of documents matched for an update."""
        return self.__bulk_api_result.get("nMatched")

    @property
    def modified_count(self):
        """The number of documents modified.

        .. note:: modified_count is only reported by MongoDB 2.6 and later.
          When connected to an earlier server version, or in certain mixed
          version sharding configurations, this attribute will be set to
          ``None``.
        """
        return self.__bulk_api_result.get("nModified")

    @property
    def deleted_count(self):
        """The number of documents deleted."""
        return self.__bulk_api_result.get("nRemoved")

    @property
    def upserted_count(self):
        """The number of documents upserted."""
        return self.__bulk_api_result.get("nUpserted")

    @property
    def upserted_ids(self):
        """A map of operation index to the _id of the upserted document."""
        if self.__bulk_api_result:
            return dict((upsert["index"], upsert["_id"])
                        for upsert in self.bulk_api_result["upserted"])
