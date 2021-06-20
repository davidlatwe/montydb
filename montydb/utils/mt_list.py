
from ..engine.field_walker import FieldWalker
from ..engine.weighted import Weighted
from ..engine.queries import QueryFilter, ordering
from ..engine.project import Projector
from ..types import integer_types, init_bson, bson
from ..base import (
    _fields_list_to_dict,
    _index_document,
    _index_list,
)


def _to_cmp(mlist, other):
    mlist = super(MontyList, mlist).__getitem__(slice(None))
    if isinstance(other, MontyList):
        other = super(MontyList, other).__getitem__(slice(None))
    return mlist, other


class MontyList(list):
    """A list type with the ability to perform Mongo's CRUD.

    Experimental, a subclass of list, combined the common CRUD methods from
    pymongo's Collection and Cursor.
    """

    def __init__(self,
                 documents=None,
                 name=None,
                 doc_type=None,
                 use_bson=None):
        super(MontyList, self).__init__(documents or [])
        init_bson(use_bson=use_bson)
        self.name = name or bson.ObjectId()
        self.doc_type = doc_type or dict  # Set `dict` as default doc type
        self.rewind()

    def __repr__(self):
        return "MontyList({})".format(super(MontyList, self).__repr__())

    def __iter__(self):
        return self

    def __eq__(self, other):
        mlist, other = _to_cmp(self, other)
        return Weighted(mlist) == Weighted(other)

    def __gt__(self, other):
        mlist, other = _to_cmp(self, other)
        return Weighted(mlist) > Weighted(other)

    def __lt__(self, other):
        mlist, other = _to_cmp(self, other)
        return Weighted(mlist) < Weighted(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __ge__(self, other):
        return not self.__lt__(other)

    def __le__(self, other):
        return not self.__gt__(other)

    def next(self):
        """Advance the cursor."""
        self.__iter_count += 1
        try:
            return self[self.__iter_count]
        except IndexError:
            raise StopIteration

    __next__ = next

    def __getitem__(self, index):
        if isinstance(index, slice):
            return MontyList(super(MontyList, self).__getitem__(index))
        if isinstance(index, integer_types):
            return super(MontyList, self).__getitem__(index)
        raise TypeError("index %r cannot be applied to MontyList "
                        "instances" % index)

    def rewind(self):
        self.__iter_count = -1

    def find(self,
             filter=None,
             projection=None,
             sort=None):
        """Return a filtered MontyList()
        """
        if projection is not None:
            projection = _fields_list_to_dict(projection, "projection")
        sort = sort and _index_document(sort) or None

        spec, proj = filter or {}, projection,

        qf = QueryFilter(spec)
        pj = Projector(proj, qf) if proj is not None else None

        fieldwalkers = [qf.fieldwalker for doc in self[:]
                        if qf(doc, self.doc_type)]
        if sort:
            fieldwalkers = ordering(fieldwalkers, sort)
        if pj is not None:
            for fw in fieldwalkers:
                pj(fw)

        return MontyList([fw.doc for fw in fieldwalkers])

    def sort(self, key_or_list, direction=None):
        keys = _index_list(key_or_list, direction)
        sort = _index_document(keys)
        fieldwalkers = [FieldWalker(doc, self.doc_type) for doc in self[:]]
        self[:] = [fw.doc for fw in
                   ordering(fieldwalkers, sort, self.doc_type)]
        return self

    def replace_one(self):
        raise NotImplementedError("Not implemented.")

    def update_one(self):
        raise NotImplementedError("Not implemented.")

    def update_many(self):
        raise NotImplementedError("Not implemented.")

    def delete_one(self):
        raise NotImplementedError("Not implemented.")

    def delete_many(self):
        raise NotImplementedError("Not implemented.")
