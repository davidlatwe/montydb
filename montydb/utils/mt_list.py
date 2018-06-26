
from bson import ObjectId
from bson.py3compat import integer_types

from ..engine.core import FieldWalker, Weighted
from ..engine.queries import QueryFilter, ordering
from ..engine.project import Projector
from ..base import (
    _fields_list_to_dict,
    _index_document,
    _index_list,
    _bson_touch,
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

    def __init__(self, documents=None, name=None):
        super(MontyList, self).extend(documents or [])
        self.name = name or ObjectId()
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
        spec = _bson_touch(filter or {})
        proj = _bson_touch(projection)

        qf = QueryFilter(spec)
        pj = Projector(proj, qf) if proj is not None else None

        fieldwalkers = [qf.fieldwalker for doc in self[:] if qf(doc)]
        if sort:
            fieldwalkers = ordering(fieldwalkers, sort)
        if pj is not None:
            for fw in fieldwalkers:
                pj(fw)

        return MontyList([fw.doc for fw in fieldwalkers])

    def sort(self, key_or_list, direction=None):
        keys = _index_list(key_or_list, direction)
        sort = _index_document(keys)
        fieldwalkers = [FieldWalker(doc) for doc in self[:]]
        self[:] = [fw.doc for fw in ordering(fieldwalkers, sort)]
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