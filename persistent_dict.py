"""$URL$
$Id$
"""

from copy import copy
from durus.persistent import Persistent

class PersistentDict(Persistent):

    """
    Instance attributes:
      data : dict
    """
    data_is = dict

    def __init__(self, *args, **kwargs):
        self.data = dict(*args, **kwargs)

    def __cmp__(self, dict):
        if isinstance(dict, PersistentDict):
            return cmp(self.data, dict.data)
        else:
            return cmp(self.data, dict)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, item):
        self._p_note_change()
        self.data[key] = item

    def __delitem__(self, key):
        self._p_note_change()
        del self.data[key]

    def clear(self):
        self._p_note_change()
        self.data.clear()

    def copy(self):
        if self.__class__ is PersistentDict:
            return PersistentDict(self.data)
        # Use the copy module to copy self without data, and then use the
        # update method to fill the data in the new instance.
        changed = self.get_p_changed()
        data = self.data
        try:
            self.data = {} # This is why we saved _p_changed.
            c = copy(self)
        finally:
            self.data = data
            self._p_note_change(changed)
        c.update(self)
        return c

    def keys(self):
        return self.data.keys()

    def items(self):
        return self.data.items()

    def iteritems(self):
        return self.data.iteritems()

    def iterkeys(self):
        return self.data.iterkeys()

    def itervalues(self):
        return self.data.itervalues()

    def values(self):
        return self.data.values()

    def has_key(self, key):
        return self.data.has_key(key)

    def update(self, other):
        self._p_note_change()
        if isinstance(other, PersistentDict):
            self.data.update(other.data)
        elif isinstance(other, dict):
            self.data.update(other)
        else:
            for k, v in dict.items():
                self[k] = v

    def get(self, key, failobj=None):
        return self.data.get(key, failobj)

    def setdefault(self, key, failobj=None):
        if key not in self.data:
            self._p_note_change()
            self.data[key] = failobj
            return failobj
        return self.data[key]

    def pop(self, key, *args):
        self._p_note_change()
        return self.data.pop(key, *args)

    def popitem(self):
        self._p_note_change()
        return self.data.popitem()

    def __contains__(self, key):
        return key in self.data

    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            d[key] = value
        return d
    fromkeys = classmethod(fromkeys)

    def __iter__(self):
        return iter(self.data)

