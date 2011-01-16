"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/persistent_dict.py $
$Id: persistent_dict.py 30862 2008-06-18 14:13:35Z dbinger $
"""
from copy import copy
from durus.persistent import PersistentObject
from durus.utils import iteritems

class PersistentDict (PersistentObject):
    """
    Instance attributes:
      data : dict
    """
    __slots__ = ['data']

    data_is = dict # for type checking using QP's spec module

    def __init__(self, *args, **kwargs):
        self.data = dict(*args, **kwargs)

    def __eq__(self, other):
        return isinstance(other, PersistentDict) and self.data == other.data

    def __ne__(self, other):
        return not self == other

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
        result = copy(self)
        result.data = self.data.copy()
        return result

    def keys(self):
        return list(self.data.keys())

    def items(self):
        return list(self.data.items())

    def iteritems(self):
        return iteritems(self.data)

    def iterkeys(self):
        for k, v in self.iteritems():
            yield k

    def itervalues(self):
        for k, v in self.iteritems():
            yield v

    def values(self):
        return list(self.data.values())

    def has_key(self, key):
        return key in self.data

    def update(self, *others, **kwargs):
        self._p_note_change()
        if len(others) > 1:
            raise TypeError("update() expected at most 1 argument")
        elif others:
            other = others[0]
            if isinstance(other, PersistentDict):
                self.data.update(other.data)
            elif isinstance(other, dict):
                self.data.update(other)
            elif hasattr(other, 'keys'):
                for k in other.keys():
                    self[k] = other[k]
            else:
                for k, v in other:
                    self[k] = v
        for kw in kwargs:
            self[kw] = kwargs[kw]

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


