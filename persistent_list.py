"""$URL$
$Id$
"""
from durus.persistent import Persistent

class PersistentList(Persistent):

    """
    Instance attributes:
      data : list
    """
    data_is = list

    def __init__(self, *args, **kwargs):
        self.data = list(*args, **kwargs)

    def __cast(self, other):
        if isinstance(other, PersistentList): return other.data
        else: return other

    def __lt__(self, other):
        return self.data <  self.__cast(other)

    def __le__(self, other):
        return self.data <= self.__cast(other)

    def __eq__(self, other):
        return self.data == self.__cast(other)

    def __ne__(self, other):
        return self.data != self.__cast(other)

    def __gt__(self, other):
        return self.data >  self.__cast(other)

    def __ge__(self, other):
        return self.data >= self.__cast(other)

    def __cmp__(self, other):
        return cmp(self.data, self.__cast(other))

    def __contains__(self, item):
        return item in self.data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, i):
        return self.data[i]

    def __setitem__(self, i, item):
        self._p_note_change()
        self.data[i] = item

    def __delitem__(self, i):
        self._p_note_change()
        del self.data[i]

    def __getslice__(self, i, j):
        i = max(i, 0); j = max(j, 0)
        return self.__class__(self.data[i:j])

    def __setslice__(self, i, j, other):
        self._p_note_change()
        i = max(i, 0); j = max(j, 0)
        if isinstance(other, PersistentList):
            self.data[i:j] = other.data
        elif isinstance(other, type(self.data)):
            self.data[i:j] = other
        else:
            self.data[i:j] = list(other)

    def __delslice__(self, i, j):
        self._p_note_change()
        i = max(i, 0); j = max(j, 0)
        del self.data[i:j]

    def __add__(self, other):
        if isinstance(other, PersistentList):
            return self.__class__(self.data + other.data)
        elif isinstance(other, type(self.data)):
            return self.__class__(self.data + other)
        else:
            return self.__class__(self.data + list(other))

    def __radd__(self, other):
        if isinstance(other, PersistentList):
            return self.__class__(other.data + self.data)
        elif isinstance(other, type(self.data)):
            return self.__class__(other + self.data)
        else:
            return self.__class__(list(other) + self.data)

    def __iadd__(self, other):
        self._p_note_change()
        if isinstance(other, PersistentList):
            self.data += other.data
        else:
            self.data += list(other)
        return self

    def __mul__(self, n):
        return self.__class__(self.data * n)

    __rmul__ = __mul__

    def __imul__(self, n):
        self._p_note_change()
        self.data *= n
        return self

    def append(self, item):
        self._p_note_change()
        self.data.append(item)

    def insert(self, i, item):
        self._p_note_change()
        self.data.insert(i, item)

    def pop(self, i=-1):
        self._p_note_change()
        return self.data.pop(i)

    def remove(self, item):
        self._p_note_change()
        self.data.remove(item)

    def count(self, item):
        return self.data.count(item)

    def index(self, item, *args):
        return self.data.index(item, *args)

    def reverse(self):
        self._p_note_change()
        self.data.reverse()

    def sort(self, *args):
        self._p_note_change()
        self.data.sort(*args)

    def extend(self, other):
        self._p_note_change()
        if isinstance(other, PersistentList):
            self.data.extend(other.data)
        else:
            self.data.extend(other)
