"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/persistent_set.py $
$Id: persistent_set.py 31433 2009-02-02 15:10:18Z dbinger $
"""
from durus.persistent import PersistentObject

class PersistentSet (PersistentObject):

    __slots__ = ['s']

    s_is = set # for type checking using QP's spec module

    def __init__(self, *args):
        self.s = set(*args)

    def __and__(self, other):
        if isinstance(other, PersistentSet):
            return self.__class__(self.s & other.s)
        else:
            return self.__class__(self.s & other)

    def __contains__(self, item):
        return item in self.s

    def __eq__(self, other):
        if not isinstance(other, PersistentSet):
            return False
        return self.s == other.s

    def __ge__(self, other):
        if not isinstance(other, PersistentSet):
            raise TypeError("can only compare to a PersistentSet")
        return self.s >= other.s

    def __gt__(self, other):
        if not isinstance(other, PersistentSet):
            raise TypeError("can only compare to a PersistentSet")
        return self.s > other.s

    def __iand__(self, other):
        self._p_note_change()
        if isinstance(other, PersistentSet):
            self.s &= other.s
        else:
            self.s &= other
        return self

    def __ior__(self, other):
        self._p_note_change()
        if isinstance(other, PersistentSet):
            self.s |= other.s
        else:
            self.s |= other
        return self

    def __isub__(self, other):
        self._p_note_change()
        if isinstance(other, PersistentSet):
            self.s -= other.s
        else:
            self.s -= other
        return self

    def __iter__(self):
        for x in self.s:
            yield x

    def __ixor__(self, other):
        self._p_note_change()
        if isinstance(other, PersistentSet):
            self.s ^= other.s
        else:
            self.s ^= other
        return self

    def __le__(self, other):
        if not isinstance(other, PersistentSet):
            raise TypeError("can only compare to a PersistentSet")
        return self.s <= other.s

    def __len__(self):
        return len(self.s)

    def __lt__(self, other):
        if not isinstance(other, PersistentSet):
            raise TypeError("can only compare to a PersistentSet")
        return self.s < other.s

    def __ne__(self, other):
        if not isinstance(other, PersistentSet):
            return True
        return self.s != other.s

    def __or__(self, other):
        if isinstance(other, PersistentSet):
            return self.__class__(self.s | other.s)
        else:
            return self.__class__(self.s | other)

    def __rand__(self, other):
        return self.__class__(other & self.s)

    def __ror__(self, other):
        return self.__class__(other | self.s)

    def __rsub__(self, other):
        return self.__class__(other - self.s)

    def __rxor__(self, other):
        return self.__class__(other ^ self.s)

    def __sub__(self, other):
        if isinstance(other, PersistentSet):
            return self.__class__(self.s - other.s)
        else:
            return self.__class__(self.s - other)

    def __xor__(self, other):
        if isinstance(other, PersistentSet):
            return self.__class__(self.s ^ other.s)
        else:
            return self.__class__(self.s ^other)

    def add(self, item):
        self._p_note_change()
        self.s.add(item)

    def clear(self):
        self._p_note_change()
        self.s.clear()

    def copy(self):
        return self.__class__(self.s)

    def discard(self, item):
        self._p_note_change()
        self.s.discard(item)

    def pop(self):
        self._p_note_change()
        return self.s.pop()

    def remove(self, item):
        self._p_note_change()
        self.s.remove(item)

    difference = __sub__
    difference_update = __isub__
    intersection = __and__
    intersection_update = __iand__
    issubset = __le__
    issuperset = __ge__
    symmetric_difference = __xor__
    symmetric_difference_update = __ixor__
    union = __or__
    update = __ior__

