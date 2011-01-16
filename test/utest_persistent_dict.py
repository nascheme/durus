"""
$URL: svn+ssh://svn.mems-exchange.org/repos/trunk/durus/test/utest_persistent_dict.py $
$Id: utest_persistent_dict.py 30862 2008-06-18 14:13:35Z dbinger $
"""
from durus.connection import Connection
from durus.persistent_dict import PersistentDict
from durus.storage import MemoryStorage
from sancho.utest import UTest, raises

class PersistentDictTest (UTest):

    def no_arbitrary_attributes(self):
        pd = PersistentDict()
        raises(AttributeError, setattr, pd, 'bogus', 1)

    def nonzero(self):
        pd = PersistentDict()
        assert not pd
        pd['1'] = 1
        assert pd

    def setdefault(self):
        pd = PersistentDict()
        assert pd.setdefault('1', []) == []
        assert pd['1'] == []
        pd.setdefault('1', 1).append(1)
        assert pd['1'] == [1]
        pd.setdefault('1', [])
        assert pd['1'] == [1]
        pd.setdefault('1', 1).append(2)
        assert pd['1'] == [1, 2]

    def iter(self):
        pd = PersistentDict()
        assert list(pd) == []
        pd[1] = 2
        assert list(pd) == [1]

    def insert_again(self):
        pd = PersistentDict()
        pd[1] = 2
        pd[1] = 3
        assert pd[1] == 3
        assert list(pd) == [1], list(pd)

    def get(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert pd.get(2) == True
        assert pd.get(-1) == None
        assert pd.get(-1, 5) == 5

    def contains(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert 2 in pd
        assert -1 not in pd

    def has_key(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert pd.has_key(2)
        assert not pd.has_key(-1)

    def clear(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert pd.has_key(2)
        pd.clear()
        assert not pd.has_key(2)
        assert list(pd.keys()) == []

    def update(self):
        pd = PersistentDict()
        pd.update()
        raises(TypeError, pd.update, {}, {})
        assert not list(pd.items())
        pd.update(a=1)
        assert list(pd.items()) == [('a', 1)]
        pd = PersistentDict()
        pd.update(dict(b=2), a=1)
        assert len(list(pd.items())) == 2
        assert pd['b'] == 2
        assert pd['a'] == 1
        pd = PersistentDict()
        pd.update([('b', 2)], a=1)
        assert len(pd.items()) == 2
        assert pd['b'] == 2
        assert pd['a'] == 1
        pd2 = PersistentDict((x, True) for x in range(10))
        pd.update(pd2)
        class keyed(object):
            data = dict(a=3)
            keys = data.keys
            __setitem__ = data.__setitem__
            __getitem__ = data.__getitem__
        pd.update(keyed())
        assert pd['a'] == 3

    def cmp(self):
        pd = PersistentDict((x, True) for x in range(10))
        pd2 = PersistentDict((x, True) for x in range(10))
        assert pd == pd2
        assert dict(pd) == dict(pd2)

    def delete(self):
        connection = Connection(MemoryStorage())
        pd = PersistentDict((x, True) for x in range(10))
        connection.root['x'] = pd
        connection.commit()
        del pd[1]
        assert pd._p_is_unsaved()

    def copy(self):
        connection = Connection(MemoryStorage())
        pd = PersistentDict((x, True) for x in range(10))
        pd2 = pd.copy()
        assert pd == pd2
        pd[1] = 34
        assert pd != pd2

    def iter(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert list(pd.iteritems()) == list(zip(pd.iterkeys(), pd.itervalues()))
        assert list(pd.items()) == list(zip(pd.keys(), pd.values()))

    def pops(self):
        pd = PersistentDict((x, True) for x in range(10))
        pd.pop(3)
        assert 3 not in pd
        assert type(pd.popitem()) is tuple

    def fromkeys(self):
        x = PersistentDict.fromkeys(dict(a=2), value=4)
        assert isinstance(x, PersistentDict)
        assert dict(x) == dict(a=4)


if __name__ == '__main__':
    PersistentDictTest()
