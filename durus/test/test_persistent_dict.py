"""
$URL$
$Id$
"""
from durus.connection import Connection
from durus.persistent_dict import PersistentDict
from durus.storage import MemoryStorage
from pytest import raises

class TestPersistentDict(object):

    def test_no_arbitrary_attributes(self):
        pd = PersistentDict()
        raises(AttributeError, setattr, pd, 'bogus', 1)

    def test_nonzero(self):
        pd = PersistentDict()
        assert not pd
        pd['1'] = 1
        assert pd

    def test_setdefault(self):
        pd = PersistentDict()
        assert pd.setdefault('1', []) == []
        assert pd['1'] == []
        pd.setdefault('1', 1).append(1)
        assert pd['1'] == [1]
        pd.setdefault('1', [])
        assert pd['1'] == [1]
        pd.setdefault('1', 1).append(2)
        assert pd['1'] == [1, 2]

    def test_iter(self):
        pd = PersistentDict()
        assert list(pd) == []
        pd[1] = 2
        assert list(pd) == [1]

    def test_insert_again(self):
        pd = PersistentDict()
        pd[1] = 2
        pd[1] = 3
        assert pd[1] == 3
        assert list(pd) == [1], list(pd)

    def test_get(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert pd.get(2) == True
        assert pd.get(-1) == None
        assert pd.get(-1, 5) == 5

    def test_contains(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert 2 in pd
        assert -1 not in pd

    def test_has_key(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert pd.has_key(2)
        assert not pd.has_key(-1)

    def test_clear(self):
        pd = PersistentDict((x, True) for x in range(10))
        assert pd.has_key(2)
        pd.clear()
        assert not pd.has_key(2)
        assert list(pd.keys()) == []

    def test_update(self):
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

    def test_cmp(self):
        pd = PersistentDict((x, True) for x in range(10))
        pd2 = PersistentDict((x, True) for x in range(10))
        assert pd == pd2
        assert dict(pd) == dict(pd2)

    def test_delete(self):
        connection = Connection(MemoryStorage())
        pd = PersistentDict((x, True) for x in range(10))
        connection.root['x'] = pd
        connection.commit()
        del pd[1]
        assert pd._p_is_unsaved()

    def test_copy(self):
        connection = Connection(MemoryStorage())
        pd = PersistentDict((x, True) for x in range(10))
        pd2 = pd.copy()
        assert pd == pd2
        pd[1] = 34
        assert pd != pd2

    def test_iter(self):
        pd = PersistentDict((x, True) for x in range(10))
        if hasattr({}, 'iteritems'):
            assert list(pd.iteritems()) == list(zip(pd.iterkeys(), pd.itervalues()))
        else:
            assert list(pd.items()) == list(zip(pd.keys(), pd.values()))
        assert list(pd.items()) == list(zip(pd.keys(), pd.values()))

    def test_pops(self):
        pd = PersistentDict((x, True) for x in range(10))
        pd.pop(3)
        assert 3 not in pd
        assert type(pd.popitem()) is tuple

    def test_fromkeys(self):
        x = PersistentDict.fromkeys(dict(a=2), value=4)
        assert isinstance(x, PersistentDict)
        assert dict(x) == dict(a=4)
