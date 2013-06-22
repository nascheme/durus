"""
$URL$
$Id$
"""
from durus.connection import Connection
from durus.persistent_list import PersistentList
from durus.storage import MemoryStorage
from pytest import raises

def interval(n):
    return list(range(n))

class TestPersistentList(object):

    def setup(self):
        self.connection = Connection(MemoryStorage())
        self.root = self.connection.get_root()

    def test_no_arbitrary_attributes(self):
        p = PersistentList()
        raises(AttributeError, setattr, p, 'bogus', 1)

    def test_nonzero(self):
        p = PersistentList()
        assert not p
        self.root['a'] = p
        self.connection.commit()
        p.append(1)
        assert p
        assert p._p_is_unsaved()

    def test_iter(self):
        p = PersistentList()
        assert list(p) == []
        p.extend([2,3,4])
        assert list(p) == [2,3,4]

    def test_insert_again(self):
        p = PersistentList([5,6,7])
        p[1] = 2
        p[1] = 3
        assert p[1] == 3

    def test_contains(self):
        p = PersistentList(x for x in interval(5))
        assert 2 in p
        assert -1 not in p

    def test_cmp(self):
        p = PersistentList(interval(10))
        p2 = PersistentList(interval(10))
        assert p == p2
        assert p == list(p2)
        assert p <= p2
        assert p >= p2
        assert not p < p2
        assert not p > p2
        p.append(3)
        assert p != p2

    def test_delete(self):
        p = PersistentList(x for x in interval(10))
        self.root['x'] = p
        self.connection.commit()
        del p[1]
        assert p._p_is_unsaved()

    def test_pop(self):
        p = PersistentList(x for x in interval(10))
        p.pop()
        assert 9 not in p

    def test_slice(self):
        p = PersistentList(x for x in interval(10))
        p[:] = [2,3]
        assert len(p) == 2
        assert p[-1:] == [3]
        p[1:] = PersistentList(interval(2))
        assert p == [2,0,1], p.data
        p[:] = (3,4)
        assert p == [3,4]
        del p[:1]
        assert p == [4]

    def test_sort(self):
        p = PersistentList(x for x in interval(10))
        p.reverse()
        assert p == list(reversed(interval(10)))
        p = sorted(p)
        assert p == interval(10)

    def test_arith(self):
        p = PersistentList(interval(3))
        p2 = PersistentList(interval(3))
        assert p + p2 == interval(3) + interval(3)
        assert interval(3) + p2 == interval(3) + interval(3)
        assert tuple(interval(3)) + p2 == interval(3) + interval(3)
        assert p + interval(3) == interval(3) + interval(3)
        assert p + tuple(interval(3)) == interval(3) + interval(3)
        assert p * 2 == interval(3) + interval(3)
        p += p2
        assert p == interval(3) + interval(3)
        p2 += interval(3)
        assert p == interval(3) + interval(3)
        p = PersistentList(interval(3))
        p *= 2
        assert p == interval(3) + interval(3)

    def test_other(self):
        p = PersistentList()
        p.insert(0, 2)
        assert p == [2]
        assert p.count(0) == 0
        assert p.count(2) == 1
        assert p.index(2) == 0
        p.remove(2)
        p.extend(PersistentList(interval(3)))
        assert p == interval(3)
