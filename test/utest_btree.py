"""
$URL$
$Id$
"""
from durus.btree import BTree, BNode
from durus.connection import Connection
from durus.file_storage import TempFileStorage
from random import randint
from sancho.utest import UTest, raises

class CoverageTest(UTest):

    def delete_case_1(self):
        bt = BTree()
        bt[1] = 2
        del bt[1]

    def delete_keyerror(self):
        bt = BTree()
        try:
            del bt[1]
        except KeyError, e:
            assert str(e) == '1'

    def delete_case_2a(self):
        bt = BTree(BNode)
        map(bt.add, 'jklmoab')
        del bt['k']

    def delete_case_2b(self):
        bt = BTree(BNode)
        map(bt.add, 'abcdef')
        assert bt.root.items == [('b', True), ('d', True)]
        del bt['d']

    def delete_case_2c(self):
        bt = BTree(BNode)
        map(bt.add, 'abcdefghi')
        assert bt.root.items == [('d', True)]
        del bt['d']

    def _delete_case_3(self):
        bt = BTree(BNode)
        map(bt.add, range(100))
        assert bt.root.items == [(31, True), (63, True)]
        assert [n.items for n in bt.root.nodes] == [
            [(15, True)], [(47, True)], [(79, True)]]
        assert [[n.items for n in node.nodes]
                for node in bt.root.nodes] == [
            [[(7, True)], [(23, True)]],
            [[(39, True)], [(55, True)]],
            [[(71, True)], [(87, True)]]]
        return bt

    def delete_case_3a1(self):
        bt = self._delete_case_3()
        del bt[39]
        del bt[55]

    def delete_case_3a2(self):
        bt = self._delete_case_3()
        del bt[39]
        del bt[7]

    def delete_case_3b1(self):
        bt = self._delete_case_3()
        del bt[39]

    def delete_case_3b2(self):
        bt = self._delete_case_3()
        del bt[7]

    def nonzero(self):
        bt = BTree()
        assert not bt
        bt['1'] = 1
        assert bt

    def setdefault(self):
        bt = BTree()
        assert bt.setdefault('1', []) == []
        assert bt['1'] == []
        bt.setdefault('1', 1).append(1)
        assert bt['1'] == [1]
        bt.setdefault('1', [])
        assert bt['1'] == [1]
        bt.setdefault('1', 1).append(2)
        assert bt['1'] == [1, 2]

    def find_extremes(self):
        bt = BTree()
        assert raises(AssertionError, bt.get_min_item)
        assert raises(AssertionError, bt.get_max_item)
        map(bt.add, range(100))
        assert bt.get_min_item() == (0, True)
        assert bt.get_max_item() == (99, True)

    def iter(self):
        bt = BTree()
        map(bt.add, range(100))
        assert list(bt) == list(bt.iterkeys())
        assert list(bt.iteritems()) == zip(bt, bt.itervalues())
        assert list(bt.iterkeys()) == bt.keys()
        assert list(bt.itervalues()) == bt.values()
        assert list(bt.iteritems()) == bt.items()

    def reversed(self):
        bt = BTree()
        map(bt.add, range(100))
        assert list(reversed(bt)) == list(reversed(list(bt)))

    def items_backward(self):
        bt = BTree()
        map(bt.add, range(100))
        assert list(reversed(bt.items())) == list(bt.items_backward())

    def items_from(self):
        bt = BTree()
        map(bt.add, range(100))
        for cutoff in (-1, 1, 50.1, 100, 102):
            assert (list([(x, y) for (x, y) in bt.items() if x >= cutoff]) ==
                    list(bt.items_from(cutoff)))

    def items_backward_from(self):
        bt = BTree()
        map(bt.add, range(100))
        for cutoff in (-1, 1, 50.1, 100, 102):
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                                    if x < cutoff]))
            got = list(bt.items_backward_from(cutoff))
            assert expect == got, (cutoff, expect, got)

    def items_range(self):
        bt = BTree()
        map(bt.add, range(100))
        lo = 0
        hi = 40
        for lo, hi in [(-1,10), (3, 9), (30, 200), (-10, 200)]:
            expect = list([(x, y) for (x, y) in bt.items()
                        if lo <= x < hi])
            got = list(bt.items_range(lo, hi))
            assert expect == got, (lo, hi, expect, got)
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                        if lo < x <= hi]))
            got = list(bt.items_range(hi, lo))
            assert expect == got, (hi, lo, expect, got)

    def search(self):
        bt = BTree(BNode)
        map(bt.add, range(100))
        assert bt[1] == True
        try:
            assert bt[-1]
        except KeyError:
            pass

    def insert_again(self):
        bt = BTree(BNode)
        bt[1] = 2
        bt[1] = 3
        assert bt[1] == 3
        assert list(bt) == [1], list(bt)

    def get(self):
        bt = BTree()
        map(bt.add, range(10))
        assert bt.get(2) == True
        assert bt.get(-1) == None
        assert bt.get(-1, 5) == 5

    def contains(self):
        bt = BTree()
        map(bt.add, range(10))
        assert 2 in bt
        assert -1 not in bt

    def has_key(self):
        bt = BTree()
        map(bt.add, range(10))
        assert bt.has_key(2)
        assert not bt.has_key(-1)

    def clear(self):
        bt = BTree()
        map(bt.add, range(10))
        assert bt.has_key(2)
        bt.clear()
        assert not bt.has_key(2)
        assert bt.keys() == []

class SlowTest(UTest):

    def slow(self):
        bt = BTree()
        print 'bt = BTree()'
        d = {}
        number = 0
        limit = 10000
        for k in xrange(limit*10):
            number = randint(0, limit)
            if number in bt:
                assert number in d
                if randint(0, 1) == 1:
                    del bt[number]
                    del d[number]
                    print 'del bt[%s]' % number
            else:
                if number in d:
                    print number
                    print number in bt
                    print number in d
                    assert number not in d
                bt[number] = 1
                d[number] = 1
                print 'bt[%s] = 1' % number
            if k % limit == 0:
                d_items = d.items()
                d_items.sort()
                assert d_items == bt.items()
                assert len(d_items) == len(bt)

class DurusTest(UTest):

    def _pre(self):
        self.connection = Connection(TempFileStorage())

    def _post(self):
        del self.connection

    def a(self):
        bt = self.connection.get_root()['bt'] = BTree()
        t = bt.root.minimum_degree
        assert self.connection.get_cache_count() == 1
        for x in range(2 * t - 1):
            bt.add(x)
        self.connection.commit()
        assert self.connection.get_cache_count() == 3
        bt.add(2 * t - 1)
        self.connection.commit()
        assert self.connection.get_cache_count() == 5


if __name__ == '__main__':
    CoverageTest()
    DurusTest()
    SlowTest()
