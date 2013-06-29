"""
$URL$
$Id$
"""
from durus.btree import BTree, BNode
from durus.connection import Connection
from durus.storage import MemoryStorage
from random import randint
from pytest import raises, skip
import sys

class TestCoverage(object):

    def test_no_arbitrary_attributes(self):
        bt = BTree()
        with raises(AttributeError):
            setattr(bt, 'bogus', 1)
        with raises(AttributeError):
            setattr(bt.root, 'bogus', 1)

    def test_delete_case_1(self):
        bt = BTree()
        bt[1] = 2
        del bt[1]

    def test_delete_keyerror(self):
        bt = BTree()
        try:
            del bt[1]
        except KeyError:
            assert str(sys.exc_info()[1]) == '1'

    def test_delete_case_2a(self):
        bt = BTree(BNode)
        for j in 'jklmoab':
            bt.add(j)
        del bt['k']

    def test_delete_case_2b(self):
        bt = BTree(BNode)
        for j in 'abcdef':
            bt.add(j)
        assert bt.root.items == [('b', True), ('d', True)]
        del bt['d']

    def test_delete_case_2c(self):
        bt = BTree(BNode)
        for j in 'abcdefghi':
            bt.add(j)
        assert bt.root.items == [('d', True)]
        del bt['d']

    def _delete_case_3(self):
        bt = BTree(BNode)
        for j in range(100):
            bt.add(j)
        assert bt.root.items == [(31, True), (63, True)]
        assert [n.items for n in bt.root.nodes] == [
            [(15, True)], [(47, True)], [(79, True)]]
        assert [[n.items for n in node.nodes]
                for node in bt.root.nodes] == [
            [[(7, True)], [(23, True)]],
            [[(39, True)], [(55, True)]],
            [[(71, True)], [(87, True)]]]
        return bt

    def test_delete_case_3a1(self):
        bt = self._delete_case_3()
        del bt[39]
        del bt[55]

    def test_delete_case_3a2(self):
        bt = self._delete_case_3()
        del bt[39]
        del bt[7]

    def test_delete_case_3b1(self):
        bt = self._delete_case_3()
        del bt[39]

    def test_delete_case_3b2(self):
        bt = self._delete_case_3()
        del bt[7]

    def test_nonzero(self):
        bt = BTree()
        assert not bt
        bt['1'] = 1
        assert bt

    def test_setdefault(self):
        bt = BTree()
        assert bt.setdefault('1', []) == []
        assert bt['1'] == []
        bt.setdefault('1', 1).append(1)
        assert bt['1'] == [1]
        bt.setdefault('1', [])
        assert bt['1'] == [1]
        bt.setdefault('1', 1).append(2)
        assert bt['1'] == [1, 2]

    def test_find_extremes(self):
        bt = BTree()
        raises(AssertionError, bt.get_min_item)
        raises(AssertionError, bt.get_max_item)
        for j in range(100):
            bt.add(j)
        assert bt.get_min_item() == (0, True)
        assert bt.get_max_item() == (99, True)

    def test_iter(self):
        bt = BTree()
        for j in range(100):
            bt.add(j)
        if hasattr(bt, 'iterkeys'):
            assert list(bt) == list(bt.iterkeys())
            assert list(bt.iteritems()) == list(zip(bt, bt.itervalues()))
            assert list(bt.iterkeys()) == list(bt.keys())
            assert list(bt.itervalues()) == list(bt.values())
            assert list(bt.iteritems()) == list(bt.items())
        else:
            assert list(bt) == list(bt.keys())
            assert list(bt.items()) == list(zip(bt, bt.values()))

    def test_reversed(self):
        bt = BTree()
        for j in range(100):
            bt.add(j)
        assert list(reversed(bt)) == list(reversed(list(bt)))

    def test_items_backward(self):
        bt = BTree()
        for j in range(100):
            bt.add(j)
        assert list(reversed(list(bt.items()))) == list(bt.items_backward())

    def test_items_from(self):
        bt = BTree()
        for j in range(100):
            bt.add(j)
        for cutoff in (-1, 1, 50.1, 100, 102):
            assert (list([(x, y) for (x, y) in bt.items() if x >= cutoff]) ==
                    list(bt.items_from(cutoff)))
            assert (list([(x, y) for (x, y) in bt.items() if x > cutoff]) ==
                    list(bt.items_from(cutoff, closed=False)))


    def test_items_backward_from(self):
        bt = BTree()
        for j in range(100):
            bt.add(j)
        for cutoff in (-1, 1, 50.1, 100, 102):
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                                    if x < cutoff]))
            got = list(bt.items_backward_from(cutoff))
            assert expect == got, (cutoff, expect, got)
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                                    if x <= cutoff]))
            got = list(bt.items_backward_from(cutoff, closed=True))
            assert expect == got, (cutoff, expect, got)

    def test_items_range(self):
        bt = BTree()
        for j in range(100):
            bt.add(j)
        lo = 0
        hi = 40
        for lo, hi in [(-1,10), (3, 9), (30, 200), (-10, 200)]:
            expect = list([(x, y) for (x, y) in bt.items()
                        if lo <= x < hi])
            got = list(bt.items_range(lo, hi))
            assert expect == got, (lo, hi, expect, got)
            expect = list([(x, y) for (x, y) in bt.items()
                        if lo < x < hi])
            got = list(bt.items_range(lo, hi, closed_start=False))
            assert expect == got, (lo, hi, expect, got)
            expect = list([(x, y) for (x, y) in bt.items()
                        if lo < x <= hi])
            got = list(bt.items_range(
                lo, hi, closed_start=False, closed_end=True))
            assert expect == got, (lo, hi, expect, got)
            expect = list([(x, y) for (x, y) in bt.items()
                        if lo <= x <= hi])
            got = list(bt.items_range(
                lo, hi, closed_start=True, closed_end=True))
            assert expect == got, (lo, hi, expect, got)
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                        if lo < x <= hi]))
            got = list(bt.items_range(hi, lo))
            assert expect == got, (hi, lo, expect, got)
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                        if lo <= x <= hi]))
            got = list(bt.items_range(hi, lo, closed_end=True))
            assert expect == got, (hi, lo, expect, got)
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                        if lo <= x < hi]))
            got = list(bt.items_range(
                hi, lo, closed_start=False, closed_end=True))
            assert expect == got, (hi, lo, expect, got)
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                        if lo < x < hi]))
            got = list(bt.items_range(
                hi, lo, closed_start=False, closed_end=False))
            assert expect == got, (hi, lo, expect, got)
            expect = list(reversed([(x, y) for (x, y) in bt.items()
                        if lo <= x <= hi]))
            got = list(bt.items_range(hi, lo, closed_end=True))
            assert expect == got, (hi, lo, expect, got)

    def test_search(self):
        bt = BTree(BNode)
        for j in range(100):
            bt.add(j)
        assert bt[1] == True
        try:
            assert bt[-1]
        except KeyError:
            pass

    def test_insert_again(self):
        bt = BTree(BNode)
        bt[1] = 2
        bt[1] = 3
        assert bt[1] == 3
        assert list(bt) == [1], list(bt)

    def test_get(self):
        bt = BTree()
        for j in range(10):
            bt.add(j)
        assert bt.get(2) == True
        assert bt.get(-1) == None
        assert bt.get(-1, 5) == 5

    def test_contains(self):
        bt = BTree()
        for j in range(10):
            bt.add(j)
        assert 2 in bt
        assert -1 not in bt

    def test_has_key(self):
        bt = BTree()
        for j in range(10):
            bt.add(j)
        assert bt.has_key(2)
        assert not bt.has_key(-1)

    def test_clear(self):
        bt = BTree()
        for j in range(10):
            bt.add(j)
        assert bt.has_key(2)
        bt.clear()
        assert not bt.has_key(2)
        assert list(bt.keys()) == []

    def test_update(self):
        bt = BTree()
        bt.update()
        assert not list(bt.items())
        bt.update(a=1)
        assert list(bt.items()) == [('a', 1)]
        bt = BTree()
        bt.update(dict(b=2), a=1)
        assert len(list(bt.items())) == 2
        assert bt['b'] == 2
        assert bt['a'] == 1
        bt = BTree()
        bt.update([('b', 2)], a=1)
        assert len(list(bt.items())) == 2
        assert bt['b'] == 2
        assert bt['a'] == 1
        class Fake(object):
            def items(self):
                return [('1',2)]
        bt.update(Fake())
        raises(TypeError, bt.update, 1, 2)

    def test_insert_item(self):
        # This sequences leads to a splitting where
        # the inserted item has the same key as the split
        # point.
        keys = [3, 56, 11, 57, 1, 32, 106, 98, 103, 108,
        101, 104, 7, 94, 105, 85, 99, 89, 28, 65,
        107, 95, 97, 93, 96, 102, 86, 100, 0, 14,
        35, 15, 12, 6, 84, 90, 2, 81, 4, 5,
        69, 9, 30, 78, 13, 10, 8, 82, 47, 62,
        27, 88, 87, 83, 31, 79, 45, 91, 29, 92,
        34, 33, 44, 25, 50, 26, 16, 17, 19, 43,
        21, 64, 24, 37, 22, 59, 63, 18, 20, 38,
        52, 55, 53, 42, 23, 39, 60, 40, 36, 41,
        46, 61, 77, 75, 68, 74, 73, 71, 72, 70,
        80, 54, 67, 66, 51, 49, 76, 58, 49]
        bt = BTree()
        for i, key in enumerate(keys):
            bt[key] = i
            assert bt[key] is i, (i,  key, bt[key])
        # Test set_bnode_minimum_degree()
        assert bt.get_depth() == 2, bt.get_depth()
        assert bt.get_node_count() == 7, bt.get_node_count()
        assert bt.set_bnode_minimum_degree(4) == True
        assert bt.get_depth() == 3, bt.get_depth()
        assert bt.get_node_count() == 34, bt.get_node_count()

DEBUG = False

class TestSlow(object):

    def test_slow(self):
        if '--slow' not in sys.argv:
            skip('test not run because it is slow')
        if DEBUG:
            from copy import deepcopy

        for bnode_class in BNode.__subclasses__():
            if bnode_class.minimum_degree not in (4, 16):
                continue
            sys.stdout.write('\nbnode_class=%s' % bnode_class)
            bt = BTree(bnode_class)
            d = {}
            number = 0
            limit = 10000
            for k in range(limit*10):
                number = randint(0, limit)
                if DEBUG:
                    bt_bak = deepcopy(bt)
                    d_bak = deepcopy(d)
                if number in bt:
                    assert number in d
                    if randint(0, 1) == 1:
                        del bt[number]
                        del d[number]
                        op = 'del'
                        sys.stdout.write('\ndel bt[%s]' % number)
                else:
                    bt[number] = 1
                    d[number] = 1
                    op = 'ins'
                if DEBUG or k % limit == 0:
                    d_items = sorted(d.items())
                    assert d_items == list(bt.items())
                    if DEBUG and not len(d_items) == len(bt):
                        # set a breakpoint here for interactive debugging
                        if op == 'ins':
                            bt_bak[number] = 1
                        else:
                            del bt_bak[number]
                    assert len(d_items) == len(bt)

class TestDurus(object):

    def setup(self):
        self.connection = Connection(MemoryStorage())

    def teardown(self):
        del self.connection

    def test_a(self):
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
        bt.note_change_of_bnode_containing_key(1)

