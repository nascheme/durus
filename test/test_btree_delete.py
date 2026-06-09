#!/usr/bin/env python
"""Regression tests for BTree.delete() correctness and _count integrity.

These cover two bugs that were only reachable with internal (non-leaf) nodes,
so they hid behind leaf-only test cases:

  * Case 2c deleted from a t-1 child before merging, which could collapse an
    internal sibling into a leaf and raise
    "TypeError: can only concatenate list (not 'NoneType') to list".
  * Cases 3a1/3a2 rotated a child subtree between siblings but only adjusted
    the cached _count by 1 (the separator key), ignoring the moved subtree.
"""

import itertools

from durus.btree import BTree, BNode, BNode4, _NullCount


def _check_counts(node):
    """Assert the cached _count equals the real subtree size, recursively."""
    actual = len(node.items) + sum(_check_counts(c) for c in (node.nodes or []))
    assert not isinstance(node._count, _NullCount)
    assert node._count == actual, (node._count, actual)
    return actual


def _run(node_class, insert, delete):
    bt = BTree(node_class)
    ref = {}
    for k in insert:
        bt[k] = k
        ref[k] = k
    _check_counts(bt.root)
    for k in delete:
        del bt[k]
        del ref[k]
        assert sorted(bt.keys()) == sorted(ref.keys()), (k, "keys")
        assert len(bt) == len(ref), (k, "len")
        _check_counts(bt.root)
    return bt


def test_delete_case_2c_does_not_collapse_sibling():
    """Minimal case that used to raise TypeError in Case 2c.

    insert 0..9 into a t=2 tree, then delete 6, 2, 5: deleting 5 hits Case 2c
    where merging the two children of 5 used to crash.
    """
    bt = _run(BNode, list(range(10)), [6, 2, 5])
    assert sorted(bt.keys()) == [0, 1, 3, 4, 7, 8, 9]


def test_delete_rotation_keeps_count_consistent():
    """Case 3a1/3a2 rotation across internal nodes keeps _count correct.

    insert 0..9 (t=2) then delete 2: this shifts an item -- and a child
    subtree -- from an internal sibling, which used to leave _count off by the
    moved subtree's size.
    """
    bt = _run(BNode, list(range(10)), [6, 2])
    _check_counts(bt.root)
    assert len(bt) == 8


def test_delete_all_orders_small_exhaustive():
    """Every insert-order x delete-order for small trees, t=2 and t=4."""
    for node_class, max_n in [(BNode, 5), (BNode4, 4)]:
        for n in range(1, max_n + 1):
            keys = range(n)
            for insert in itertools.permutations(keys):
                for delete in itertools.permutations(keys):
                    bt = _run(node_class, insert, delete)
                    assert len(bt) == 0


if __name__ == '__main__':
    test_delete_case_2c_does_not_collapse_sibling()
    test_delete_rotation_keeps_count_consistent()
    test_delete_all_orders_small_exhaustive()
    print("All BTree delete regression tests passed")
