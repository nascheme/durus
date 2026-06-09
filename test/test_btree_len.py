#!/usr/bin/env python
"""Test BTree _count tracking"""

from durus.btree import BTree, BNode, BNode16, _NullCount


def test_len_basic():
    """Test that _count is maintained correctly during basic operations"""
    bt = BTree()

    # Empty tree
    assert len(bt) == 0
    assert bt.root._count == 0

    # Add some items
    for i in range(10):
        bt.add(i, i)
        assert len(bt) == i + 1
        assert bt.root._count == i + 1
        # Also check the slow method matches
        assert bt.root.get_count() == i + 1

    print("Basic len tracking works")


def test_len_with_splits():
    """Test _len during node splits"""
    bt = BTree(BNode)

    # Force some splits by adding many items
    for i in range(100):
        bt.add(i, i)
        assert len(bt) == i + 1
        assert bt.root._count == i + 1
        assert bt.root.get_count() == i + 1

    print("Len tracking with splits works")


def test_len_with_deletes():
    """Test _len during deletions"""
    bt = BTree(BNode)

    # Add items
    for i in range(50):
        bt.add(i, i)

    # Delete some
    for i in range(10, 20):
        del bt[i]
        expected = 50 - (i - 10 + 1)
        assert len(bt) == expected
        assert bt.root._count == expected
        assert bt.root.get_count() == expected

    print("Len tracking with deletes works")


def _make_legacy(node):
    """Make node (and descendants) look like data stored before _count
    existed: remove the instance attribute so it falls back to the
    class-default _NullCount() sentinel."""
    node.__dict__.pop('_count', None)
    assert isinstance(node._count, _NullCount)
    for n in (node.nodes or []):
        _make_legacy(n)


def test_legacy_len_and_delete():
    """Nodes stored before _count existed have a _NullCount sentinel.

    len() must still work (via the get_count() fallback) and, crucially,
    delete() must not choke on the sentinel.  Regression test for a
    TypeError ("'_NullCount' object cannot be interpreted as an integer")
    raised once BNode grew a __len__ that routes bool(node) through _count.
    """
    for node_class in (BNode, BNode16):
        bt = BTree(node_class)
        ref = {}
        for i in range(300):
            bt[i] = i * i
            ref[i] = i * i
        _make_legacy(bt.root)

        # O(1) path is unavailable for legacy nodes; fall back to get_count().
        assert len(bt) == len(ref)
        # len() on the node itself must also use the fallback, not leak the
        # _NullCount sentinel.
        assert len(bt.root) == len(ref)

        # Deletion must work despite the _NullCount sentinel.
        for k in list(range(300)):
            del bt[k]
            del ref[k]
            assert sorted(bt.keys()) == sorted(ref.keys())
        assert len(bt) == 0
        assert list(bt.items()) == []
    print("Legacy _NullCount len/delete works")


if __name__ == '__main__':
    test_len_basic()
    test_len_with_splits()
    test_len_with_deletes()
    test_legacy_len_and_delete()
    print("\nAll BTree _len tests passed")
