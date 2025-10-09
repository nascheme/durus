#!/usr/bin/env python
"""Test BTree _count tracking"""

from durus.btree import BTree, BNode


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


if __name__ == '__main__':
    test_len_basic()
    test_len_with_splits()
    test_len_with_deletes()
    print("\nAll BTree _len tests passed")
