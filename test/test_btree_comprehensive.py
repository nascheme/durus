#!/usr/bin/env python
"""Comprehensive test that BTree still works correctly with _len tracking"""

from durus.btree import BTree, BNode
import random

def test_comprehensive():
    """Test that BTree works correctly with many operations"""
    bt = BTree(BNode)
    data = {}
    
    # Add many random items
    print("Testing inserts...")
    for _ in range(500):
        key = random.randint(0, 1000)
        value = random.randint(0, 1000)
        bt[key] = value
        data[key] = value
        assert len(bt) == len(data)
        assert bt.root._len == bt.root.get_count()
    
    # Verify all items are there
    print("Verifying data...")
    for key, value in data.items():
        assert bt[key] == value
    
    # Delete random items
    print("Testing deletes...")
    keys_to_delete = random.sample(list(data.keys()), min(100, len(data)))
    for key in keys_to_delete:
        del bt[key]
        del data[key]
        assert len(bt) == len(data)
        assert bt.root._len == bt.root.get_count()
    
    # Verify remaining items
    print("Verifying remaining data...")
    for key, value in data.items():
        assert bt[key] == value
    
    # Test that _len is always consistent
    print("Final consistency check...")
    assert len(bt) == len(data)
    assert bt.root._len == bt.root.get_count()
    
    print(f"All tests passed! Final size: {len(bt)} items")

if __name__ == '__main__':
    random.seed(42)  # For reproducibility
    test_comprehensive()