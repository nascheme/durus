"""$URL$
$Id$
"""

from durus.persistent import Persistent

class BNode(Persistent):
    """
    Instance attributes:
      items: list
      nodes: [BNode]
    """

    minimum_degree = 2 # a.k.a. t

    def __init__(self):
        self.items = []
        self.nodes = None

    def is_leaf(self):
        return self.nodes is None

    def __iter__(self):
        if self.is_leaf():
            for item in self.items:
                yield item
        else:
            for position, item in enumerate(self.items):
                for it in self.nodes[position]:
                    yield it
                yield item
            for it in self.nodes[-1]:
                yield it

    def is_full(self):
        return len(self.items) == 2 * self.minimum_degree - 1

    def get_position(self, key):
        for position, item in enumerate(self.items):
            if item[0] >= key:
                return position
        return len(self.items)

    def search(self, key):
        """(key:anything) -> None | (key:anything, value:anything)
        Return the matching pair, or None.
        """
        position = self.get_position(key)
        if position < len(self.items) and self.items[position][0] == key:
            return self.items[position]
        elif self.is_leaf():
            return None
        else:
            return self.nodes[position].search(key)

    def insert_item(self, item):
        """(item:(key:anything, value:anything))
        """
        assert not self.is_full()
        key = item[0]
        position = self.get_position(key)
        if position < len(self.items) and self.items[position][0] == key:
            self.items[position] = item
            self._p_changed = 1
        elif self.is_leaf():
            self.items.insert(position, item)
            self._p_changed = 1
        else:
            child = self.nodes[position]
            if child.is_full():
                self.split_child(position, child)
                if key > self.items[position][0]:
                    position += 1
            self.nodes[position].insert_item(item)

    def split_child(self, position, child):
        """(position:int, child:BNode)
        """
        assert not self.is_full()
        assert not self.is_leaf()
        assert self.nodes[position] is child
        assert child.is_full()
        bigger = self.__class__()
        middle = self.minimum_degree - 1
        splitting_key = child.items[middle]
        bigger.items = child.items[middle + 1:]
        child.items = child.items[:middle]
        assert len(bigger.items) == len(child.items)
        if not child.is_leaf():
            bigger.nodes = child.nodes[middle + 1:]
            child.nodes = child.nodes[:middle + 1]
            assert len(bigger.nodes) == len(child.nodes)
        self.items.insert(position, splitting_key)
        self.nodes.insert(position + 1, bigger)
        self._p_changed = 1

    def get_min_item(self):
        """() -> (key:anything, value:anything)
        Return the item with the minimal key.
        """
        if self.is_leaf():
            return self.items[0]
        else:
            return self.nodes[0].get_min_item()

    def get_max_item(self):
        """() -> (key:anything, value:anything)
        Return the item with the maximal key.
        """
        if self.is_leaf():
            return self.items[-1]
        else:
            return self.nodes[-1].get_max_item()

    def delete(self, key):
        """(key:anything)
        Delete the item with this key.
        This is intended to follow the description in 19.3 of
        'Introduction to Algorithms' by Cormen, Lieserson, and Rivest.
        """
        def is_big(node):
            # Precondition for recursively calling node.delete(key).
            return node and len(node.items) >= self.minimum_degree
        p = self.get_position(key)
        matches = p < len(self.items) and self.items[p][0] == key
        if self.is_leaf():
            if matches:
                # Case 1.
                del self.items[p]
                self._p_changed = 1
            else:
                raise KeyError(key)
        else:
            node = self.nodes[p]
            lower_sibling = p > 0 and self.nodes[p - 1]
            upper_sibling = p < len(self.nodes) - 1 and self.nodes[p + 1]
            def is_big(n):
                return n and len(n.items) >= n.minimum_degree
            if matches:
                # Case 2.
                if is_big(node):
                    # Case 2a.
                    extreme = node.get_max_item()
                    node.delete(extreme[0])
                    self.items[p] = extreme
                elif is_big(upper_sibling):
                    # Case 2b.
                    extreme = upper_sibling.get_min_item()
                    upper_sibling.delete(extreme[0])
                    self.items[p] = extreme
                else:
                    # Case 2c.
                    extreme = upper_sibling.get_min_item()
                    upper_sibling.delete(extreme[0])
                    node.items = node.items + [extreme] + upper_sibling.items
                    if not node.is_leaf():
                        node.nodes = node.nodes + upper_sibling.nodes
                    del self.items[p]
                    del self.nodes[p + 1]
                self._p_changed = 1
            else:
                if not is_big(node):
                    if is_big(lower_sibling):
                        # Case 3a1: Shift an item from lower_sibling.
                        node.items.insert(0, self.items[p - 1])
                        self.items[p - 1] = lower_sibling.items[-1]
                        del lower_sibling.items[-1]
                        if not node.is_leaf():
                            node.nodes.insert(0, lower_sibling.nodes[-1])
                            del lower_sibling.nodes[-1]
                        lower_sibling._p_changed = 1
                    elif is_big(upper_sibling):
                        # Case 3a2: Shift an item from upper_sibling.
                        node.items.append(self.items[p])
                        self.items[p] = upper_sibling.items[0]
                        del upper_sibling.items[0]
                        if not node.is_leaf():
                            node.nodes.append(upper_sibling.nodes[0])
                            del upper_sibling.nodes[0]
                        upper_sibling._p_changed = 1
                    elif lower_sibling:
                        # Case 3b1: Merge with lower_sibling
                        node.items = (lower_sibling.items + [self.items[p-1]] +
                                      node.items)
                        if not node.is_leaf():
                            node.nodes = lower_sibling.nodes + node.nodes
                        del self.items[p-1]
                        del self.nodes[p-1]
                    else:
                        # Case 3b2: Merge with upper_sibling
                        node.items = (node.items + [self.items[p]] +
                                      upper_sibling.items)
                        if not node.is_leaf():
                            node.nodes = node.nodes + upper_sibling.nodes
                        del self.items[p]
                        del self.nodes[p+1]
                    self._p_changed = 1
                    node._p_changed = 1
                assert is_big(node)
                node.delete(key)
            if not self.items:
                # This can happen when self is the root node.
                self.items = self.nodes[0].items
                self.nodes = self.nodes[0].nodes


class BNode2  (BNode): minimum_degree = 2
class BNode4  (BNode): minimum_degree = 4
class BNode8  (BNode): minimum_degree = 8
class BNode16 (BNode): minimum_degree = 16
class BNode32 (BNode): minimum_degree = 32
class BNode64 (BNode): minimum_degree = 64
class BNode128(BNode): minimum_degree = 128
class BNode256(BNode): minimum_degree = 256
class BNode512(BNode): minimum_degree = 512


class BTree(Persistent):
    """
    Instance attributes:
      root: BNode
    """
    def __init__(self, node_constructor=BNode16):
        assert issubclass(node_constructor, BNode)
        self.root = node_constructor()

    def iteritems(self):
        for item in self.root:
            yield item

    def iterkeys(self):
        for item in self.root:
            yield item[0]

    def itervalues(self):
        for item in self.root:
            yield item[1]

    def items(self):
        return list(self.iteritems())

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def __iter__(self):
        for key in self.iterkeys():
            yield key

    def __contains__(self, key):
        return self.root.search(key) is not None

    def __setitem__(self, key, value):
        self.add(key, value)

    def __getitem__(self, key):
        item = self.root.search(key)
        if item is None:
            raise KeyError(key)
        return item[1]

    def __delitem__(self, key):
        self.root.delete(key)

    def get(self, key, default=None):
        """(key:anything, default:anything=None) -> anything
        """
        try:
            return self[key]
        except KeyError:
            return default

    def add(self, key, value=True):
        """(key:anything, value:anything=True)
        Make self[key] == val.
        """
        if self.root.is_full():
            # replace and split.
            node = self.root.__class__()
            node.nodes = [self.root]
            node.split_child(0, node.nodes[0])
            self.root = node
        self.root.insert_item((key, value))

    def get_min_item(self):
        """() -> (key:anything, value:anything)
        Return the item whose key is minimal."""
        return self.root.get_min_item()

    def get_max_item(self):
        """() -> (key:anything, value:anything)
        Return the item whose key is maximal."""
        return self.root.get_max_item()
