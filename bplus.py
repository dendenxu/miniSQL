"""
Theoretically this implementation of B+ tree in Python
should support any data type in python
to avoid introducing complexity and to program natively
we don't import 3rd party package to this implementation of B+ tree

However if we're to optimize for operations on large trunk of data
we can import numpy as np as change data structures to np.array

Moreover there's implementation of BPlusTree in python already, if we're to be lazy
we can simply pip install bplustree
and from bplustree import BPlusTree
"""
from exceptions import *


def search(items, key, cmp=lambda x, y: x < y):
    """
    binary search
    :param iterable items: the iterable to perform bin_search on
    :param key: same type as items in vs
    :param cmp: a comparator function
    :return int: index of key if found, index of lower_bound if not
    """
    left = 0
    right = len(items)
    while True:
        mid = (left + right) // 2
        if left >= right - 1:
            break
        if cmp(key, items[mid]):
            right = mid
        else:
            left = mid
    return mid


def _check(m, n, is_root=False):
    """
    Check whether a node is over-full or under-full, needs splitting or merging

    :param m:
    :param n:
    :param is_root:
    :return:
    """
    if not is_root:  # we treat leaf node as inner node
        # size of v should be ceil(m//2)-1 to m-1
        # size of k should be ceil(m//2) to m
        # we assume size of k to be size of v+1
        return 1 if (m - 1 < n) else (-1 if (n < m // 2 - (not m % 2)) else 0)
    else:
        # size of v should be 1 to m-1
        # size of k should be 0
        return 0 if m - 1 >= n else 1


def _find(node, key, cmp=lambda x, y: x < y):
    """
    find an element by key beginning from node, defined as function to be called recursively

    :param Node node: the node to begin the find process with
    :param comparable object key: the key we're trying to find
    :param comparator function cmp: the comparator function, takes two args and return True if arg1 < arg2
    :return: (node, pos, bias) if we're at the leaf node of the finding process, else recursively call _find again
    """
    pos = search(node.keys, key, cmp)  # calls binary search function
    bias = 0 if pos == 0 and key < node.keys[0] else 1

    # return zipped info if at leaf
    # return next node if not at leaf
    return (node, pos, bias) if node.leaf else _find(node.values[pos + bias], key, cmp)


class Node:
    def __init__(self, leaf=False, parent=None, pos=None, left=None, right=None, m=4):
        self.m = m  # maximum number of child as an inner node
        self.leaf = leaf  # bool, whether this is a leaf node
        self.parent = parent  # pointer to parent
        self.pos = pos  # current position at self.parent.keys, beginning at -1
        self.left = left  # pointer to left node
        self.right = right  # pointer to right node
        self.keys = []
        self.values = []

    def check(self, is_root=False):
        """
        Check whether the node itself is full, calls _check

        :param is_root:
        :return:
        """
        return _check(self.m, len(self.keys), is_root)

    def find(self, key, cmp=lambda x, y: x < y):
        """
        Find lower bound of an element, beginning from this node, calls _find

        :param key:
        :param cmp:
        :return:
        """
        return _find(self, key, cmp)

    def __str__(self):  # debugging string
        """
        Convert the Node to string for output (used in debugging)

        :return: the converted string
        """
        to_string = ""
        for i in range(self.m - 1):  # we've limited the range to self.m - 1, so the over-full node won't be printed
            to_string += ":{}".format(' ' if i >= len(self.keys) else self.keys[i])
        to_string += ":"
        return to_string

    @property
    def empty(self):
        return len(self.values) == 0

    @property
    def min(self):
        return self.keys[0], self.values[0], self

    @property
    def max(self):
        return self.keys[-1], self.values[-1], self


def _fix_parent(node):
    """
    Get the right position to update and update the parent of a node
    when node's position in parent is -1, go up, else update and abort

    :param node: the node whose parent needs updating
    :return:
    """
    key = node.keys[0]
    while node.parent is not None and node.pos == -1:
        node = node.parent
    if node.parent is not None:
        # print(node.keys, node.parent.keys, key, node.pos)
        node.parent.keys[node.pos] = key


def _fix_on_sibling(m, node, is_left, is_deleting=False):
    """
    The node is over/under full, borrowing/sending key-value parts to a valid sibling
    if no sibling is valid, return False so that caller can split or merge node(s)

    :param m:
    :param node:
    :param is_left:
    :param is_deleting:
    :return:
    """
    sibling = node.left if is_left else node.right
    is_left = not is_left if is_deleting else is_left
    temp = node
    node = sibling if is_deleting else node
    sibling = temp if is_deleting else sibling
    if sibling is not None and node is not None and not _check(m, (len(node.keys) - 1) if is_deleting else (
            len(sibling.keys) + 1)):
        # sibling node is good for sharing
        # TODO: move the left/right most node around

        sibling_pos = len(sibling.keys) if is_left else 0
        node_pos = 0 if is_left else -1
        sibling.keys.insert(sibling_pos, node.keys[node_pos])
        sibling.values.insert(sibling_pos, node.values[node_pos])

        del node.keys[node_pos]
        del node.values[node_pos]
        _fix_parent(node if is_left else sibling)
        return True
    else:
        return False


class Tree:
    """
    We use the property of storing only memory address of Object of Python
    for example, if Object() is initialized to variable a
    and we assign variable a to b
    b won't be copying full content of a in memory
    instead we'd store pointer to a in b

    This feature enables us to implement tree like data structure in Python quite easily
    """

    def __init__(self, m=4, cmp=lambda x, y: x < y, is_replace=False):
        self.m = m
        self.cmp = cmp
        self.root = Node(True, m=self.m)  # initially the root is also a leaf

    def __str__(self):  # debugging string
        """
        Convert the Node to string for output (used in debugging)
        :return: the converted string
        """
        to_string = "|"
        deque = [self.root, "\n"]
        # Level order traversal of a B+ tree
        while len(deque) > 1:
            node = deque[0]
            to_string += str(node) + "|"  # calls __str__ of a node
            del deque[0]
            if type(node) == Node:
                for child in node.values:
                    deque.append(child)
            elif node == "\n":
                deque.append("\n")
        return to_string

    @property
    def empty(self):
        return len(self.root.values) == 0

    @property
    def min(self):
        if self.empty:
            raise TreeException("Tree {} is empty".format(id(self)), self)
        node = self.root
        while not node.leaf:
            node = node.min[1]
        return node.min

    @property
    def max(self):
        if self.empty:
            raise TreeException("Tree {} is empty".format(id(self)), self)
        node = self.root
        while not node.leaf:
            node = node.max[1]
        return node.max

    def find(self, key):
        """
        calls find on root node

        :param key: the key we're trying to find
        :return:
        """
        return self.root.find(key, self.cmp)

    def insert(self, key, value):
        """
        insert a key-value pair into the B+ tree
        might need splitting or fixing on siblings

        :param key: the key to be inserted, used in find
        :param value: the value, usually a pointer or line number
        :return:
        """
        # A tree might be empty
        if self.root.empty:
            self.root.keys = [key, ]
            self.root.values = [value, ]
            return True

        # Trying finding the key
        node, pos, bias = self.find(key)
        if node.keys[pos] == key:
            # Duplication and should not replace
            raise KeyException("Duplicated key {} in tree {}".format(key, id(self)), (key, self))
        # print(node, pos, bias)
        pos += bias
        # update info on the leaf node
        node.keys.insert(pos, key)
        node.values.insert(pos, value)
        self._fix(node)
        return True

    def delete(self, key, node=None, pos=None, bias=None):
        """
        Delete a key value pair
        can specify the position by hand or let the deleter find it for you

        :param key: the key to delete
        :param node: the position to delete, None by default
        :param pos: the position to delete, None by default
        :param bias: the position to delete, None by default
        :return:
        """
        # Whole tree might be empty
        if self.root.empty:
            raise TreeException("Tree {} is empty".format(id(self)), self)  # root is empty, cannot delete
        # find the key if not already specified
        if node is None or pos is None or bias is None:
            node, pos, bias = self.find(key)
        # is the key matched with the position
        if node.keys[pos] != key:
            raise KeyException("Cannot find key {} in tree {}".format(key, id(self)), (key, self))
            # cannot find the key specified
        del node.keys[pos]
        del node.values[pos]
        flow_status = node.check(id(self.root) == id(node))
        if not flow_status and pos == 0 and len(node.keys) > 0:  # manually peculate up
            _fix_parent(node)
        self._fix(node)
        return True

    def _fix(self, node):
        """
        Fix a node if it's over-full or under-full, iteratively peculate up to fix the node's parent
        uses _check to determine we're deleting or inserting

        :param node: the node we should try to fix
        :return:
        """
        while True:
            # check the node itself at first
            # print(self)
            flow_status = node.check(id(self.root) == id(node))
            if not flow_status:
                break
                # node is too big, might needs splitting
                # try left
            if node.leaf and _fix_on_sibling(self.m, node, True, flow_status == -1):
                # try right sibling
                break
            if node.leaf and _fix_on_sibling(self.m, node, False, flow_status == -1):
                break
            if flow_status == 1:  # overfull, should split
                # no sibling is ok
                # is root
                if id(self.root) == id(node):
                    self.root = Node(False, m=self.m)  # initialize the new root node
                    self.root.keys = []  # initialize keys
                    self.root.values = [node, ]  # initialize values
                    node.parent = self.root  # update parent of current node
                    node.pos = -1  # position begins from -1

                # new node is the right node to be split to
                new = Node(node.leaf, node.parent, node.pos + 1, node, node.right, m=self.m)
                if new.right is not None:  # update if new.right exists
                    new.right.left = new
                    right = new.right
                    while right is not None:  # update all right siblings since the insertion
                        right.pos += 1  # this might should keep consistency of information
                        right = right.right

                # update left right pointer
                node.right = new
                # Copy keys
                new.keys = node.keys[self.m // 2::]
                node.keys = node.keys[0:self.m // 2]
                # Copy values along with keys, might be pointers
                new.values = node.values[self.m // 2 + (0 if node.leaf else 1)::]
                node.values = node.values[0:self.m // 2 + (0 if node.leaf else 1)]
                # Update the parent
                new.parent.keys.insert(new.pos, new.keys[0])
                new.parent.values.insert(new.pos + 1, new)

                # inner node should not contain the extra node
                if not node.leaf:
                    del new.keys[0]
                # update children's parent if is not leaf node
                if not new.leaf:
                    for index, child in enumerate(new.values):
                        child.parent = new
                        child.pos = index - 1
                node = new.parent
                continue
            else:  # under-flow, should pack up
                # we've already checked left and right, no extra node too share, safe to pack
                if node.right is None:
                    node = node.left
                node.keys += node.right.keys
                node.values += node.right.values
                del node.parent.keys[node.pos + 1]
                del node.parent.values[node.pos + 2]
                temp = node.right
                node.right = temp.right
                del temp
                node = node.parent
                if id(node) == id(self.root) and len(node.keys) == 0:
                    self.root = node.values[0]
                    del node
                continue


class PositionList:
    """
    This is a data structure that
    returns the position when trying to retrieve information at a certain position
    """

    def __getitem__(self, item):
        return item


class SortedList:
    """
    This should provide similar interface as B+ tree
    It should only contain values, but not key
    """

    def __init__(self, cmp=lambda x, y: x < y, replace=False):
        self.cmp = cmp
        self.root = Node()  # a single node as list
        self.root.values = PositionList()  # should return 1 if you call root.values[1]

    def find(self, key):
        return self.root.find(key, self.cmp)

    def insert(self, key, is_replace=False):
        if self.root.empty:
            self.root.keys = [key, ]
            return True
        node, pos, _ = self.find(key)
        if node.keys[pos] == key and not is_replace:
            # Duplication and should not replace
            raise KeyException("Duplicated key {} in SortedList {}".format(key, id(self)), (key, self))
        # update info on the leaf node
        node.keys.insert(pos, key)
        return True

    def delete(self, key, node=None, pos=None, bias=None):
        if self.root.empty:
            raise TreeException("SortedList {} is empty".format(id(self)), (self))  # root is empty, cannot delete
        if node is None or pos is None or bias is None:
            node, pos, bias = self.find(key)
        if node.keys[pos] != key:
            raise KeyException("Cannot find key {} in SortedList {}".format(key, id(self)), (key, self))
            # cannot find the key specified
        del node.keys[pos]
        return True


if __name__ == "__main__":
    def insert_test_int(tree):
        tree.insert(0, "Hello, world.")
        tree.insert(5, "Hello, again.")
        tree.insert(10, "Hello, hi.")
        # print(t)
        tree.insert(15, "Hello, bad.")
        tree.insert(20, "Hello, bad.")
        tree.insert(25, "Hello, bad.")
        tree.insert(11, "bad, damn")
        tree.insert(12, "gotta ya")
        tree.insert(13, "gotta ya")
        tree.insert(14, "gotta ya")
        tree.insert(28, "gotta ya")
        try:
            tree.insert(29, "gotta ya")
            tree.insert(29, "gotta ya")
            tree.insert(29, "gotta ya")
        except KeyException as e:
            print(e)
        print(tree)

    def insert_test_str(tree):
        tree.insert("0", "Hello, world.")
        tree.insert("5", "Hello, again.")
        tree.insert("10", "Hello, hi.")
        tree.insert("15", "Hello, bad.")
        tree.insert("20", "Hello, bad.")
        tree.insert("25", "Hello, bad.")
        tree.insert("11", "bad, damn")
        tree.insert("12", "gotta ya")
        tree.insert("13", "gotta ya")
        tree.insert("14", "gotta ya")
        tree.insert("28", "gotta ya")
        try:
            tree.insert("29", "gotta ya")
            tree.insert("29", "gotta ya")
            tree.insert("29", "gotta ya")
        except KeyException as e:
            print(e)
        print(tree)

    for m_value in range(9, 2, -1):
        print("Currently tesing tree with m={}".format(m_value))
        t = Tree(m_value)
        insert_test_int(t)
    # don't try this, since when m = 2, some of the insertion splitting case is undefine
    # for example when you've got |:5:|\n|:0:|:5:| and you wanna insert 10 into this
    # |:5:|\n|:0:|:5:10:| should split, getting |:5:10:|\n|:0:|:5:|:10:|
    # and this should be split too, getting |:10:|\n|:5:|:10:|\n|:0:|:5:|:10:| and 10 should be deleted here!
    # t = Tree(2)
    # insert_test(t)
    t = Tree(3)
    insert_test_int(t)
    t.delete(28)
    print(t)
    t.delete(29)
    print(t)

    t = Tree(5)
    insert_test_int(t)
    t.delete(20)
    t.delete(25)
    t.delete(28)
    t.delete(29)
    print(t)

    t = Tree(4)
    insert_test_str(t)

    print(t)

    print(t.min)
    print(t.max)
