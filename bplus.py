import numpy as np


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
        self.keys = np.array([])
        self.values = np.array([])

    def check(self, is_root=False):
        return _check(self.m, len(self.keys), is_root)

    def find(self, key, cmp=lambda x, y: x < y):
        return _find(self, key, cmp)

    def __str__(self):  # debugging string
        to_string = ""
        for i in range(self.m - 1):
            to_string += ":{}".format(' ' if i >= len(self.keys) else self.keys[i])
        to_string += ":"
        return to_string


def _fix_parent(node):
    key = node.keys[0]
    while node.parent is not None and node.pos == -1:
        node = node.parent
    if node.parent is not None:
        # print(node.keys, node.parent.keys, key, node.pos)
        node.parent.keys[node.pos] = key


def _fix_on_sibling(m, node, is_left, is_deleting=False):
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
        sibling.keys = np.insert(sibling.keys, sibling_pos, node.keys[node_pos])
        sibling.values = np.insert(sibling.values, sibling_pos, node.values[node_pos])

        node.keys = np.delete(node.keys, node_pos)
        node.values = np.delete(node.values, node_pos)
        key = node.keys[node_pos]
        _fix_parent(node if is_left else sibling)
        return True
    else:
        return False


class Tree:
    def __init__(self, m=4):
        self.m = m
        self.root = Node(True, m=self.m)  # initially the root is also a leaf

    def __str__(self):  # debugging string
        to_string = "|"
        deque = np.array([self.root, "\n"])
        while len(deque) > 1:
            node = deque[0]
            to_string += str(node) + "|"
            deque = np.delete(deque, 0)
            if type(node) == Node:
                for child in node.values:
                    deque = np.append(deque, child)
            elif node == "\n":
                deque = np.append(deque, "\n")
        return to_string

    def find(self, key, cmp=lambda x, y: x < y):
        return self.root.find(key, cmp)

    def insert(self, key, value, cmp=lambda x, y: x < y, replace=False):
        if len(self.root.keys) == 0:
            self.root.keys = np.array([key, ])
            self.root.values = np.array([value, ])
            return True
        node, pos, bias = self.find(key, cmp)
        if node.values[pos] == key and not replace:
            # Duplication and should not replace
            return False
        # print(node, pos, bias)
        pos += bias
        # update info on the leaf node
        node.keys = np.insert(node.keys, pos, key)
        node.values = np.insert(node.values, pos, value)
        self.fix(node, key)
        return True

    def delete(self, key, cmp=lambda x, y: x < y, del_all=False):
        if len(self.root.keys) == 0:
            return False  # root is empty, cannot delete
        node, pos, bias = self.find(key, cmp)
        if node.keys[pos] != key:
            return False  # cannot find the key specified
        node.keys = np.delete(node.keys, pos)
        node.values = np.delete(node.values, pos)
        flow_status = node.check(id(self.root) == id(node))
        if not flow_status and pos == 0 and len(node.keys) > 0:  # manually peculate up
            _fix_parent(node)
        self.fix(node, key)
        return True

    def fix(self, node, key):
        while True:
            # check the node itself at first
            flow_status = node.check(id(self.root) == id(node))
            if flow_status:
                # node is too big, might needs splitting
                # try left
                if not node.leaf or not _fix_on_sibling(self.m, node, True, flow_status == -1):
                    # try right sibling
                    if not node.leaf or not _fix_on_sibling(self.m, node, False, flow_status == -1):
                        if flow_status == 1:  # overfull, should split
                            # no sibling is ok
                            # TODO: peculate up, splitting
                            # is root
                            if id(self.root) == id(node):
                                self.root = Node(False, m=self.m)
                                self.root.keys = np.array([], dtype=type(key))
                                self.root.values = np.array([node, ], dtype=Node)
                                node.parent = self.root
                                node.pos = -1
                            new = Node(node.leaf, node.parent, node.pos + 1, node, node.right, m=self.m)
                            if new.right is not None:  # update if new.right exists
                                new.right.left = new
                                right = new.right
                                while right is not None:  # update all right siblings since the insertion
                                    right.pos += 1
                                    right = right.right
                            node.right = new
                            # Copy keys
                            new.keys = node.keys[self.m // 2::]
                            node.keys = node.keys[0:self.m // 2]
                            # Copy values along with keys, might be pointers
                            new.values = node.values[self.m // 2 + (0 if node.leaf else 1)::]
                            node.values = node.values[0:self.m // 2 + (0 if node.leaf else 1)]
                            # Update the parent
                            new.parent.keys = np.insert(new.parent.keys, new.pos, new.keys[0])
                            new.parent.values = np.insert(new.parent.values, new.pos + 1, new)

                            # Prepare for the next loop
                            key = new.keys[0]
                            # inner node should not contain the extra node
                            new.keys = new.keys if node.leaf else np.delete(new.keys, 0)
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
                            node.keys = np.concatenate((node.keys, node.right.keys))
                            node.values = np.concatenate((node.values, node.right.values))
                            node.parent.keys = np.delete(node.parent.keys, node.pos + 1)
                            node.parent.values = np.delete(node.parent.values, node.pos + 2)
                            temp = node.right
                            node.right = temp.right
                            del temp
                            node = node.parent
                            if id(node) == id(self.root) and len(node.keys) == 0:
                                self.root = node.values[0]
                                del node
                            continue
            # break if we're not in the most inner if-else block
            break


if __name__ == "__main__":
    def insert_test(t):
        t.insert(0, "Hello, world.")
        t.insert(5, "Hello, again.")
        t.insert(10, "Hello, hi.")
        t.insert(15, "Hello, bad.")
        t.insert(20, "Hello, bad.")
        t.insert(25, "Hello, bad.")
        t.insert(11, "bad, damn")
        t.insert(12, "gotta ya")
        t.insert(13, "gotta ya")
        t.insert(14, "gotta ya")
        t.insert(28, "gotta ya")
        t.insert(29, "gotta ya")
        print(t)


    for i in range(9, 2, -1):
        t = Tree(i)
        insert_test(t)

    t.delete(28)
    print(t)
    t.delete(29)
    print(t)

    t = Tree(5)
    insert_test(t)
    t.delete(20)
    t.delete(25)
    t.delete(28)
    t.delete(29)
    print(t)
