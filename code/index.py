import buffer
from bplus import Tree, SortedList
from exceptions import *


def _check_range(t, keys):
    """
    check whether range is OK
    raise exceptions if not OK
    :param t: the t to check
    :param keys: the key(s) whose range is to be checked
    :raise TreeException: empty tree
    :raise RangeException: range is invalid
    """
    if t.empty:
        raise TreeException("Tree {} is empty".format(id(t)), t)
    elif t.cmp(keys[-1], keys[0]):
        raise RangeException("Start {} is bigger than end {}".format(keys[0], keys[-1]), (keys, t))
    elif t.cmp(keys[-1], t.min[0]):
        raise RangeException("Min {} is bigger than end {}".format(t.min[0], keys[-1]), (keys, t))
    elif t.cmp(t.max[0], keys[0]):
        raise RangeException("Start {} is bigger than end {}".format(keys[0], t.max[0]), (keys, t))


def create_index(ind, data_list, cmp=dummy_cmp, is_primary=False):
    """
    :param ind: the id of the index to be saved to file
    :param data_list: the data, as list, to create index on
    :param cmp: the comparator of the index, defaults to operator<
    :param is_primary: whether we're dealing with primary key, using sorted list
    :return: index of the newly created table
    """
    if is_primary:
        t = SortedList()
    else:
        # TODO: dynamically compute the M value of the B+ tree
        # TODO: what if you're out of memory
        t = Tree(m=50, cmp=cmp)
    for index, data in enumerate(data_list):
        # TODO: what happens if you get an error from the B+ tree
        t.insert(data, index)  # insert data as key and line number as value

    # TODO: what happens if the disk is full and you cannot save the index on disk?
    return buffer.save_index(t, ind)


def drop_index(ind):
    """
    :param ind: the id of the index
    :return: currently nothing is returned
    """
    # TODO: what if buffer cannot handle this deletion
    buffer.delete_index(ind)


def insert(ind, key, value, is_replace=False):
    """
    :param ind: the id of the index
    :param key: the key to insert into the index
    :param value: the value of the B+ tree, probably the line number of the inserted item
    :param is_replace: whether we should replace on duplication
    :raise KeyException: duplication
    """
    # TODO: what if we cannot get what we want
    t = buffer.get_index(ind)

    t.insert(key, value, is_replace)
    # TODO: what if we cannot insert? e.g. out of space

    buffer.save_index(t, ind)


def _operate_single(t, key, is_search):
    """
    :param t: the B+ tree we've got
    :param key: the key to delete
    :param is_search: whether we're doing a search
    :return: searched key
    :raise KeyException: cannot find key
    """
    node, pos, bias = t.find(key)
    if node.keys[pos] == key:
        if is_search:
            return node.values[pos]
        else:
            t.delete(None, node, pos, bias)
            # buffer.save_index(t)
    else:
        raise KeyException("Cannot find key {} in tree {}".format(key, id(t)), (key, t))


def _operate_range(t, keys, is_search, is_current):
    """
    :param t: the tree in consideration
    :param keys: the two keys to be searched, start and end
    :param is_search: whether we're dealing with search operation
    :return: error info if error
    """
    # get information about the beginning and ending
    node_a, pos_a, bias_a = t.find(keys[0])
    node_z, pos_z, bias_z = t.find(keys[-1])
    _ = 1
    # valid range:
    # start <= end
    # start <= max
    # min <= end
    values = []
    _check_range(t, keys)
    if node_a != node_z:
        # handle node_a
        for pos in range(pos_a + int(not is_current), len(node_a.keys)):
            if is_search:
                values.append(node_a.values[pos])
            else:
                t.delete(None, node_a, pos, _)
        # handle nodes between node_a and node_z (not including)
        node = node_a.right
        # if node_a is already node_z, don't do anything
        while node != node_z:
            # delete everything in between onw by one
            for pos in range(0, len(node.keys)):
                if is_search:
                    values.append(node.values[pos])
                else:
                    t.delete(None, node, pos, 0)
            # go to another node
            node = node.right
        # handle node_a
        for pos in range(0, pos_z):
            if is_search:
                values.append(node_z.values[pos])
            else:
                t.delete(None, node_z, pos, _)
    else:
        # handle only this node if start and end is in the same node
        for pos in range(pos_a + int(not is_current), pos_z + 1):
            if is_search:
                values.append(node_a.values[pos])
            else:
                t.delete(None, node_a, pos, _)
    if is_search:
        return values
    # else:
    #     buffer.save_index(t)


def _operate(ind, key, is_search, is_greater, is_current, is_range, is_not_equal=False):
    """
    A thin wrapper around _operate_single and _operate_range
    :param ind: the id of the index to be deleted on
    :param key: the key/keys to be deleted (single or range)
    :param is_search: whether we're searching
    :param is_current: whether we want a single value range search with current node
    :param is_greater: whether we want a single value range search of greater than
    :param is_range: are we searching in range?
    :return: currently nothing is returned
    """
    # assert type(key) == int or len(key) == 2, "The length of the key/keys should be one (single) or two (range)"
    # TODO: what if we cannot get what we want
    t = buffer.get_index(ind)
    if is_range:
        # TODO: clean up buffer operations in _operate_range function
        if is_not_equal:
            if t.cmp(key, t.min[0]) or t.cmp(t.max[2], key):
                return get_values(ind)
            values = _operate(ind, key, is_search, True, False, True)
            values += _operate(ind, key, is_search, False, False, True)
            return values
        if is_greater is not None and is_current is not None:
            if is_greater:
                left = key
                right, _, _ = t.max
            else:
                left, _, _ = t.min
                right = key
            values = _operate_range(t, [left, right], is_search, is_current)
        else:
            values = _operate_range(t, key, is_search, is_current)
    else:
        values = _operate_single(t, key, is_search)
        buffer.save_index(t, ind)
        return values


def search(ind, key, is_greater=None, is_current=None, is_range=False, is_not_equal=False):
    """
    A thin wrapper around _operate
    :param is_current: whether we want a single value range search with current node
    :param is_greater: whether we want a single value range search of greater than
    :param ind: the id of the index to be deleted on
    :param key: the key/keys to be deleted (single or range)
    :param is_range: are we searching in range?
    :return: currently nothing is returned
    """
    return _operate(ind, key, is_search=True, is_greater=is_greater, is_current=is_current, is_range=is_range,
                    is_not_equal=is_not_equal)


def delete(ind, key, is_greater=None, is_current=None, is_range=False, is_not_equal=False):
    """
    A thin wrapper around _operate
    :param is_current: whether we want a single value range search with current node
    :param is_greater: whether we want a single value range search of greater than
    :param ind: the id of the index to be searched on
    :param key: the key/keys to be searched (single or range)
    :return: currently nothing is returned
    """
    return _operate(ind, key, is_search=False, is_greater=None, is_current=None, is_range=is_range,
                    is_not_equal=is_not_equal)


def get_values(ind):
    t = buffer.get_index(ind)
    _, _, node_a = t.min
    _, _, node_z = t.max
    values = []
    if node_a == node_z:
        for pos in range(len(node_a.values)):
            values += node_a.values[pos]
    else:
        count = 0
        # handle node_a
        for pos in range(0, len(node_a.values)):
            values += node_a.values[pos]
            count += 1
        # handle nodes between node_a and node_z (not including)
        node = node_a
        # if node_a is already node_z, don't do anything
        while node != node_z:
            # delete everything in between onw by one
            for pos in range(0, len(node.values)):
                values += node.values[pos]
                count += 1
            # go to another node
            node = node.right
        # handle node_a
        for pos in range(0, len(node_z.values)):
            values += node_z.values[pos]
            count += 1

    return values


def update_values(ind, values):
    """
    updates information about an index
    :param ind:
    :param values:
    :return:
    """
    t = buffer.get_index(ind)
    _, _, node_a = t.min
    _, _, node_z = t.max
    if node_a == node_z:
        for pos in range(len(node_a.values)):
            node_a.values[pos] = values[pos]
    else:
        count = 0
        # handle node_a
        for pos in range(0, len(node_a.values)):
            node_a.values[pos] = values[count]
            count += 1
        # handle nodes between node_a and node_z (not including)
        node = node_a
        # if node_a is already node_z, don't do anything
        while node != node_z:
            # delete everything in between onw by one
            for pos in range(0, len(node.values)):
                node.values[pos] = values[count]
                count += 1
            # go to another node
            node = node.right
        # handle node_a
        for pos in range(0, len(node_z.values)):
            node_z.values[pos] = values[count]
            count += 1
