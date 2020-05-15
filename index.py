import buffer
from bplus import Tree, SortedList
from constants import *

"""
API Specification
create_index(table_name,attribute_index)返回index_index
drop_index(index_index)返回行号范围{ind}
insert(index_index,value)
delete(index_index,value)【分为value和range】并返回行号ind【或{ind}】
search_value(index_index,value)或search_range(index_index,val1,val2)返回ind或{ind}
"""


def _check_range(t, keys):
    if t.empty:
        return EMPTY_TREE
    elif t.cmp(keys[-1], keys[0]):
        return START_BIGGER_THAN_END
    elif t.cmp(keys[-1], t.min[0]):
        return MIN_BIGGER_THAN_END
    elif t.cmp(t.max[0], keys[0]):
        return START_BIGGER_THAN_MAX
    else:
        return RANGE_IS_OK


def create_index(data_list, cmp=lambda x, y: x < y, is_primary=False):
    """
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
        t = Tree(m=6, cmp=cmp)
    for data, index in enumerate(data_list):
        # TODO: what happens if you get an error from the B+ tree
        t.insert(data, index)  # insert data as key and line number as value

    # TODO: what happens if the disk is full and you cannot save the index on disk?
    return buffer.save_index(t)


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
    :return: the inserted position of the new key, probably the last of the whole table
    """
    # TODO: what if we cannot get what we want
    t = buffer.get_index(ind)
    node, pos, bias = t.find(key)
    if node.keys[pos] == key:
        if not is_replace:
            # Duplication and should not replace
            return DUPLICATED_KEY
        else:
            # TODO: what happens if the delete operation returns false
            # Although we cannot think of a situation for that
            t.delete(key, node, pos, bias)
    # TODO: what if we cannot insert? e.g. out of space
    t.insert(key, value)
    return buffer.save_index(t)


def _operate_single(t, key, is_search):
    """
    :param t: the B+ tree we've got
    :param key: the key to delete
    :param is_search: whether we're doing a search
    :return: information if deletion failed
    """
    node, pos, bias = t.find(key)
    if node.keys[pos] == key:
        if is_search:
            return node.values[pos]
        else:
            t.delete(None, node, pos, bias)
            return buffer.save_index(t)
    else:
        return CANNOT_FIND_KEY


def _operate_range(t, keys, is_search):
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
    range_check_result = _check_range(t, keys)
    if range_check_result != RANGE_IS_OK:
        return range_check_result
    else:
        if node_a != node_z:
            # handle node_a
            for pos in range(pos_a, len(node_a.keys)):
                if is_search:
                    values.append(node_a.values[pos])
                else:
                    t.delete(None, node_a, pos, _)
            # handle nodes between node_a and node_z (not including)
            node = node_a
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
            for pos in range(0, pos_z + 1):
                if is_search:
                    values.append(node_z.values[pos])
                else:
                    t.delete(None, node_z, pos, _)
        else:
            # handle only this node if start and end is in the same node
            for pos in range(pos_a, pos_z + 1):
                if is_search:
                    values.append(node_a.values[pos])
                else:
                    t.delete(None, node_a, pos, _)
    if is_search:
        return values
    else:
        return buffer.save_index(t)


def _operate(ind, key, is_search):
    """
    A thin wrapper around _operate_single and _operate_range
    :param ind: the id of the index to be deleted on
    :param key: the key/keys to be deleted (single or range)
    :param is_search: whether we're searching
    :return: currently nothing is returned
    """
    assert len(key) == 2 or len(key) == 1, "The length of the key/keys should be one (single) or two (range)"
    # TODO: what if we cannot get what we want
    t = buffer.get_index(ind)
    if len(key) == 2:
        return _operate_range(t, key, is_search)
    else:
        return _operate_single(t, key, is_search)


def search(ind, key):
    """
    A thin wrapper around _operate
    :param ind: the id of the index to be deleted on
    :param key: the key/keys to be deleted (single or range)
    :return: currently nothing is returned
    """
    return _operate_range(ind, key, is_search=True)


def delete(ind, key):
    """
    A thin wrapper around _operate
    :param ind: the id of the index to be searched on
    :param key: the key/keys to be searched (single or range)
    :return: currently nothing is returned
    """
    return _operate_range(ind, key, is_search=False)


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
