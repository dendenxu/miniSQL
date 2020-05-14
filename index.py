import buffer
from bplus import Tree

"""
API Specification
create_index(table_name,attribute_index)返回index_index
drop_index(index_index)返回行号范围{ind}
insert(index_index,value)
delete(index_index,value)【分为value和range】并返回行号ind【或{ind}】
search_value(index_index,value)或search_range(index_index,val1,val2)返回ind或{ind}
"""

DUPLICATED_KEY = -1
CANNOT_FIND_KEY = -2
START_BIGGER_THAN_END = -3
START_BIGGER_THAN_MAX = -4
MIN_BIGGER_THAN_END = -5


def create_index(data_list, cmp=lambda x, y: x < y):
    """
    :param data_list: the data, as list, to create index on
    :param cmp: the comparator of the index, defaults to operator<
    :return: index of the newly created table
    """
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
    if node.values[pos] == key:
        if not is_replace:
            # Duplication and should not replace
            return DUPLICATED_KEY
        else:
            # TODO: what happens if the delete operation returns false
            # Although we cannot think of a situation for that
            t.delete(key, node, pos, bias)
    # TODO: what if we cannot insert? e.g. out of space
    t.insert(key, value)


def _delete_single(t, key):
    node, pos, bias = t.find(key)
    if node.values[pos] == key:
        t.delete(None, node, pos, bias)
    else:
        return CANNOT_FIND_KEY


def _delete_range(t, keys):
    # get information about the beginning and ending
    node_a, pos_a, bias_a = t.find(keys[0])
    node_z, pos_z, bias_z = t.find(keys[-1])
    _ = 1
    # valid range:
    # start <= end
    # start <= max
    # min <= end
    if t.cmp(keys[-1], keys[0]):
        return START_BIGGER_THAN_END
    elif t.cmp(keys[-1], t.min[0]):
        return MIN_BIGGER_THAN_END
    elif t.cmp(t.max[0], keys[0]):
        return START_BIGGER_THAN_MAX
    else:
        if node_a != node_z:
            # handle node_a
            for pos in range(pos_a, len(node_a.values)):
                t.delete(None, node_a, pos, _)
            # handle nodes between node_a and node_z (not including)
            node = node_a
            # if node_a is already node_z, don't do anything
            while node != node_z:
                # delete everything in between onw by one
                for pos in range(0, len(node.values)):
                    t.delete(None, node, pos, 0)
                # go to another node
                node = node.right
            # handle node_a
            for pos in range(0, pos_z + 1):
                t.delete(None, node_z, pos, _)
        else:
            # handle only this node if start and end is in the same node
            for pos in range(pos_a, pos_z + 1):
                t.delete(None, node_a, pos, _)


def delete(ind, key, is_range=False):
    # TODO: what if we cannot get what we want
    t = buffer.get_index(ind)
    if is_range:
        return _delete_range(t, key)
    else:
        return _delete_single(t, key)
