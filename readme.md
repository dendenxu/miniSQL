# miniSQL

## File Specification
- `bplus.py` implements B+ tree and Sorted Array for index
- `index.py` implements index operations as Index Manager
- `buffer.py` contains API that the Index Manager requires Buffer Manager to have
- `constants.py` declares constant values in one place for miniSQL, mostly return values
- `readme.md` contains basic information about this project, and I'm `readme.md`
- `.gitignore` contains things git should ignore when updating repository
- `.idea` contains file of PyCharm settings. Shouldn't appear in the repository but it makes PyCharm user happy. Ignore it if you don't use PyCharm

## Index Manager API

### Provides

```python

def create_index(data_list, cmp=lambda x, y: x < y, is_primary=False):
    """
    :param data_list: the data, as list, to create index on
    :param cmp: the comparator of the index, defaults to operator<
    :param is_primary: whether we're dealing with primary key, using sorted list
    :return: index of the newly created table
    """


def drop_index(ind):
    """
    :param ind: the id of the index
    :return: currently nothing is returned
    """


def insert(ind, key, value, is_replace=False):
    """
    :param ind: the id of the index
    :param key: the key to insert into the index
    :param value: the value of the B+ tree, probably the line number of the inserted item
    :param is_replace: whether we should replace on duplication
    :raise KeyException: duplication
    """


def search(ind, key):
    """
    A thin wrapper around _operate
    :param ind: the id of the index to be deleted on
    :param key: the key/keys to be deleted (single or range)
    :return: currently nothing is returned
    """

def delete(ind, key):
    """
    A thin wrapper around _operate
    :param ind: the id of the index to be searched on
    :param key: the key/keys to be searched (single or range)
    :return: currently nothing is returned
    """

def update_values(ind, values):
    """
    updates information about an index
    :param ind:
    :param values:
    :return:
    """
```

### Requires

```python
def save_index(tree):
    """
    :param tree: the tree to be saved to disk or memory buffer
    :return: the index of the saved tree/index
    """
def get_index(ind):
    """
    :param ind: the identifier of the index to be extracted
    :return: the extracted tree/index
    """
def delete_index(ind):
    """
    :param ind: the identifier of the index to be deleted
    :return: nothing yet
    """
```