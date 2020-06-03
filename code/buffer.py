import file_manager


def get_index(Ind):
    return file_manager.get_index_file(Ind)


def save_index(tree, Ind):
    file_manager.save_index_file(Ind, tree)


def delete_index(Ind):
    file_manager.delete_index_file(Ind)
