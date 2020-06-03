import record_manager
import pickle
import pandas as pd
import os, sys
import dill


def save_record(fname, record, posR):
    if (posR == 'end'):
        tmplist = []
        tmplist.append(record)
        df = pd.DataFrame(tmplist)
        df.to_csv(r'.\\data\\' + fname + r'.csv', mode='a', header=False, index=False)
        dfall = pd.read_csv(r'.\\data\\' + fname + r'.csv', header=None, encoding="gbk")
        dflist = record_manager.dataframe_to_list(dfall)
        return len(dflist) - 1
    else:
        df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header=None, encoding="gbk")
        tmplist = record_manager.dataframe_to_list(df)
        tmplist[posR] = record
        newdf = record_manager.list_to_dataframe(tmplist)
        newdf.to_csv(r'.\\data\\' + fname + r'.csv', mode='w', header=False, index=False)
        return posR


def get_data(fname):
    df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header=None, encoding="gbk")
    return df


# def del_data(fname, delInd):
#    df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header = None, encoding="gbk")
#    tmplist = record_manager.dataframe_to_list(df)
#    deldatalist = []
#    for i in delInd:
#        deldatalist.append(tmplist[i])
#    for deldata in deldatalist:
#        tmplist.remove(deldata)
#    newdf = record_manager.list_to_dataframe(tmplist)
#    newdf.to_csv(r'.\\data\\' + fname + r'.csv', mode='w', header = False, index = False)


def create_data_file(fname):
    f = open(r'.\\data\\' + fname + r'.csv', 'w')
    f.close()


def delete_data_file(fname):
    os.remove(r'.\\data\\' + fname + r'.csv')


def clear_data_file(fname):
    f = open(r'.\\data\\' + fname + r'.csv', 'w')
    f.close()


def delete_index_file(Ind):
    os.remove(r".\index\{}.txt".format(Ind))


def save_index_file(Ind, tree):
    f = open(r'.\index\{}.txt'.format(Ind), 'wb')
    pickle.dump(tree, f)
    f.close()


def get_index_file(Ind):
    f = open(r'.\index\{}.txt'.format(Ind), 'rb')
    tree = pickle.load(f)
    f.close()
    return tree


def save_catalog_file(catalogManager):
    f = open(r'.\catalog\meta.txt', 'wb')
    pickle.dump(catalogManager, f)
    f.close()


def get_catalog_file():
    try:
        f = open(r'.\catalog\meta.txt', 'rb+')
    except:
        return None
    tmpcat = pickle.load(f)
    return tmpcat
