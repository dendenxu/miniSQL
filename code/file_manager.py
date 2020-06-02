import record_manager
import pickle
import pandas as pd
import os, sys

def save_record(fname, record, posR):
    if(posR == 'end'):
        tmplist = []
        tmplist.append(record)
        df = pd.DataFrame(tmplist)
        df.to_csv(r'.\\data\\' + fname + r'.csv', mode='a', header = False, index = False)
    else:
        df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header = None, encoding="gbk")
        tmplist = record_manager.dataframe_to_list(df)
        tmplist.insert(posR, record)
        newdf = record_manager.list_to_dataframe(tmplist)
        newdf.to_csv(r'.\\data\\' + fname + r'.csv', mode='w', header = False, index = False)

def get_data(fname):
    df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header = None, encoding="gbk")
    return df

def del_data(fname, delInd):
    df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header = None, encoding="gbk")
    tmplist = record_manager.dataframe_to_list(df)
    deldatalist = []
    for i in delInd:
        deldatalist.append(tmplist[i])
    for deldata in deldatalist:
        tmplist.remove(deldata)
    newdf = record_manager.list_to_dataframe(tmplist)
    newdf.to_csv(r'.\\data\\' + fname + r'.csv', mode='w', header = False, index = False)

def create_data_file(fname):
    f = open(r'.\\data\\' + fname + r'.csv', 'w')
    f.close()

def delete_data_file(fname):
    os.remove(r'.\\data\\' + fname + r'.csv')

def clear_data_file(fname):
    f = open(r'.\\data\\' + fname + r'.csv', 'w')
    f.close()
