import record_manager
from buffer import *
import buffer
import pickle
import pandas as pd
import os, sys
import shutil

# 检查data，catalog，index文件夹是否存在
# 若不存在则创建文件夹
def mkdir(path):
    isExists=os.path.exists(path)
    if not isExists:
        os.makedirs(path) 

def initialize_file():
    mkdir("data")
    mkdir("index")
    mkdir("catalog")

def dataframe_to_list(df):  # 内部接口
    return df.values.tolist()

def list_to_dataframe(li):  # 内部接口
    return pd.DataFrame(li)

def insert_new_record(fname, record):
    tmplist = []
    tmplist.append(record)
    df = pd.DataFrame(tmplist)
    df.to_csv(r'.\\data\\' + fname + r'.csv', mode='a', header=False, index=False)


def write_back(data_block):
    fname = data_block.tablename
    blocknum = data_block.position
    startpos = blocknum * 256
    recordnum = len(data_block.content)
    df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header=None, encoding="gbk", engine='python')
    tmpList = dataframe_to_list(df)
    for i in range(recordnum):
        tmpList[startpos+i] = data_block.content[i]
    newdf = list_to_dataframe(tmpList)
    newdf.to_csv(r'.\\data\\' + fname + r'.csv', mode='w', header=False, index=False)


def get_block_data(fname, blockpos):
    startpos = blockpos * 256
    df = pd.read_csv(r'.\\data\\' + fname + r'.csv', header=None, encoding="gbk", engine='python')
    tmpList = dataframe_to_list(df)   
    length = len(tmpList)
    dataList = []
    for i in range(256):
        if startpos+i >= length:
            break
        dataList.append(tmpList[startpos+i])
    return dataList


def create_data_file(fname):
    f = open(r'.\\data\\' + fname + r'.csv', 'w')
    f.close()


def delete_data_file(fname):
    os.remove(r'.\\data\\' + fname + r'.csv')


def clear_data_file(fname):
    f = open(r'.\\data\\' + fname + r'.csv', 'w')
    f.close()
    

#def delete_index_file(Ind):
#    os.remove(r".\index\{}.txt".format(Ind))

  
def save_index_file(index_buffer):
    isExists=os.path.exists(r'.\index')
    if isExists == True:
        shutil.rmtree(r'.\index')
    mkdir(r'.\index')
    for Ind in index_buffer.keys():
        f = open(r'.\index\{}.txt'.format(Ind), 'wb')
        pickle.dump(index_buffer[Ind], f)
        f.close()


def get_index_file():
    index_buffer = {}
    listind = os.listdir(r'.\index')
    count = 0
    while count < len(listind):
        isExists=os.path.exists(r'.\index\{}.txt'.format(count))
        if isExists == True:
            f = open(r'.\index\{}.txt'.format(count), 'rb')
            tree = pickle.load(f)
            index_buffer[count] = tree
            f.close()
            count += 1
    return index_buffer


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
    f.close()
    return tmpcat

def save_freeList(freeList):
    f = open(r'.\data\freeList.txt', 'wb')
    pickle.dump(freeList, f)
    f.close()

def get_freeList():
    freeList = {}
    isExists = os.path.exists(r'.\data\freeList.txt')
    if isExists == True:
        f = open(r'.\data\freeList.txt', 'rb')
        freeList = pickle.load(f)
        f.close()
    return freeList

#def save_data_buffer():
#    f = open(r'.\data\dataBuffer.txt', 'wb')
#    pickle.dump(data_buffer, f)
#    f.close()

#def get_data_buffer():
#    isExists = os.path.exists(r'.\data\dataBuffer.txt')
#    if isExists == True:
#        f = open(r'.\data\dataBuffer.txt', 'rb')
#        data_buffer = pickle.load(f)
#        f.close()

def save_maxrecordNum(maxrecordNum):
    f = open(r'.\data\maxrecordNum.txt', 'wb')
    pickle.dump(maxrecordNum, f)
    f.close()

def get_maxrecordNum():
    maxrecordNum = {}
    isExists = os.path.exists(r'.\data\maxrecordNum.txt')
    if isExists == True:
        f = open(r'.\data\maxrecordNum.txt', 'rb')
        maxrecordNum = pickle.load(f)
        f.close()
    return maxrecordNum