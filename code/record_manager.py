import file_manager
import pandas as pd
freeList = {}

class Condition:
    def __init__(self, attribute=-1, type = -1, value = None):
        self.attribute = attribute
        self.type = type            # 0:等于 ; 1:小于 ; 2:大于 ; 3:不等于
        self.value = value

def insert_record(fname, record):           # 外部接口
    posR = 'end'
    for k in freeList.keys():
        if k == fname:
            posR = freeList[k][0]
            freeList[k].remove(posR)
    file_manager.save_record(fname, record, posR)

def dataframe_to_list(df):                  # 内部接口
    return df.values.tolist()

def list_to_dataframe(li):                  # 内部接口
    return pd.DataFrame(li)

def searchDF(df, conditionList):            # 内部接口
    for cond in conditionList:
        if len(df) == 0:
            return False
        if cond.type == 0:
            df = df[df[cond.attribute] == cond.value]
        elif cond.type == 1:
            df = df[df[cond.attribute] < cond.value]
        elif cond.type == 2:
            df = df[df[cond.attribute] > cond.value]
        elif cond.type == 3:
            df = df[df[cond.attribute] != cond.value]

def delete_record(fname, conditionList):    # 外部接口
    origdf = file_manager.get_data(fname)
    nowdf = origdf
    state = searchDF(nowdf, conditionList)
    if state == False:
        return 0        # 没有可以删除的记录                      
    delInd = nowdf.index.tolist()
    file_manager.del_data(fname, delInd)
    if fname not in freeList:
        freeList[fname] = []
    for posi in delInd:
        freeList[fname].append(delInd)
    return delInd       # 可能index manager有用 吗

def search_record(fname, conditionList):    # 外部接口
    origdf = file_manager.get_data(fname)
    nowdf = origdf
    state = searchDF(nowdf, conditionList)
    if state == False:
        return 0        # 没有满足条件的记录
    searchlist = dataframe_to_list(nowdf)
    return searchlist

def create_table(fname):
    file_manager.create_data_file(fname)

def delete_table(fname):
    file_manager.delete_data_file(fname)

def clear_table(fname):     # delete from fname
    file_manager.clear_data_file(fname)
