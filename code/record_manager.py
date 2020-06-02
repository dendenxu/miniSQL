import file_manager
import pandas as pd
freeList = {}

class Condition:
    def __init__(self, attribute=-1, type = -1, value = None):
        self.attribute = attribute
        self.type = type            # 0:等于 ; 1:小于 ; 2:大于 ; 3:不等于 ; 4:小于等于 ; 5:大于等于
        self.value = value

def insert_record(fname, record):           # 外部接口
    posR = 'end'
    for k in freeList.keys():
        if k == fname:
            posR = freeList[k][0]
            freeList[k].remove(posR)
    newposR = file_manager.save_record(fname, record, posR)
    return newposR

def dataframe_to_list(df):                  # 内部接口
    return df.values.tolist()

def list_to_dataframe(li):                  # 内部接口
    return pd.DataFrame(li)

# 当没有index时的查询方法
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
        elif cond.type == 4:
            df = df[df[cond.attribute] <= cond.value]
        elif cond.type == 5:
            df = df[df[cond.attribute] >= cond.value]

def search_record(fname, conditionList):    # 内部接口
    origdf = file_manager.get_data(fname)
    nowdf = origdf
    state = searchDF(nowdf, conditionList)
    if state == False:
        return 0        # 没有找到符合条件的记录                      
    findInd = nowdf.index.tolist()
    return findInd       

def search_record_with_Index(fname, indList, conditionList):    # 内部接口  for delete and select
    findInd = search_record(fname, conditionList)
    if findInd == 0 or len(indList) == 0:
        return 0
    finalInd = []
    for i in indList:
        for k in i:
            for j in findInd:
                if k == j:
                    finalInd.append(i)
    if len(finalInd) == 0:
        return 0
        
    return finalInd           # for delete and select

def delete_record_with_Index(fname, indList, conditionList):            # 外部接口 for delete
    delInd = search_record_with_Index(fname, indList, conditionList)
    if fname not in freeList:
        freeList[fname] = []
        compareList = []
    else:
        compareList = freeList[fname]
    finaldelInd = []
    for i in delInd:
        if i not in compareList:
            finaldelInd.append(i)
    if len(finaldelInd) == 0:
        return 0            # 不用删除
    for posi in finaldelInd:
        freeList[fname].append(posi)
    return finaldelInd

def select_record_with_Index(fname, indList, conditionList):            # 外部接口 for select
    selInd = search_record_with_Index(fname, indList, conditionList)
    df = file_manager.get_data(fname)
    selList = dataframe_to_list(df)
    finalselList = []
    for i in selInd:
        finalselList.append(selList[i])
    return finalselList

def create_table(fname):
    file_manager.create_data_file(fname)

def delete_table(fname):
    file_manager.delete_data_file(fname)

def clear_table(fname):     # delete from fname
    file_manager.clear_data_file(fname)
