import buffer
from buffer import *

class Condition:
    def __init__(self, attribute=-1, type=-1, value=None):
        self.attribute = attribute
        self.type = type  # 0:等于 ; 1:小于 ; 2:大于 ; 3:不等于 ; 4:小于等于 ; 5:大于等于
        self.value = value


def insert_record(fname, record):  # 外部接口
    newposR = data_buffer.insert_record(fname, record)      # data_buffer 是一个DataBuffer类的对象
    return newposR


# indList == [] 时，表示index没找到东西
# indList == 0  时，表示未通过index查找
def check_fit(fname, pos, record, indList, conditionList):           # 内部接口
    flag = True
    #if (type(indList) != int and pos not in indList) or (pos in freeList[fname]):
    #    return False        # 无需删除或没有选中 返回后处理
    if pos in buffer.freeList[fname]:
        return False
    if type(indList) != int:
        for tmplist in indList:
            if pos not in tmplist:
                return False
    for cond in conditionList:
        if cond.type == 0:      # ==
            if record[cond.attribute] != cond.value:
                flag = False
                break
        elif cond.type == 1:    # <
            if record[cond.attribute] >= cond.value:
                flag = False
                break
        elif cond.type == 2:    # >
            if record[cond.attribute] <= cond.value:
                flag = False
                break
        elif cond.type == 3:    # <>
            if record[cond.attribute] == cond.value:
                flag = False
                break
        elif cond.type == 4:    # <=
            if record[cond.attribute] > cond.value:
                flag = False
                break
        elif cond.type == 5:    # >=
            if record[cond.attribute] < cond.value:
                flag = False
                break
    return flag

def delete_record_with_Index(fname, indList, conditionList):  # 外部接口 for delete
    if indList != 0 and len(indList) == 0:
        return 0        # 没有要删除的元素（算执行成功还是算Exception？）
    blockList = data_buffer.get_blocks(fname)
    delInd = []
    for blocki in blockList:
        for i in range(len(blocki.content)):
            posR = blocki.position*256 + i
            flag = check_fit(fname, posR, blocki.content[i], indList, conditionList)
            if flag == True:
                delInd.append(posR)
                buffer.freeList[fname].append(posR)
    return delInd

def select_record_with_Index(fname, indList, conditionList):  # 外部接口 for select
    if indList != 0 and len(indList) == 0:
        return 0        # 没有被选中的元素（算执行成功还是算Exception？）
    blockList = data_buffer.get_blocks(fname)
    selList = []
    for blocki in blockList:
        for i in range(len(blocki.content)):
            posR = blocki.position*256 + i
            flag = check_fit(fname, posR, blocki.content[i], indList, conditionList)
            if flag == True:
                #print(blocki.content[i])
                selList.append(blocki.content[i])
    return selList


def create_table(fname):
    data_buffer.create_table(fname)

def delete_table(fname):
    data_buffer.delete_table(fname)

def clear_table(fname):  # delete from fname
    data_buffer.clear_table(fname)
