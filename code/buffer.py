import time
import file_manager

MAX_BLOCK_NUM = 2048
MAX_TIME = 10000000000

freeList = {}
index_buffer = {}
maxrecordNum = {}

class DataBlock:
    def __init__(self, pin = False, dirty = False, tname = "", pos = -1, content = [], time = MAX_TIME):
        self.pin = pin
        self.dirty = dirty
        self.tablename = tname
        self.position = pos
        self.content = content
        self.LAT = time                         # Last Access Time

class DataBuffer:
    def __init__(self):
        self.buffer = list()
        self.blockNum = 0
        #self.maxrecordNum = dict()              # 当前最大的record序号（包括已删除的记录）

    def LRU(self):
        accesstime = MAX_TIME
        minindex = -1
        for i in range(len(self.buffer)):
            if self.buffer[i].pin == False and self.buffer[i].LAT < accesstime:
                accesstime = self.buffer[i].LAT
                minindex = i
        LATblock = self.buffer[i]
        file_manager.write_back(LATblock)
        self.buffer.remove(LATblock)

    ''' 
    首先判断该表在 freeList 中是否有值
        若有值，有 2 种情况     （把该位置在 freeList 中删除）
            1)  block 在 buffer 内
                插入 buffer 中的 block
                # 无需调整 maxrecordNum 和 blockNum
            2)  block 在 buffer 外，文件(磁盘)内
                判断是否 buffer 中 block数 已满
                    若已满
                        LRU 写回，blockNum -= 1
                    通过file_manager取出需要的 block，插入记录并放入 buffer，blockNum += 1 (已满或未满都要)
        若在 freeList 中没有值，即表示要在最后新增记录        (注意：此时必须写入文件中!!!!!!!!
            首先，计算出所在 block 的位置和在当前 block 的第几条
            maxrecordNum += 1
            1)  若要加在新增的 block 内
                    若 buffer 已满
                        LRU 写回，blockNum -= 1
                    创建新 block 类，第一条记录就是要插入的记录，加入到 buffer 中，blockNum += 1 (已满或未满都要)
            2)  若加在已有的 block 内
                    若在 buffer 中找不到
                        若 buffer 已满
                            LRU 写回，blockNum -= 1
                        通过file_manager取出并插入记录，放入buffer，blockNum += 1
                    反之，直接插入即可
    '''
    def insert_record(self, tname, record):         # 返回位置: posB*256 + posR
        if tname in freeList.keys():
            if len(freeList[tname]) != 0:
                positionR = freeList[tname][0]
                freeList[tname].remove(freeList[tname][0])
                blockpos  = positionR // 256
                recordpos = positionR %  256
                insertFlag = False
                for i in range(len(self.buffer)):
                    if self.buffer[i].tablename == tname and self.buffer[i].position == blockpos:
                        self.buffer[i].content[recordpos] = record
                        self.buffer[i].LAT = time.time()
                        self.buffer[i].dirty = True
                        insertFlag = True
                        return blockpos*256 + recordpos
                if insertFlag == False:
                    if self.blockNum == MAX_BLOCK_NUM:
                        LRU()
                        self.blockNum -= 1
                    blockcontent = file_manager.get_block_data(tname, blockpos)
                    if len(blockcontent) > recordpos:
                        blockcontent[recordpos] = record
                    else:
                        blockcontent.append(record)
                    tmpBlock = DataBlock(False, True, tname, blockpos, blockcontent, time.time())
                    self.buffer.append(tmpBlock)
                    self.blockNum += 1
                    return blockpos*256 + recordpos
    
        blockpos  = maxrecordNum[tname] // 256
        recordpos = maxrecordNum[tname] %  256
        #print(blockpos)
        maxrecordNum[tname] += 1   
        file_manager.insert_new_record(tname, record)       # 必须写入文件，以便之后的取操作
        if recordpos == 0:  
            if self.blockNum == MAX_BLOCK_NUM:
                LRU()
                self.blockNum -= 1
            tmpBlock = DataBlock(False, False, tname, blockpos, [record], time.time())
            #print(tmpBlock.position)
            self.buffer.append(tmpBlock)
            self.blockNum += 1
        else:
            insertFlag = False
            for i in range(len(self.buffer)):
                if self.buffer[i].tablename == tname and self.buffer[i].position == blockpos:
                    self.buffer[i].content.append(record)
                    self.buffer[i].LAT = time.time()
                    self.buffer[i].dirty = False
                    insertFlag = True
                    return blockpos*256 + recordpos
            if insertFlag == False:
                if self.blockNum == MAX_BLOCK_NUM:
                    LRU()
                    self.blockNum -= 1
                blockcontent = file_manager.get_block_data(tname, blockpos)
                #blockcontent.append(record)   新record已经在文件中
                tmpBlock = DataBlock(False, False, tname, blockpos, blockcontent, time.time())
                self.buffer.append(tmpBlock)
                self.blockNum += 1
                return blockpos*256 + recordpos
        
    def LRU_except_one(self, tname):
        accesstime = MAX_TIME
        minindex = -1
        for i in range(len(self.buffer)):
            if self.buffer[i].pin == False and self.buffer[i].LAT < accesstime and self.buffer[i].tablename != tname:
                accesstime = self.buffer[i].LAT
                minindex = i
        LATblock = self.buffer[i]
        file_manager.write_back(LATblock)
        self.buffer.remove(LATblock)

    '''
        首先计算该表的page数n，构建0-n-1的set
        在buffer中找该表已经存在的块序号，在set中减去
        记set中还剩的元素个数即该表未在buffer中的元素个数为m
                      MAX_BLOCK_NUM - self.blockNum = k
        若 k>=m   则直接取入即可
        若 k<m    则需要LRU m-k 个块，再取入m个块(按set的序号)

        返回这个块的所有块列表

    '''
    def get_blocks(self, tname):            # 获取某一个表的全部block信息
        #print(maxrecordNum)
        numberOfBlocks = maxrecordNum[tname]//256
        if maxrecordNum[tname]%256 != 0:
            numberOfBlocks += 1
        getList = list()
        returnList = list()
        for i in range(numberOfBlocks):
            getList.append(i)
        #print(getList)
        for i in range(len(self.buffer)):
            if self.buffer[i].tablename == tname:
                #print(self.buffer[i].position)
                self.buffer[i].LAT = time.time()
                getList.remove(self.buffer[i].position)
                returnList.append(self.buffer[i])
        m = len(getList)
        k = MAX_BLOCK_NUM - self.blockNum
        if k>=m:
            for blockposi in getList:
                tmpContent = file_manager.get_block_data(tname, blockposi)
                tmpBlock = DataBlock(False, False, tname, blockposi, tmpContent, time.time())    
                returnList.append(tmpBlock)         # 注意是浅拷贝，参数引用传递，因此上一句一直需要重新申请分配空间
                self.buffer.append(tmpBlock)
            self.blockNum += m
        else:
            for i in range(m-k):
                LRU_except_one(self, tname)
            self.blockNum = MAX_BLOCK_NUM
            for blockposi in getList:
                tmpContent = file_manager.get_block_data(tname, blockposi)
                tmpBlock = DataBlock(False, False, tname, blockposi, tmpContent, time.time())
                returnList.append(tmpBlock)
                self.buffer.append(tmpBlock)
        return returnList
        
    def create_table(self, tname):
        maxrecordNum[tname] = 0
        freeList[tname] = []
        file_manager.create_data_file(tname)
    
    def delete_table(self, tname):
        del maxrecordNum[tname]
        del freeList[tname]
        delList = []
        for i in range(len(self.buffer)):
            if self.buffer[i].tablename == tname:
                delList.append(self.buffer[i])
                self.blockNum -= 1
        for i in delList:
            self.buffer.remove(i)
        file_manager.delete_data_file(tname)
        
    def clear_table(self, tname):
        maxrecordNum[tname] = 0
        freeList[tname] = []
        delList = []
        for i in range(len(self.buffer)):
            if self.buffer[i].tablename == tname:
                delList.append(self.buffer[i])
                self.blockNum -= 1
        for i in delList:
            self.buffer.remove(i)
        file_manager.clear_data_file(tname)

def get_index(Ind):
    if Ind not in index_buffer:
        pass                        # 欠一个Exception
    else:
        return index_buffer[Ind]

def save_index(tree, Ind):
    if Ind in index_buffer:
        pass                        # 欠一个Exception
    else:
        index_buffer[Ind] = tree 

def delete_index(Ind):
    if Ind not in index_buffer:
        pass                        # 欠一个Exception
    else:
        del index_buffer[Ind]

data_buffer = DataBuffer()

def initialize_buffer():
    global freeList, index_buffer, maxrecordNum
    #data_buffer = DataBuffer()
    freeList = file_manager.get_freeList()
    index_buffer = file_manager.get_index_file()
    #file_manager.get_data_buffer()
    maxrecordNum = file_manager.get_maxrecordNum()
    #print(maxrecordNum)
    #print(freeList)

def quit_buffer():
    for tmpBLock in (data_buffer.buffer):
        if tmpBLock.dirty == True:
            file_manager.write_back(tmpBLock)
    file_manager.save_freeList(freeList)
    file_manager.save_index_file(index_buffer)
    #file_manager.save_data_buffer()
    #print(maxrecordNum)
    #print(freeList)
    #time.sleep(5)
    file_manager.save_maxrecordNum(maxrecordNum)

