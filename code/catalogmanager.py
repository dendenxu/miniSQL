import file_manager
from minisqlclass import *

class CatalogManager:
    def __init__(self, table=[], index=[]):
        self.table = table[:]
        self.index = index[:]
        self.tableNum = int(len(table))
        self.indexNum = len(index)
        self.indexcnt = 0
    '''
    check_table(name)返回T/F
    check_index(table_name,attribute_name)返回index_index
    check_unique(table_name,attribute_name)返回T/F
    check_attribute(table_name,attribute_name)返回attribute_index
    get_unique(name)从catalog得到这个表的所有unique属性
    get_index(name)返回此表的所有index
    create_table(name，index_index,if_unique)交给catalog更改metadata并存入主键的index编号
    drop_table(name)给catalog并返回index_index(s)
    add_index(table_name,attribute_name,index_index)
    drop_index(table_name,attribute_name)返回index_index
    '''
    def check_table(self,name):
        for i in self.table:
            if i.name==name:
                return 1
        return 0

    def check_index(self,table_name,attribute_name):
        for i in self.index:
            if i.table_name==table_name and i.attribute_name==attribute_name:
                return 1
        return 0

    def check_unique(self,table_name,attribute_name):
        for i in self.index:
            if i.table_name == table_name and i.attribute_name == attribute_name:
                return i.isUnique
        return 0

    def check_attribute(self,table_name,attribute_name):
        for i in self.index:
            if i.table_name == table_name and i.attribute_name == attribute_name:
                return i.index_name
        return 0

    def get_unique(self, table_name):
        ret = []
        for i in self.table:
            if i.name == table_name:
                for j in i.attributeList:
                    if j.isUnique == True:
                        ret.append(j.name)
        return ret

    def get_index(self,table_name):
        ret=[]
        for i in self.index:
            if i.table_name==table_name:
                ret.append(i)
        return ret

    def create_table(self, table,index):  # create a new table
        self.tableNum += 1
        self.table.append(table)
        self.create_index(index)
        #缺少了更改metadata和文件这部分

    def create_index(self, index):  # create a new index
        self.indexNum += 1
        self.index.append(index)
        #是否需要对文件进行操作？

    def drop_table(self, table_name):  # drop a table
        for i in range(len(self.table)):
            if self.table[i].name == table_name:
                del self.table[i]
                break
        self.tableNum -= 1
        for i in self.index:  # delete index on it
            if i.table_name == table_name:
                self.index.remove(i)
                self.indexNum -= 1

    def drop_index(self, index_name):  # drop an index
        for i in self.index:
            if i.index_name == index_name:
                self.index.remove(i)
                self.indexNum -= 1

    def check_index_name(self,index_name):
        for i in self.index:
            if i.name==index_name:
                return 1
        return 0

    def get_attribute(self, table_name):
        search_table = [i for i in self.table if i.name == table_name]
        result = []
        for i in search_table[0].attributeList:
            result.append([i.name, i.type, i.length])
        return result

    def get_primary(self, table_name):  # find the primary key of a table
        for i in self.table:
            if i.name == table_name:  # find the table
                for j in i.attributeList:
                    if j.isPrimary == True:  # find the primary key
                        return j.name

    def get_index_attribute(self, table):
        result = [i.attribute_name for i in self.index if i.table_name == table]
        return result

    def get_index_info(self, index_name):  # get the table and attribute of an index
        for i in self.index:
            if i.index_name == index_name:
                return [i.table_name, i.attribute_name,i.index_id]

    def get_length(self, table_name):
        for i in self.table:
            if i.name == table_name:  # find the table
                return i.size

    def get_type_length(self, table_name):
        search_table = [i for i in self.table if i.name == table_name]
        result = []
        for i in search_table[0].attributeList:
            result.append([i.type, i.length])
        return result

    def get_attribute_type(self, table_name, attr_name):
        search_table = [i for i in self.table if i.name == table_name]
        for i in search_table[0].attributeList:
            if i.name == attr_name:
                return i.type

    def get_index_id(self,table_name,attribute_name):
        for i in self.index:
            if i.table_name==table_name and i.attribute_name==attribute_name:
                return i.index_id
        return 0

    def get_attribute_cnt(self,table_name,attr_name):
        search_table = [i for i in self.table if i.name == table_name]
        cnt=0
        for i in search_table[0].attributeList:
            if i.name == attr_name:
                return cnt
            cnt=cnt+1