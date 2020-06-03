class Attribute:
    def __init__(self,name="",type="",length=0,isP=False,isU=False):
        self.name = name
        self.type = type
        self.length = length
        self.isPrimary = isP
        self.isUnique = isU
    def clear(self):
        self.name = ''
        self.type = ''
        self.length = 0
        self.isPrimary = False
        self.isUnique = False
    def __repr__(self):
        return self.name + ' '+ self.type

class Table:
    def __init__(self,name="",attributeList=list(),size=0):
        self.name = name
        self.attributeList = attributeList[:]
        self.attributeNum = len(attributeList)
        self.size = size
    def clear(self):
        self.name = ''
        self.attributeList = []
        self.attributeNum = 0
        self,size = 0
    def __repr__(self):
        return self.name

class Index:
    def __init__(self,tableName="",indexName="",attrName="",id=-1):
        self.table_name = tableName
        self.index_name = indexName #还没决定index_name是等于index manager中的index序号还是create index语句中的indexname
        self.index_id = id
        self.attribute_name = attrName
    def clear(self):
        self.table_name = ''
        self.index_name = ''
        self.attribute_name = ''
    def __repr__(self):
        return self.index_name+':'+self.table_name+'.'+self.attribute_name


class Condition:
    def __init__(self,attribute_name="",op='',operand=None):
        self.attribute_name = attribute_name
        self.op = op
        self.operand = operand
    def get_attribute_name(self):
        return self.attribute_name
    def get_op(self):
        return self.op
    def get_operand(self):
        return self.operand
    def clear(self):
        self.varType = ''
        self.op = ''
        self.operand = 0

class Attribute:
    def __init__(self,name="",type="",length=0,isP=False,isU=False):
        self.name = name
        self.type = type
        self.length = length
        self.isPrimary = isP
        self.isUnique = isU
    def clear(self):
        self.name = ''
        self.type = ''
        self.length = 0
        self.isPrimary = False
        self.isUnique = False
    def __repr__(self):
        return self.name + ' '+ self.type

class Condition_2:
    def __init__(self, attribute=-1, type = -1, value = None):
        self.attribute = attribute
        self.type = type            # 0:等于 ; 1:小于 ; 2:大于 ; 3:不等于
        self.value = value

class error_type:
    def __init__(self):
        self.NONE = ""
        self.syn = "Error : syntax error"
        self.ivld_cmd = "Error : No query specified"
        self.exist_t = "Error : table is exist"
        self.exist_i = "Error : index is exist"
        self.exist_attrib = "Error : attribute is exist"
        self.not_exist_t = "Error : table is not exist"
        self.not_exist_i = "Error : index is not exist"
        self.not_exist_a = "Error : attribute  is not exist"
        self.ivld_char = "Error : invalid chat"
        self.ivld_dt_tp = "Error : invalid data type"
        self.no_prim_k = "Error : no primary key"
        self.invalid_prim_k = "Error : invalid primary key"
        self.not_exist_k = "Error : key is not exist"
        self.ept_i = "Error : empty index"
        self.ept_t = "Error : empty table"
        self.no_unq_a = "Error : the attribute is not unique"
        self.insert_not_match = "Error : insert data not match"
        self.out_of_range = "Error : data out of range"