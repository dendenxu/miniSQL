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