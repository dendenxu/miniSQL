import re
import sys
import copy
import catalogmanager
from minisqlclass import *

error = error_type()


class command:
    def __init__(self, catalog_manager):
        self.error_tp = error.NONE
        self.catalog = catalog_manager

    def clear(self):
        self.error_tp = error.NONE

    def translate(self, instruction):
        try:
            _command = {}
            # print(instruction)
            inst_temp = re.split('[,; ()]', instruction)
            inst = []
            for i in inst_temp:
                if i != "":
                    i = i.replace(" ", "").replace("\t", "").strip()
                    inst.append(i)
            # print(inst)
            if inst[0] == 'create':
                if inst[1] == 'table':  # create table
                    _command = self.create_table(inst)
                elif inst[1] == 'index':  # create index
                    _command = self.create_index(inst)
            elif inst[0] == 'drop':
                if inst[1] == 'index':  # drop index
                    _command = self.delete_index(inst)
                elif inst[1] == 'table':  # drop table
                    _command = self.delete_table(inst)
            elif inst[0] == 'select':  # select
                _command = self.select_from_table(inst)
            elif inst[0] == 'insert':  # 插入
                if inst[1] == 'into':
                    _command = self.insert_into_table(inst)
            elif inst[0] == 'delete':  # 删除数据
                if inst[1] == 'from':
                    _command = self.delete_from_table(inst)
            elif inst[0] == 'execfile':  # 执行文件
                _command = self.read_file(inst)
            else:
                self.error_tp = error.ivld_cmd
                return
            return _command
        except:
            self.error_tp = error.syn
            return

    def read_file(self, inst):
        result = {}
        result['type'] = 'read_file'
        result["file_name"] = inst[1]
        return result

    def create_table(self, inst):
        result = {}
        result['type'] = 'create_table'
        new_table = Table()
        attrib = []
        if len(inst) < 3:
            self.error_tp = error.syn
            return
        result['table_name'] = inst[2]
        new_table.name = inst[2]
        if self.catalog.check_table(new_table.name)==1:
            self.error_tp = error.exist_t
            return
        i = 3
        have_pri = False
        while i < len(inst):
            new_attrib = Attribute()
            if inst[i] == "primary":
                if i + 1 < len(inst) and inst[i + 1] == "key":
                    have_pri = True
                    break
                else:
                    self.error_tp = error.syn
                    return
            new_attrib.name = inst[i]
            i += 1
            if i >= len(inst):
                self.error_tp = error.syn
                return
            new_attrib.type = inst[i]
            if new_attrib.type == "char":
                if i + 1 >= len(inst):
                    self.error_tp = error.syn
                    return
                length = int(inst[i + 1])
                new_attrib.length = length
                i = i + 2
            elif new_attrib.type == "int":
                new_attrib.length = 4
                i += 1
            elif new_attrib.type == "float":
                new_attrib.length = 4
                i += 1
            else:
                self.error_tp = error.ivld_dt_tp
                return
            new_table.size += new_attrib.length
            if i < len(inst):
                if inst[i] == "unique":
                    new_attrib.isUnique = True
                    new_attrib.isPrimary = False
                    i += 1
                else:
                    new_attrib.isUnique = False
                    new_attrib.isPrimary = False
                attrib.append(copy.deepcopy(new_attrib))
        if ~have_pri and i >= len(inst):
            self.error_tp = error.no_prim_k
            return
        PRI_key = ""
        if i < len(inst):
            if inst[i] == "primary":
                if i + 2 < len(inst):
                    PRI_key = inst[i + 2]
                else:
                    self.error_tp = error.syn
                    return
                flag_pri_in_att = False
                for j in attrib:
                    if j.name == PRI_key:
                        j.isPrimary = True
                        j.isUnique = True
                        flag_pri_in_att = True
                if flag_pri_in_att == False:
                    self.error_tp = error.invalid_prim_k
                    have_pri = False
                    return
            else:
                self.error_tp = error.no_prim_k
                return

        new_table.attributeList = attrib
        new_table.attributeNum = len(attrib)
        result['new_table'] = new_table
        pri_index = Index()

        pri_index.table_name = new_table.name
        pri_index.index_name = new_table.name + ' ' + PRI_key
        pri_index.attribute_name = PRI_key
        pri_index.index_id = self.catalog.indexcnt
        self.catalog.indexcnt = self.catalog.indexcnt + 1
        result['pri_index'] = pri_index
        return result

    def create_index(self, inst):
        result = {}
        result['type'] = 'create_index'
        new_index = Index()
        i = 2
        if i >= len(inst):
            self.error_tp = error.syn
            return

        new_index.index_name = inst[i]
        result['index_name'] = new_index.index_name
        # print('meow')    不知道为什么不对，先不管了
        # self.catalog.check_index_name(new_index.index_name)
        # print('meow')
        # if self.catalog.check_index_name(new_index.index_name):
        #     self.error_tp=error.exist_i
        #     return
        i = 3
        if i >= len(inst) or inst[i] != "on":
            self.error_tp = error.syn
            return
        i = 4
        if i >= len(inst):
            self.error_tp = error.syn
            return
        new_index.table_name = inst[i]
        if (self.catalog.check_table(new_index.table_name) != 1):
            self.error_tp = error.not_exist_t
            return
        result['table_name'] = new_index.table_name
        i = 5
        if i >= len(inst):
            self.error_tp = error.syn
            return
        new_index.attribute_name =inst[i]
        new_index.attribute_name = inst[i]
        if self.catalog.check_index(new_index.table_name, new_index.attribute_name)==1:
            self.error_tp = error.exist_i
            return
        if (self.catalog.check_unique(new_index.table_name,new_index.attribute_name )!=1):
            self.error_tp = error.no_unq_a
            return
        new_index.index_id = self.catalog.indexcnt
        self.catalog.indexcnt = self.catalog.indexcnt + 1
        result['new_index'] = new_index
        return result

    def delete_table(self, inst):
        result = {}
        result['type'] = "delete table"
        if len(inst) < 3:
            self.error_tp = error.ept_t
            return
        if (self.catalog.check_table(inst[2]) != 1):
            self.error_tp = error.not_exist_t
            return
        result['table_name'] = inst[2]
        return result

    def delete_index(self, inst):
        result = {}
        result['type'] = "delete index"
        if len(inst) < 3:
            self.error_tp = error.syn
            return
        if (self.catalog.check_index_name(inst[2])!=1):
            self.error_tp = error.not_exist_i
            return
        result['index_name'] = inst[2]
        return result

    def select_from_table(self, inst):
        result = {}
        attrib = []
        result['type'] = "select data not use index"
        if len(inst) < 4:
            self.error_tp = error.syn
            return
        i = 1
        flag_select_all = False
        if inst[i] != '*':
            while inst[i] != 'from':
                if i >= len(inst):
                    self.error_tp = error.syn
                    return
                attrib.append(inst[i])
                i += 1
        else:
            i += 1
            flag_select_all = True

        i += 1
        result['table_name'] = inst[i]
        if (self.catalog.check_table(result['table_name']) != 1):
            self.error_tp = error.not_exist_t
            return

        if len(attrib) == 0 and flag_select_all == False:
            self.error_tp = error.syn
            return
        i += 1
        result['index_condt'] = []
        result['not_index_condt'] = []
        indexes = self.catalog.get_index_attribute(result['table_name'])
        attributes = self.catalog.get_attribute(result['table_name'])
        attributes_name = []
        for m in attributes:
            attributes_name.append(m[0])

        if flag_select_all:
            attrib = attributes_name
        else:
            for j in attrib:
                if j not in attributes_name:
                    self.error_tp = error.not_exist_a
                    return
        result['attributes'] = attrib
        index_condition = []
        no_index_condition = []
        condition = []
        if i >= len(inst):
            result['condt'] = condition
            return result
        # print(inst)
        if i < len(inst):
            if inst[i] == 'where':
                i = i + 1
                while i < len(inst):
                    attrib_name = inst[i]
                    if self.catalog.check_attribute(result['table_name'],attrib_name)!=1:
                        self.error_tp = error.not_exist_a
                        return
                    if i + 1 >= len(inst):
                        self.error_tp = error.syn
                        return
                    operation = inst[i + 1]
                    value = None
                    if i + 2 >= len(inst):
                        self.error_tp = error.syn
                        return
                    attrib_tp = self.catalog.get_attribute_type(result['table_name'], attrib_name)
                    if attrib_tp == 'float':
                        value = float(inst[i + 2])
                    elif attrib_tp == 'int':
                        value = int(inst[i + 2])
                    elif attrib_tp == 'char':
                        value = str(inst[i + 2][1:-1])
                    condition.append(Condition(attrib_name, operation, value))
                    if attrib_name in indexes:
                        result['type'] = "select data use index"
                        index_condition.append(Condition(attrib_name, operation, value))
                    else:
                        result['type'] = "select data not use index"
                        no_index_condition.append(Condition(attrib_name, operation, value))
                    i = i + 4
            else:
                self.error_tp = error.syn
                return
        result['condt'] = condition
        result['index_condt'] = index_condition
        result['not_index_condt'] = no_index_condition
        return result

    def insert_into_table(self, inst):
        result = {}
        result['type'] = "insert data"
        if len(inst) < 5:
            self.error_tp = error.syn
            return
        i = 2
        result['table_name'] = inst[2]
        if self.catalog.check_table(result['table_name']) != 1:
            self.error_tp = error.not_exist_t
            return
        attrib = self.catalog.get_attribute(result['table_name'])
        types = self.catalog.get_type_length(result['table_name'])
        i += 1
        if inst[i] != 'values':
            self.error_tp = error.syn
            return
        insert_values = {}
        i = 0
        while i < len(types):
            if i + 4 >= len(inst):
                self.error_tp = error.insert_not_match
                return
            if types[i][0] == 'float':
                insert_values[attrib[i][0]] = float(inst[4 + i])
            elif types[i][0] == 'int':
                if int(inst[4 + i]) > sys.maxsize or int(inst[4 + i]) < -1 * sys.maxsize:
                    self.error_tp = error.out_of_range
                    return
                else:
                    insert_values[attrib[i][0]] = int(inst[4 + i])
            elif types[i][0] == 'char':
                if inst[4 + i][0] != '\'' and inst[4 + i][0] != '\"' or inst[4 + i][0] != inst[4 + i][-1]:
                    self.error_tp = error.syn
                    return
                insert_values[attrib[i][0]] = str(inst[i + 4][1:-1])
                if len(str(inst[i + 4][1:-1])) > types[i][1]:
                    self.error_tp = error.out_of_range
                    return
            i += 1
        if i != len(types):
            self.error_tp = error.insert_not_match
            return
        result['val'] = insert_values
        return result

    def delete_from_table(self, inst):
        result = {}
        result['type'] = "delete data"
        if len(inst) < 5:
            self.error_tp = error.syn
            return
        i = 2
        result['table_name'] = inst[i]
        if len(inst) == 3:
            result['condt'] = []
            return result
        if (self.catalog.check_table(result['table_name']) != 1):
            self.error_tp = error.not_exist_t
            return
        i = i + 1
        if inst[i] != 'where':
            self.error_tp = error.syn
            return
        else:
            i += 1
        result['condt'] = []
        while i < len(inst):
            if i + 2 >= len(inst):
                self.error_tp = error.syn
                return
            if self.catalog.check_attribute(result['table_name'], inst[i]) != 1:
                self.error_tp = error.not_exist_a
                return
            nowtype = self.catalog.get_attribute_type(result['table_name'], inst[i])
            if nowtype == 'float':
                value = float(inst[i + 2])
            elif nowtype == 'int':
                value = int(inst[i + 2])
            elif nowtype == 'char':
                value = str(inst[i + 2][1:-1])
            cond = Condition(inst[i], inst[i + 1], value)
            result['condt'].append(copy.deepcopy(cond))
            i = i + 4
        return result
