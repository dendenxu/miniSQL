import catalogmanager
import interpreter
import index
import file_manager
import record_manager
from minisqlclass import *
from time import perf_counter
import re

catalog_manager = None
start_time = None
parser = None


def init():
    global catalog_manager, parser
    catalog_manager = file_manager.get_catalog_file()
    if catalog_manager is None:
        catalog_manager = catalogmanager.CatalogManager()
    parser = interpreter.command(catalog_manager)


def sql_exit():
    file_manager.save_catalog_file(catalog_manager)
    print('Bye\n')


def create_table(dict):
    prim_index = dict['pri_index']
    index.create_index(prim_index.index_id, [])
    table = dict['new_table']
    catalog_manager.create_table(table, prim_index)
    record_manager.create_table(dict['table_name'])
    # print('done')


def drop_table(table_name):
    ind = catalog_manager.get_index(table_name)
    for i in ind:
        index.drop_index(i.index_id)
    record_manager.delete_table(table_name)
    catalog_manager.drop_table(table_name)
    # print('done')

def delete_all(table_name):
    ind=catalog_manager.get_index(table_name)
    for i in ind:
        index.drop_index(i.index_id)
        index.create_index(i.index_id,[])
    record_manager.delete_table(table_name)
    record_manager.create_table(table_name)

def select_all(table_name):
    try:
        print(record_manager.select_record_with_Index(table_name, 0, []));
    except:
        print("Empty table")

def select(table_name, conditions):
    # print("meow")
    conditionlist = []
    for condi in conditions:
        cond = Condition_2()
        cond.value = condi.operand
        cond.attribute = catalog_manager.get_attribute_cnt(table_name, condi.attribute_name)
        if condi.op == '=':
            cond.type = 0
        elif condi.op == '<':
            cond.type = 1
        elif condi.op == '>':
            cond.type = 2
        else:
            cond.type = 3
        conditionlist.append(cond)
    # print(conditionlist[0].type,conditionlist[0].attribute,conditionlist[0].value)
    print(record_manager.select_record_with_Index(table_name, 0, conditionlist))


def select_index(table_name, not_index_conditions, index_conditions):
    templist = []
    try:
        for index_condition in index_conditions:
            index_id = catalog_manager.get_index_id(table_name, index_condition.attribute_name)
            if index_condition.op == '=':
                temp = index.search(index_id, index_condition.operand, is_range=False, is_not_equal=False)
            elif index_condition.op == '<':
                temp = index.search(index_id, index_condition.operand, is_greater=False, is_current=False, is_range=True, is_not_equal=False)
            elif index_condition.op == '>':
                temp = index.search(index_id, index_condition.operand, is_greater=True, is_current=False, is_range=True, is_not_equal=False)
            elif index_condition.op == '<>':
                temp = index.search(index_id, index_condition.operand, is_range=True, is_not_equal=True)
            elif index_condition.op == '<=':
                temp = index.search(index_id, index_condition.operand, is_greater=False, is_current=True, is_range=True, is_not_equal=False)
            elif index_condition.op == '>=':
                temp = index.search(index_id, index_condition.operand, is_greater=True, is_current=True, is_range=True, is_not_equal=False)
            if(type(temp) == int):
                templist.append([temp])
            else:
                templist.append(temp)
    except Exception as e:
        print(e)
    else:
        conditionlist = []
        for condi in not_index_conditions:
            cond = Condition_2()
            cond.value = condi.operand
            cond.attribute = catalog_manager.get_attribute_cnt(table_name, condi.attribute_name)
            if condi.op == '=':
                cond.type = 0
            elif condi.op == '<':
                cond.type = 1
            elif condi.op == '>':
                cond.type = 2
            else:
                cond.type = 3
            conditionlist.append(cond)
        print(record_manager.select_record_with_Index(table_name, templist, conditionlist))


def delete(table_name, conditions):
    if_index = catalog_manager.check_index(table_name, conditions.attribute_name)
    if if_index:
        index_id = catalog_manager.get_index_id(table_name, conditions.attribute_name)
        if conditions.op == '=':
            temp = index.search(index_id, conditions.operand, is_range=False, is_not_equal=False)
            index.delete(index_id, conditions.operand, is_range=False, is_not_equal=False)
        elif conditions.op == '<':
            temp = index.search(index_id, conditions.operand, is_greater=False, is_current=False, is_range=True,is_not_equal=False)
            index.delete(index_id, conditions.operand, is_greater=False, is_current=False, is_range=True, is_not_equal=False)
        elif conditions.op == '>':
            temp = index.search(index_id, conditions.operand, is_greater=True, is_current=False, is_range=True,is_not_equal=False)
            index.delete(index_id, conditions.operand, is_greater=True, is_current=False, is_range=True, is_not_equal=False)
        elif conditions.op == '<>':
            temp = index.search(index_id, conditions.operand, is_range=True, is_not_equal=True)
            index.delete(index_id, conditions.operand, is_range=True, is_not_equal=True)
        elif conditions.op == '<=':
            temp = index.search(index_id, conditions.operand, is_greater=False, is_current=True, is_range=True,is_not_equal=False)
            index.delete(index_id, conditions.operand, is_greater=False, is_current=True, is_range=True, is_not_equal=False)
        elif conditions.op == '>=':
            temp = index.search(index_id, conditions.operand, is_greater=True, is_current=True, is_range=True,is_not_equal=False)
            index.delete(index_id, conditions.operand, is_greater=True, is_current=True, is_range=True, is_not_equal=False)
        if (type(temp) == int):
            ret=record_manager.select_record_with_Index(table_name,[[temp]],[])
            record_manager.delete_record_with_Index(table_name,[[temp]],[])
        else:
            ret=record_manager.select_record_with_Index(table_name, [temp], [])
            record_manager.delete_record_with_Index(table_name, [temp], [])
        attr = catalog_manager.get_attribute(table_name)
        for i in attr:
            attr1 = i[0]
            if catalog_manager.check_index(table_name, attr1) == 1 and attr1!=conditions.attribute_name:
                index_id=catalog_manager.get_index_id(table_name,attr1)
                for infomation in ret:
                    index.delete(index_id,infomation[catalog_manager.get_attribute_cnt(table_name,attr1)],is_range=False, is_not_equal=False)
    else:
        conditionlist = []
        cond = Condition_2()
        cond.value = conditions.operand
        cond.attribute = catalog_manager.get_attribute_cnt(table_name, conditions.attribute_name)
        if conditions.op == '=':
            cond.type = 0
        elif conditions.op == '<':
            cond.type = 1
        elif conditions.op == '>':
            cond.type = 2
        else:
            cond.type = 3
        conditionlist.append(cond)
        ret = record_manager.select_record_with_Index(table_name, 0, conditionlist)
        record_manager.delete_record_with_Index(table_name, 0, conditionlist)
        attr = catalog_manager.get_attribute(table_name)
        for i in attr:
            attr1 = i[0]
            if catalog_manager.check_index(table_name, attr1) == 1:
                index_id = catalog_manager.get_index_id(table_name, attr1)
                for infomation in ret:
                    index.delete(index_id, infomation[catalog_manager.get_attribute_cnt(table_name, attr1)],
                                 is_range=False, is_not_equal=False)


def insert(table_name, values):
    value = []
    attr = catalog_manager.get_attribute(table_name)
    for i in attr:
        value.append(values[i[0]])
    attr1 = catalog_manager.get_primary(table_name)
    index_id = catalog_manager.get_index_id(table_name, attr1)
    for i in attr:
        attr2 = i[0]
        if catalog_manager.check_unique(table_name, attr2) == 1:
            if catalog_manager.check_index(table_name, attr2) == 1:
                try:
                    temp = index.search(catalog_manager.get_index_id(table_name, attr2), values[attr2], is_range=False, is_not_equal=False)
                except Exception:
                    temp = []
                else:
                    print("Unique value has already exists\n")
                    return
            else:
                conditionlist = []
                condition = Condition_2(catalog_manager.get_attribute_cnt(table_name, attr2), 0, values[attr2])
                conditionlist.append(condition)
                # fixme: record manager should throw proper exception on empty file or empty table
                # I'm currently adding a small stub
                try:
                    temp = record_manager.select_record_with_Index(table_name, 0, conditionlist)
                except Exception as e:
                    # fixme: this will print the exception, reminding us to fix this
                    print(e)
                    temp = []
                if len(temp) != 0:
                    print("Unique value has already exists\n")
                    return
    line = record_manager.insert_record(table_name, value)
    for attr_ in attr:
        attr_name = attr_[0]
        if catalog_manager.check_index(table_name, attr_name) == 1:
            index.insert(catalog_manager.get_index_id(table_name, attr_name), values[attr_name], line)


def drop_index(index_name):
    temp = catalog_manager.get_index_info(index_name)
    catalog_manager.drop_index(index_name)
    index.drop_index(temp[2])


def create_index(new_idex):
    if catalog_manager.check_index(new_idex.table_name, new_idex.attribute_name) != 0:
        print("Index already exists!")
        return
    catalog_manager.create_index(new_idex)
    temp = record_manager.select_record_with_Index(new_idex.table_name, 0, [])
    cnt = catalog_manager.get_attribute_cnt(new_idex.table_name, new_idex.attribute_name)
    list = []
    for i in temp:
        list.append(i[cnt])
    # print(new_idex.index_id,list)
    index.create_index(new_idex.index_id, list)


def read_file(file_name):
    try:
        with open(file_name, "r") as f:
            command_prompt(file_file=f)
    except:
        print("File not exist")

def show_tables():
    tabls=catalog_manager.get_all_table()
    print(tabls)

def show_table(table_name):
    print("create table",table_name,"(")
    attr=catalog_manager.get_attribute(table_name)
    for attribute in attr:
        #[i.name, i.type, i.length]
        if attribute[1]=='char':
            print(attribute[0],attribute[1],"(",attribute[2],"),")
        else:
            print(attribute[0],attribute[1],",")
    pri=catalog_manager.get_primary(table_name)
    print("primary key(",pri,")")
    print(");")

def show_index(table_name):
    ind=catalog_manager.get_index(table_name)
    for idx in ind:
        if idx.index_name=="":
            idx.index_name=idx.table_name+"_"+idx.attribute_name
        print("index name:",idx.index_name,"\t index attribute name:",idx.attribute_name,"\t index id:",idx.index_id)

def show_attribute(table_name):
    attr = catalog_manager.get_attribute(table_name)
    for attribute in attr:
        if attribute[1] == 'char':
            print(attribute[0], attribute[1], "(", attribute[2], ")")
        else:
            print(attribute[0], attribute[1])

def execute(command_dict):
    # print(command_dict)
    start_time = perf_counter()
    if command_dict == None:
        print(parser.error_tp)
        return
    if len(command_dict) == 0:
        print('ERROR: invalid command')
        return
    if command_dict['type'] == 'create_table':
        create_table(command_dict)
    elif command_dict['type'] == 'delete table':
        drop_table(command_dict['table_name'])
    elif command_dict['type'] == 'select data not use index':
        select(command_dict['table_name'], command_dict['not_index_condt'])
    elif command_dict['type'] == 'select data use index':
        select_index(command_dict['table_name'], command_dict['not_index_condt'], command_dict['index_condt'])
    elif command_dict['type'] == 'delete data':
        delete(command_dict['table_name'], command_dict['condt'][0])
    elif command_dict['type'] == 'insert data':
        insert(command_dict['table_name'], command_dict['val'])
    elif command_dict['type'] == 'delete index':
        drop_index(command_dict['index_name'])
    elif command_dict['type'] == 'create_index':
        create_index(command_dict['new_index'])
    elif command_dict["type"] == "read_file":
        read_file(command_dict["file_name"])
    elif command_dict['type'] == "delete data all":
        delete_all(command_dict['table_name'])
    elif command_dict["type"] == "select data all":
        select_all(command_dict["table_name"])
    elif command_dict["type"] == "show tables":
        show_tables()
    elif command_dict["type"] == "show table":
        show_table(command_dict['table name'])
    elif command_dict["type"] == "show index":
        show_index(command_dict['table name'])
    elif command_dict["type"] == "show attribute":
        show_attribute(command_dict['table name'])
    else:
        print('Error: unknown command ')
    end_time = perf_counter()
    print("{:.3f}s elapsed.".format(end_time - start_time))


def command_prompt(file_file=None):
    while True:
        command = ""
        while ';' not in command:
            if file_file is None:
                thi_command = input('>> ')
            else:
                thi_command = file_file.readline()
                if thi_command == "":
                    # EOF reached
                    print("One file done.")
                    return
            thi_command = thi_command.strip()
            temp = re.split("[#-]",thi_command)
            thi_command = temp[0]
            command += thi_command
        if command == '':
            continue
        elif command == 'exit;':
            sql_exit()
            return
        else:
            # print(command)
            # print(type(command))
            try:
                execute(parser.translate(command))
            except:
                sql_exit()
                raise


def main():
    init()
    command_prompt()


if __name__ == '__main__':
    main()
