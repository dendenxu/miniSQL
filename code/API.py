import catalogmanager
import interpreter
import index
import file_manager
import record_manager
from minisqlclass import *

catalog_manager = None
start_time = None
parser = None

def init():
    global catalog_manager, parser
    # catalog_manager = file_manager.get_catalog_file()
    catalog_manager=catalogmanager.CatalogManager()
    parser = interpreter.command(catalog_manager)


def sql_exit():
    file_manager.save_catalog_file(catalog_manager)
    print('Bye\n')


def create_table(dict):
    prim_index=dict['pri_index']
    index.create_index(prim_index.index_id,[])
    table=dict['new_table']
    catalog_manager.create_table(table,prim_index)
    record_manager.create_table(dict['table_name'])
    print('done')


def drop_table(table_name):
    ind=catalog_manager.get_index(table_name)
    for i in ind:
        index.drop_indedx(i.index_id)
    record_manager.delete_table(table_name)
    print('done')


def select(table_name, conditions):
    print("meow")
    conditionlist=[]
    for condi in conditions:
        cond=Condition_2()
        cond.value=condi.operand
        cond.attribute=catalog_manager.get_attribute_cnt(table_name,condi.attribute_name)
        if condi.op=='=':
            cond.type=0
        elif condi.op=='<':
            cond.type=1
        elif condi.op=='>':
            cond.type=2
        else: cond.type=3
        conditionlist.append(cond)
    print(conditionlist[0].type,conditionlist[0].attribute,conditionlist[0].value)
    print(record_manager.select_record_with_Index(table_name,0,conditionlist))

def select_index(table_name, index_conditions):
    index_id=catalog_manager.get_index_id(table_name,index_conditions.attribute_name)
    if index_conditions.op=='=' :
        print(index_id,index_conditions.operand)
        temp=index.search(index_id,index_conditions.operand)
    elif index_conditions.op=='<':
        temp=index.search(index_id,index_conditions.operand,is_greater=False,is_current=False)
    elif index_conditions.op=='>':
        temp=index.search(index_id,index_conditions.operand,is_greater=True,is_current=False)
    if(type(temp)==int):
        print(record_manager.select_record_with_Index(table_name, [[temp]], []))
    else:
        print(record_manager.select_record_with_Index(table_name, [temp], []))

def delete(table_name, conditions):
    if_index=catalog_manager.check_index(table_name,conditions.attribute_name)
    if if_index:
        index_id = catalog_manager.get_index_id(table_name, conditions.attribute_name)
        if conditions.op == '=':
            temp = index.delete(index_id, conditions.operand)
        elif conditions.op == '<':
            temp = index.delete(index_id, conditions.operand, is_greater=False, is_current=False)
        elif conditions.op == '>':
            temp = index.delete(index_id, conditions.operand, is_greater=True, is_current=False)
        if (type(temp) == int):
            print(record_manager.select_record_with_Index(table_name, [[temp]], []))
        else:
            print(record_manager.select_record_with_Index(table_name, [temp], []))
    else :
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
        print(conditionlist[0].value,conditionlist[0].attribute,conditionlist[0].type)
        temp=record_manager.search_record_with_Index(table_name,0,conditionlist)
        print(temp)

def insert(table_name, values):
    value=[]
    attr=catalog_manager.get_attribute(table_name)
    for i in attr:
        value.append(values[i[0]])
    print(value)
    line=record_manager.insert_record(table_name,value)
    for attr_ in attr:
        attr_name=attr_[0]
        if catalog_manager.check_index(table_name,attr_name)==1:
            index.insert(catalog_manager.get_index_id(table_name,attr_name),values[attr_name],line)

def drop_index(index_name):
    catalog_manager.drop_index(index_name)
    temp=catalog_manager.get_index_info(index_name)
    index.drop_index(temp.index_id)

def create_index(new_idex):
    catalog_manager.create_index(new_idex)
    temp=record_manager.select_record_with_Index(new_idex.table_name,0,[])
    cnt=catalog_manager.get_attribute_cnt(new_idex.table_name,new_idex.attribute_name)
    list=[]
    for i in temp:
        list.append(i[cnt])
    print(new_idex.index_id,list)
    index.create_index(new_idex.index_id,list)

def execute(command_dict):
    print(command_dict)
    if command_dict == None:
        print(parser.error_type)
        return
    if len(command_dict) == 0:
        print('ERROR: invalid command')
        return
    if command_dict['type'] == 'create_table':
        create_table(command_dict)
    elif command_dict['type'] == 'drop_table':
        drop_table(command_dict['table_name'])
    elif command_dict['type'] == 'select data not use index':
        select(command_dict['table_name'], command_dict['not_index_condt'])
    elif command_dict['type'] == 'select data use index':
        select_index(command_dict['table_name'], command_dict['index_condt'][0])
    elif command_dict['type'] == 'delete data':
        delete(command_dict['table_name'], command_dict['condt'][0])
    elif command_dict['type'] == 'insert data':
        insert(command_dict['table_name'], command_dict['val'])
    elif command_dict['type'] == 'delete_index':
        drop_index(command_dict['index_name'])
    elif command_dict['type'] == 'create_index':
        create_index(command_dict['new_index'])
    else:
        print('Error: unknown command ')
    print()


def command_prompt():
    while True:
        command=""
        while ';' not in command:
            thi_command = input('>> ').strip()
            temp=thi_command.split('#',1)
            thi_command=temp[0]
            command+=thi_command
        if command == '':
            continue
        elif command == 'exit':
            sql_exit()
            return
        else:
            execute(parser.translate(command))


if __name__ == '__main__':
    init()
    command_prompt()