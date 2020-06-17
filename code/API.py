import catalogmanager
import interpreter
import index
import file_manager
import record_manager
import buffer
from minisqlclass import *
from time import perf_counter
from exceptions import *
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
    file_manager.initialize_file()
    buffer.initialize_buffer()

def sql_exit(if_str_command=False):
    file_manager.save_catalog_file(catalog_manager)
    buffer.quit_buffer()
    if(if_str_command):
        ret="Bye\n"
        return ret
    print('Bye\n')


def create_table(dict,if_str_command):
    prim_index = dict['pri_index']
    index.create_index(prim_index.index_id, [])
    table = dict['new_table']
    catalog_manager.create_table(table, prim_index)
    record_manager.create_table(dict['table_name'])
    # print('done')


def drop_table(table_name,if_str_command):
    ind = catalog_manager.get_index(table_name)
    for i in ind:
        index.drop_index(i.index_id)
    record_manager.delete_table(table_name)
    catalog_manager.drop_table(table_name)
    # print('done')

def delete_all(table_name,if_str_command):
    ind=catalog_manager.get_index(table_name)
    for i in ind:
        index.drop_index(i.index_id)
        index.create_index(i.index_id,[])
    record_manager.clear_table(table_name)
    #record_manager.create_table(table_name)

def select_all(table_name,if_str_command):
    try:
        if(if_str_command):
            ret=str(record_manager.select_record_with_Index(table_name, 0, []))+"\n"
            return ret
        print(record_manager.select_record_with_Index(table_name, 0, []));
    except:
        if(if_str_command):
            ret="Empty table\n"
            return ret
        print("Empty table")

def select(table_name, conditions,if_str_command):
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
    if(if_str_command):
        ret=record_manager.select_record_with_Index(table_name, 0, conditionlist)
        result = ""
        if len(ret) == 0:
            return "empty"
        result = result + "number of record selected:" + str(len(ret)) + "\n"
        attr = catalog_manager.get_attribute(table_name)
        for attribute in attr:
                result = result + attribute[0] + "\t\t"
        result = result + "\n"
        for info in ret:
            if info[0]>=1080101000:
                result=result+str(info[0])+"\t"+str(info[1])+"\t"+str(info[2])+"\n"
            else:
                result = result + str(info[0]) + "\t" + str(info[1]) + "\t\t" + str(info[2]) + "\n"
        return result
    print(record_manager.select_record_with_Index(table_name, 0, conditionlist))


def select_index(table_name, not_index_conditions, index_conditions,if_str_command):
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
        if(if_str_command):
            ret=str(e)+"\n"
            return ret
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
        if(if_str_command):
            result=""
            ret=record_manager.select_record_with_Index(table_name, templist, conditionlist)
            if len(ret)==0:
                return "empty"
            result=result+"number of record selected:"+str(len(ret))+"\n"
            attr = catalog_manager.get_attribute(table_name)
            cnt = 0
            for attribute in attr:
                result = result + attribute[0] + "\t\t"
            result = result + "\n"
            for info in ret:
                if info[0] >= 1080101000:
                    result = result + str(info[0]) + "\t" + str(info[1]) + "\t" + str(info[2]) + "\n"
                else:
                    result = result + str(info[0]) + "\t" + str(info[1]) + "\t\t" + str(info[2]) + "\n"
            return result
        print(record_manager.select_record_with_Index(table_name, templist, conditionlist))


def delete(table_name, conditions,if_str_command):
    num_changed=0
    if_index = catalog_manager.check_index(table_name, conditions.attribute_name)
    if if_index==1:
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
            num_changed=1
            ret=record_manager.select_record_with_Index(table_name,[[temp]],[])
            record_manager.delete_record_with_Index(table_name,[[temp]],[])
        else:
            num_changed=len(temp)
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
        num_changed=len(ret)
        for i in attr:
            attr1 = i[0]
            if catalog_manager.check_index(table_name, attr1) == 1:
                index_id = catalog_manager.get_index_id(table_name, attr1)
                for infomation in ret:
                    index.delete(index_id, infomation[catalog_manager.get_attribute_cnt(table_name, attr1)],
                                 is_range=False, is_not_equal=False)
    return "number of records deleted:"+str(num_changed)+"\n"


def insert(table_name, values,if_str_command):
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
                    if(if_str_command):
                        ret="Unique value has already exists\n"
                        return ret
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
                    if(if_str_command):
                        ret=str(e)+"\n"
                        return ret
                    print(e)
                    temp = []
                if len(temp) != 0:
                    if(if_str_command):
                        ret="Unique value has already exists\n"
                        return ret
                    print("Unique value has already exists\n")
                    return
    line = record_manager.insert_record(table_name, value)
    for attr_ in attr:
        attr_name = attr_[0]
        if catalog_manager.check_index(table_name, attr_name) == 1:
            index.insert(catalog_manager.get_index_id(table_name, attr_name), values[attr_name], line)


def drop_index(index_name,if_str_command):
    temp = catalog_manager.get_index_info(index_name)
    catalog_manager.drop_index(index_name)
    index.drop_index(temp[2])


def create_index(new_idex,if_str_command):
    if catalog_manager.check_index(new_idex.table_name, new_idex.attribute_name) != 0:
        if(if_str_command):
            ret="Index already exists!\n"
            return ret
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


def read_file(file_name,if_str_command):
    try:
        with open(file_name, "r",encoding='UTF-8') as f:
            if(if_str_command):
                return command_prompt(file_file=f,str_command=' ')
            else:
                command_prompt(file_file=f)
    except:
        if(if_str_command):
            ret="File not exist.\n"
            return ret
        else:print("File not exist.")

def show_tables(if_str_command):
    tabls=catalog_manager.get_all_table()
    if(if_str_command):
        return str(tabls)
    else :print(tabls)

def show_table(table_name,if_str_command):
    ret=""
    if(if_str_command):
        ret=ret+"create table "+str(table_name)+" (\n"
    else:print("create table",table_name,"(")
    attr=catalog_manager.get_attribute(table_name)
    for attribute in attr:
        #[i.name, i.type, i.length]
        if attribute[1]=='char':
            if(if_str_command):
                ret=ret+str(attribute[0])+" "+str(attribute[1])+" ( "+str(attribute[2])+" ),\n"
            else:print(attribute[0],attribute[1],"(",attribute[2],"),")
        else:
            if(if_str_command):
                ret=ret+str(attribute[0])+" "+str(attribute[1])+" ,\n"
            else:print(attribute[0],attribute[1],",")
    pri=catalog_manager.get_primary(table_name)
    if(if_str_command):
        ret=ret+"primary key("+str(pri)+")\n);\n"
    else:
        print("primary key(",pri,")")
        print(");")
    return ret

def show_index(table_name,if_str_command):
    ret=""
    ind=catalog_manager.get_index(table_name)
    for idx in ind:
        if idx.index_name=="":
            idx.index_name=idx.table_name+"_"+idx.attribute_name
        if(if_str_command):
            ret=ret+"index name:"+" "+str(idx.index_name)+" \t index attribute name:"+str(idx.attribute_name)+" \t index id:"+str(idx.index_id)
        else:print("index name:",idx.index_name,"\t index attribute name:",idx.attribute_name,"\t index id:",idx.index_id)
    return ret

def show_attribute(table_name,if_str_command):
    attr = catalog_manager.get_attribute(table_name)
    ret=""
    for attribute in attr:
        if attribute[1] == 'char':
            if(if_str_command):
                ret=ret+attribute[0]+" "+attribute[1]+" "+"(", attribute[2], ")"+'\n'
            else:print(attribute[0], attribute[1], "(", attribute[2], ")")
        else:
            if (if_str_command):
                ret = ret+attribute[0] + " " + attribute[1] + '\n'
            else: print(attribute[0], attribute[1])
    return ret

def execute(command_dict,if_str_command=False):
    ret=""
    start_time = perf_counter()
    if command_dict == None:
        if(if_str_command):
            ret=ret+parser.error_tp+"\n"
            return ret
        print(parser.error_tp)
        return
    if len(command_dict) == 0:
        if(if_str_command):
            ret=ret+'ERROR: invalid command'+"\n"
            return ret
        print('ERROR: invalid command')
        return
    if command_dict['type'] == 'create_table':
        sstr=create_table(command_dict,if_str_command)
    elif command_dict['type'] == 'delete table':
        sstr=drop_table(command_dict['table_name'],if_str_command)
    elif command_dict['type'] == 'select data not use index':
        sstr=select(command_dict['table_name'], command_dict['not_index_condt'],if_str_command)
    elif command_dict['type'] == 'select data use index':
        sstr=select_index(command_dict['table_name'], command_dict['not_index_condt'], command_dict['index_condt'],if_str_command)
    elif command_dict['type'] == 'delete data':
        sstr=delete(command_dict['table_name'], command_dict['condt'][0],if_str_command)
    elif command_dict['type'] == 'insert data':
        sstr=insert(command_dict['table_name'], command_dict['val'],if_str_command)
    elif command_dict['type'] == 'delete index':
        sstr=drop_index(command_dict['index_name'],if_str_command)
    elif command_dict['type'] == 'create_index':
        sstr=create_index(command_dict['new_index'],if_str_command)
    elif command_dict["type"] == "read_file":
        sstr=read_file(command_dict["file_name"],if_str_command)
    elif command_dict['type'] == "delete data all":
        sstr=delete_all(command_dict['table_name'],if_str_command)
    elif command_dict["type"] == "select data all":
        sstr=select_all(command_dict["table_name"],if_str_command)
    elif command_dict["type"] == "show tables":
        sstr=show_tables(if_str_command)
    elif command_dict["type"] == "show table":
        sstr=show_table(command_dict['table name'],if_str_command)
    elif command_dict["type"] == "show index":
        sstr=show_index(command_dict['table name'],if_str_command)
    elif command_dict["type"] == "show attribute":
        sstr=show_attribute(command_dict['table name'],if_str_command)
    else:
        if(if_str_command):
            sstr='Error: unknown command '
        else:print('Error: unknown command ')
    end_time = perf_counter()
    if(if_str_command):
        if(sstr!=None and len(sstr)!=0):
            ret=ret+sstr+"\n"
        sstr="{:.3f}s elapsed.".format(end_time - start_time)
        ret=ret+sstr+"\n"
        return ret
    else:
        print("{:.3f}s elapsed.".format(end_time - start_time))


def command_prompt(file_file=None,str_command=None):
    ret=""
    while True:
        command = ""
        while ';' not in command:
            if str_command != None and str_command!=" ":
                thi_command=str_command
            elif file_file is None:
                thi_command = input('>> ')
            else:
                thi_command = file_file.readline()
                if thi_command == "":
                    # EOF reached
                    if(str_command==None):
                        print("One file done.")
                        return
                    else:
                        ret=ret+"One file done."+"\n"
                        # print(ret)
                        return ret
            thi_command = thi_command.strip()
            temp = re.split("[#-]",thi_command)
            thi_command = temp[0]
            command += thi_command
        # print(command)
        if command == '':
            if(str_command==None):
                continue
            else: return ret
        elif command == 'exit;':
            sql_exit()
            ret=ret+"Bye"+"\n"
            return ret
        else:
            try:
                if(str_command==None):
                   thi=False
                else: thi=True
                # print(command)
                sstr=execute(parser.translate(command),thi)
                # print(sstr)
            except:
                sql_exit()
                raise
            else:
                if(sstr!=None and len(sstr)!=0):
                    ret=ret+sstr+"\n"
                if(thi and file_file==None):
                   return ret

def str_main(input):
    sql=input.split(";")
    ret=""
    for i in range(len(sql)-1):
        # print(i)
        if(len(sql[i])==0):
            continue
        if i!=0:
            if(len(sql[i])==0):
                continue
            while ((sql[i][0] == '\n' or sql[i][0] == '\r' or sql[i][0] == ' ')):
                if (len(sql[i]) > 1):
                    sql[i] = sql[i][1:]
                else:
                    break
            if len(sql[i]) <= 1:
                continue
            if(sql[i][0]=='#' or sql[i][0]=='-'):
                while sql[i][0]!='\n' and sql[i][0]!='\r':
                    if (len(sql[i]) > 1):
                        sql[i] = sql[i][1:]
                    else:
                        break
                if len(sql[i])<=1:
                    continue
            while((sql[i][0]=='\n' or sql[i][0]=='\r' or sql[i][0]==' ')):
                if(len(sql[i])>1):
                    sql[i]=sql[i][1:]
                else:
                    break
            if len(sql[i])<=1:
                continue
        try:
            sql[i]=sql[i]+";"
            sstr=command_prompt(str_command=sql[i])
        except Exception as e:
            # FIXME: bad practice
            sstr = "Something wrong...QAQ"
            sstr += e.__str__
        if(sstr!=None and len(sstr)!=0):
            ret=ret+sstr+"\n"
    return ret

def main():
    init()
    command_prompt()


# if __name__ == '__main__':
# that's a test
#     init()
#     sstr="""insert into student2 values(1080100001,'name1',99);\n
# insert into student2 values(1080100002,'name2',52.5);\n
# insert into student2 values(1080100003,'name3',98.5);\n
# insert into student2 values(1080100004,'name4',91.5);\n
# insert into student2 values(1080100005,'name5',72.5);\n
# insert into student2 values(1080100006,'name6',89.5);\n
# insert into student2 values(1080100007,'name7',63);\n
# insert into student2 values(1080100008,'name8',73.5);\n
# insert into student2 values(1080100009,'name9',79.5);\n
# insert into student2 values(1080100010,'name10',70.5);\n
# insert into student2 values(1080100011,'name11',89.5);\n
# insert into student2 values(1080100012,'name12',62);\n"""
#     sstr="""show tables;"""
#     print("str_out",str_main(sstr))
#     sql_exit(True)
#     command_prompt()
