def command_prompt():
    filename = ...
    f = open(filename, 'r')
    while True:
        command=""
        while True:
            newline = f.readline()
            if not newline:
                return
            command = command + newline
            if ';' in newline:
                break
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
			
def execute(command_dict):
    print(command_dict)
    initialtime = 0
    donetime = 0
    if command_dict == None:
        print(parser.error_type)
        return
    if len(command_dict) == 0:
        print('ERROR: invalid command')
        return
    if command_dict['type'] == 'create_table':
        initialtime = time()
        create_table(command_dict)
        donetime = time()
    elif command_dict['type'] == 'drop_table':
        initialtime = time()
        drop_table(command_dict['table_name'])
        donetime = time()
    elif command_dict['type'] == 'select data not use index':
        initialtime = time()
        select(command_dict['table_name'], command_dict['not_index_condt'])
        donetime = time()
    elif command_dict['type'] == 'select data use index':
        initialtime = time()
        select_index(command_dict['table_name'], command_dict['index_condt'][0])
        donetime = time()
    elif command_dict['type'] == 'delete':
        initialtime = time()
        delete(command_dict['table_name'], command_dict['conditions'][0])
        donetime = time()
    elif command_dict['type'] == 'insert data':
        initialtime = time()
        insert(command_dict['table_name'], command_dict['val'])
        donetime = time()
    elif command_dict['type'] == 'delete_index':
        initialtime = time()
        drop_index(command_dict['index_name'])
        donetime = time()
    elif command_dict['type'] == 'create_index':
        initialtime = time()
        create_index(command_dict['new_index'])
        donetime = time()
    else:
        print('Error: unknown command ')
    executetime = str(donetime - initialtime)
    print('Excute time: ' + executetime)