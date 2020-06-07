import sqlite3
from sqlite3 import Error
from data_base_func import *

db_file = r'data.db'

if __name__ == '__main__':
    
    while True:
        op = menu()
        if op == 0:
            break
        elif op == 1:
            conn = create_connection(db_file)
        elif op == 2:
            conn = main_create(db_file)
        elif op == 3:
            conn = main_insert(db_file)
        elif op == 4:
            conn = main_update(db_file)
        elif op == 5:
            conn = main_select(db_file)
        elif op == 6:
            conn = main_delete(db_file)
        else:
            print('ERRO: Opção inválida')
        
if conn:
    conn.close()
