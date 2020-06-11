import sqlite3
from sqlite3 import Error
import datetime


#! ***** MENU *****

def menu():
    linha(40)
    print('--- MENU ---')
    linha(40)
    print('1- Criar db')
    print('2- Criar tabelas')
    print('3- Insere dados')
    print('4- Atualizar dados')
    print('5- Selecionar dados')
    print('6- Apagar dados')
    linha(40)
    print('0- sair do programa')
    linha(40)
    opcao = int(input('Opção -> '))
    linha(40)
    return opcao


def linha(tam=120):
    print('-' * tam)


#! ***** MENU 1 *****

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f'SQLite v{sqlite3.version}')
        return conn
    except Error as e:
        print(e)
    
    return conn

#! ***** MENU 2 *****

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def main_create(db_file):
    sql_create_projects_table = """CREATE TABLE IF NOT EXISTS projects (
                                   id integer PRIMARY KEY,
                                   data_hora datetime NOT NULL,
                                   latitude float NOT NULL,
                                   longitude float NOT NULL,
                                   accuracy tinyint NOT NULL
                                    );"""

    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                project_id integer NOT NULL,
                                data_hora datetime NOT NULL,
                                type tinytext NOT NULL,
                                confidence tinyint NOT NULL,
                                FOREIGN KEY (project_id) REFERENCES projects (id)
                                );"""

    # create a database connection
    conn = create_connection(db_file)

    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_projects_table)

        # create tasks table
        create_table(conn, sql_create_tasks_table)
    else:
        print("Error! cannot create the database connection.")
    
    return conn

#! ***** MENU 3 *****

def create_project(conn, project):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' INSERT INTO projects(data_hora,latitude,longitude,accuracy)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, project)
    return cur.lastrowid


def create_task(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = ''' INSERT INTO tasks(project_id,data_hora,type,confidence)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, task)
    return cur.lastrowid


def main_insert(db_file, json_data):
    # create a database connection
    conn = create_connection(db_file)
    with conn:
        
        for local in json_data['locations']:
            
            state = True
            
            try:
                _ = local['timestampMs']
            except:
                state = False
            else:
                _ = int(_[0:10])
                timestampMs = datetime.datetime.utcfromtimestamp(_)
            
            try:
                _ = local['latitudeE7']
            except:
                state = False
            else:
                latitudeE7 = float(_ / 10000000)
                        
            try:
                _ = local['longitudeE7']
            except:
                state = False
            else:
                longitudeE7 = float(_ / 10000000)
            
            try:
                _ = local['accuracy']
            except:
                state = False
            else:
                accuracy = int(_)
            
            if state:
                project = (timestampMs, latitudeE7, longitudeE7, accuracy)
                project_id = create_project(conn, project)
            
            state = True
                    
            try:
                _ = local['activity']
            except:
                state = False
            else:
                _ts = _[0]['timestampMs']
                _ts = int(_ts[0:10])
                timestampMs = datetime.datetime.utcfromtimestamp(_ts)
                
                _ = _[0]['activity']
                tipo = _[0]['type']
                confidence = int(_[0]['confidence'])
                    
            if state:
                task = (project_id, timestampMs, tipo, confidence)
                create_task(conn, task)
                    
    return conn

#! ***** MENU 4 *****

def update_task(conn, task):
    """
    update priority, begin_date, and end date of a task
    :param conn:
    :param task:
    :return: project id
    """
    sql = ''' UPDATE tasks
              SET priority = ? ,
                  begin_date = ? ,
                  end_date = ?
              WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()


def main_update(db_file):
    # create a database connection
    conn = create_connection(db_file)
    with conn:
        update_task(conn, (2, '2015-01-04', '2015-01-06', 2))
    return conn

#! ***** MENU 5 *****

def select_all_tasks(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks")

    rows = cur.fetchall()

    for row in rows:
        print(row)


def select_task_by_priority(conn, priority):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE priority=?", (priority,))

    rows = cur.fetchall()

    for row in rows:
        print(row)


def main_select(db_file):
    # create a database connection
    conn = create_connection(db_file)
    with conn:
        print("1. Query task by priority:")
        select_task_by_priority(conn, 1)

        print("2. Query all tasks")
        select_all_tasks(conn)
    return conn

#! ***** MENU 6 *****

def delete_task(conn, id):
    """
    Delete a task by task id
    :param conn:  Connection to the SQLite database
    :param id: id of the task
    :return:
    """
    sql = 'DELETE FROM tasks WHERE id=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()


def delete_all_tasks(conn):
    """
    Delete all rows in the tasks table
    :param conn: Connection to the SQLite database
    :return:
    """
    sql = 'DELETE FROM tasks'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def main_delete(db_file):
    # create a database connection
    conn = create_connection(db_file)
    with conn:
        delete_task(conn, 2);
        # delete_all_tasks(conn);
    return conn
    
#! ***** json_data *****

count = 0

def json_read(json_data):

    for local in json_data['locations']:
        
        state = True
        
        try:
            _ = local['timestampMs']
        except:
            state = False
        else:
            _ = int(_[0:10])
            timestampMs = datetime.datetime.utcfromtimestamp(_)
        
        try:
            _ = local['latitudeE7']
        except:
            state = False
        else:
            latitudeE7 = float(_ / 10000000)
            
                    
        try:
            _ = local['longitudeE7']
        except:
            state = False
        else:
            longitudeE7 = float(_ / 10000000)
        
        try:
            _ = local['accuracy']
        except:
            state = False
        else:
            accuracy = int(_)
                
        """
        try:
            _ = local['activity']
        except:
            activity1.append(None)
            activity2.append(None)
            activity3.append(None)
            
        else:
            _ts = _[0]['timestampMs']
            _ts = int(_ts[0:10])
            _ts = datetime.datetime.utcfromtimestamp(_ts)
            activity1.append(_ts)
            _ = _[0]['activity']
            activity2.append(_[0]['type'])
            activity3.append(_[0]['confidence'])
        """
        if state:
            pass
    
    """
    (timestampMs, latitudeE7, longitudeE7, accuracy)
    """
