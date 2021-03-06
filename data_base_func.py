import sqlite3
from sqlite3 import Error
import datetime
from math import pi, acos, cos, sin


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
                                   accuracy int NOT NULL,
                                   info bool NOT NULL,
                                   distancia float NOT NULL,
                                   tempo time NOT NULL                                  
                                    );"""

    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                project_id integer NOT NULL,
                                data_hora datetime NOT NULL,
                                type text NOT NULL,
                                confidence int NOT NULL,
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
    sql = ''' INSERT INTO projects(data_hora,latitude,longitude,accuracy,info,distancia,tempo)
              VALUES(?,?,?,?,?,?,?) '''
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


def distancia(la1, la2, lo1, lo2):
    """
    #* Fórmula:
    #* Diâmetro da terra = 6378.137 Km
    #* coordenadas (latitude e longitude) em radianos
    #* distancia = d_terra * acos(cos(lat1) * cos(lat2) * cos(long2 - long1) + sin(lat1) * sin(lat2))
    #* acos(-1 <= x <= 1) // erro: 1.0000000000000002
    #* distancia = x Km
    """
    
    def radianos(coordenada):
            return coordenada * pi / 180
    
    d_terra = 6378.137
    la1 = radianos(la1)
    la2 = radianos(la2)
    lo1 = radianos(lo1)
    lo2 = radianos(lo2)
    
    _ = cos(la1) * cos(la2) * cos(lo2 - lo1) + sin(la1) * sin(la2)
    
    if _ == 1.0000000000000002:
        _ = 1.0
        
    _ = acos(_)  
    
    dist = d_terra * _
    dist = round(dist, 3)
    return dist


def tempo_convert(t):
    t = str(t)
    return datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')


def main_insert(db_file, json_data):
    # create a database connection
    conn = create_connection(db_file)
    with conn:
        
        count = 1
        
        for local in json_data['locations']:
            
            state1 = state2 = True
            
            try:
                _ = local['timestampMs']
            except:
                state1 = False
            else:
                _ = int(_[0:10])
                timestampMs = datetime.datetime.utcfromtimestamp(_)
            
            try:
                _ = local['latitudeE7']
            except:
                state1 = False
            else:
                latitudeE7 = float(_ / 10000000)
                        
            try:
                _ = local['longitudeE7']
            except:
                state1 = False
            else:
                longitudeE7 = float(_ / 10000000)
            
            try:
                _ = local['accuracy']
            except:
                state1 = False
            else:
                accuracy = int(_)
            
            try:
                _ = local['activity']
            except:
                state2 = False
            else:
                _ts = _[0]['timestampMs']
                _ts = int(_ts[0:10])
                tsMs = datetime.datetime.utcfromtimestamp(_ts)
                _ = _[0]['activity']
                tipo = _[0]['type']
                confidence = int(_[0]['confidence'])
            
            if state1:
                if count == 1:
                    lat1 = lat2 = latitudeE7
                    lon1 = lon2 = longitudeE7
                    dist_atual = dist_ant = 0
                    
                    tempo_atual = tempo_convert(timestampMs)
                    tempo_ant = tempo_atual
                    tempo = str(tempo_atual - tempo_ant)
                else:
                    lat1 = lat2
                    lon1 = lon2
                    lat2 = latitudeE7
                    lon2 = longitudeE7
                    dist_ant = dist_atual
                
                    tempo_atual = tempo_convert(timestampMs)
                    tempo = str(tempo_atual - tempo_ant)
                    tempo_ant = tempo_atual
                
                dist_atual = distancia(lat1, lat2, lon1, lon2)
                count += 1
                
                project = (timestampMs, latitudeE7, longitudeE7, accuracy, state2, dist_atual, tempo)
                project_id = create_project(conn, project)
            
                if state2:
                    task = (project_id, tsMs, tipo, confidence)
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
    cur.execute("SELECT * FROM tasks WHERE confidence=?", (priority,))

    rows = cur.fetchall()

    for row in rows:
        print(row)


def main_select(db_file):
    # create a database connection
    conn = create_connection(db_file)
    with conn:
        print("1. Query task by priority:")
        select_task_by_priority(conn, 100)

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
