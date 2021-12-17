###### Diary CRUD Module #####

# python package - pip install pymysql
import pymysql
from datetime import datetime

# 1. db connection
def connect():
    # db connection
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='Mindy1001!', db='vleague_women', charset='utf8')
    print('1. MySQL DB connection ', conn)
    return conn

# 2. db disconnection
def disconnect(conn):
    # mysql에서 sql명령을 commit하도록 하고 연결 닫기
    conn.commit()
    conn.close()

# 3. Read all from target table function
def readall(target):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # read
    sql="select * from "+target
    result = cur.execute(sql)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)
    rows=cur.fetchall()

    # Disconnect DB
    disconnect(conn)
    return rows

# 4. Read specific columns from target table function
def read_column(target, column_list):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # Read
    columns =""
    if len(column_list)>1:
        for i in column_list:
            columns=columns+i+","
        columns=columns[:-1]
    else:
        columns=column_list[0]
    sql="select "+columns+" from "+target
    result = cur.execute(sql)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)
    rows=cur.fetchall()

    # Disconnect DB
    disconnect(conn)
    return rows

# 5. Update match info
def update_matches(vo, rotation_HT, rotation_AT, id):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # update
    sql="update matches set "

    for i in list(zip(vo.keys(), vo.values())):
        sql=sql+i[0]+"="+str(i[1])+","

    for i in range(1,7):
        sql=sql+"home_rot"+str(i)+"="+rotation_HT.iloc[i-1,1]+","
        sql=sql+"away_rot"+str(i)+"="+rotation_AT.iloc[i-1,1]+","

    sql=sql+"home_li1="+rotation_HT.iloc[6,1]+","+"away_li1="+rotation_AT.iloc[6,1]+","

    if rotation_HT.iloc[7,1] !="":
        sql=sql+"home_li2="+rotation_HT.iloc[7,1]+","
    if rotation_AT.iloc[7,1] !="":
        sql=sql+"away_li2="+rotation_AT.iloc[7,1]+","

    sql=sql[0:-1]+" where match_set= '"+id+"'"
    print(sql)

    result = cur.execute(sql)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)

    # Disconnect DB
    disconnect(conn)


# 3. create function
def create(vo):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # Create - list case
    sql="insert into diary (writeday, title, content) values (NOW(), %s, %s)"
    result=cur.execute(sql, vo)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)

    # Disconnect DB
    disconnect(conn)

# 4. update function
def update(vo):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # update
    sql="update diary set title= %s, content = %s where id = %s"
    result = cur.execute(sql, vo)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)

    # Disconnect DB
    disconnect(conn)

# 5. delete function
def delete(vo):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # delete
    sql="delete from diary where id = %s"
    result = cur.execute(sql, vo)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)

    # Disconnect DB
    disconnect(conn)

# 6. Read specific id function
def read(vo):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # read
    sql="select * from diary where id = %s"
    result = cur.execute(sql, vo)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)
    row=cur.fetchone()

    # Disconnect DB
    disconnect(conn)
    return row