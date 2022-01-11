###### Here_is_volleyball DB CRUD Module #####

# python package - pip install pymysql
import pymysql
from datetime import datetime

# 1. db connection
def connect():
    # db connection
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='1234', db='vleague_women', charset='utf8')
    #print('1. MySQL DB connection ', conn)
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
    #print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

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
    #print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

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
def update_matches(vo, target, id):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()
    #print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # Build sql sentence
    sql=f'update {target} set '

    for i in list(zip(vo.keys(), vo.values())):
        new=f"{i[0]} = '{str(i[1])}',"
        sql=sql+new

    sql=sql[0:-1]+f" where match_set= '{id}'"

    # Update DB
    result = cur.execute(sql)
    print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)

    # Disconnect DB
    disconnect(conn)

# 6. insert touch info into liverecord table
def create_liverecord(info_list, conn):
    cur = conn.cursor()
    #print('2. DB connection stream을 접글할 수 있는 객체 획득 성공 ', cur)

    # Create - list case
    sql='''
    insert into liverecord (touch_player,touch_player_name,touch_player_position,
    touch_type,touch_type_detail,touch_result,who_touch,liverecord_id,
    match_set,rally_num,current_score_H,current_score_A,serve_team,receive_team,
    rally_win, rally_lose) values 
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''

    result=cur.execute(sql, info_list)
    #print('3. sql문을 만들어서 mysql로 보낸후 결과 ', result)
    if result < 1:
        print('error on create_liverecord', info_list)
    else:
        print(info_list,'- created liverecord')
    #print(time.time())

# 7. insert rally & touch count info into liverecord table
def update_count_info(count_df):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()

    # update
    sql = "update liverecord set rally_count=%s, touch_num=%s, touch_1ahead=%s, touch_2ahead=%s, touch_1behind=%s where liverecord_id = %s"

    for i in range(count_df.shape[0]):
        result = cur.execute(sql, list(count_df.iloc[i]))

    # Disconnect DB
    disconnect(conn)

# 8. Update - teamerror from matches
def update_teamerror(data_list):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()

    # update
    for i in data_list:
        sql="update matches set teamerror_HT= %s, teamerror_AT = %s where match_set = %s"
        result = cur.execute(sql, i)

    # Disconnect DB
    disconnect(conn)

# 9. update - touch_result for each attach attempt
def update_touch_result(update_list):
    # Call db connection func.
    conn = connect()
    cur = conn.cursor()

    # update
    for i in update_list:
        sql="update liverecord set touch_result = %s where liverecord_id = %s"
        result = cur.execute(sql, i)

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