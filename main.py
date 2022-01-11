# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import dao, kovo_crawling
import pandas as pd

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # 9.1 공격 차단 & 공격 유효브로킹 & 공격 디그 구분
    # case 1) 공격 차단 : 공격 시도(touch_type=1 & touch_result=00)인 경우, touch_1behind가 있고 그 touch_type=4 & touch_result=10 => touch_result=30 으로 update
    # case 2) 공격이 유효블로킹된 경우 : 공격 시도(touch_type=1 & touch_result=00)인 경우, touch_1behind가 있고 그 touch_type=4 & touch_result=50 => touch_result=40 으로 update
    # case 3) 공격이 디그된 경우 : 공격 시도(touch_type=1 & touch_result=00)인 경우, touch_1behind가 있고 그 touch_type=2 & touch_result=10 => touch_result=60 으로 update

    rows=dao.read_column('liverecord',['liverecord_id','touch_type','touch_type_detail','touch_result','touch_1behind'])
    df=pd.DataFrame(data=rows, columns=['liverecord_id','touch_type','touch_type_detail','touch_result','touch_1behind'])

    update_list=[]
    for i in range(df.shape[0]):
        if (df.loc[i,'touch_type']==1) & (df.loc[i,'touch_result']=='00') & (df.loc[i, 'touch_1behind']!=""):
            id=df.iloc[i,0]
            next_touch_type=df.loc[i+1, 'touch_type']
            next_touch_result=df.loc[i+1,'touch_result']
            if (next_touch_type==4) & (next_touch_result=='10'):
                df.loc[i,'touch_result']='30'
                update_list.append(['30',id])
            elif (next_touch_type==4) & (next_touch_result=='50'):
                df.loc[i,'touch_result']='40'
                update_list.append(['40',id])
            elif (next_touch_type==2) & (next_touch_result=='10'):
                df.loc[i,'touch_result']='60'
                update_list.append(['60',id])
    dao.update_touch_result(update_list)