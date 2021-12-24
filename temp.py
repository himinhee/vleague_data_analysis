import kovo_crawling
import dao
import pandas as pd
import numpy as np

def gather_cases(address):
    rally_data = kovo_crawling.get_detail(address)
    exceptions = ['팀실패', '팀득점', '타임']
    cases = set()
    for n in range(1, rally_data.shape[0] - 1):
        action = ''
        if type(rally_data.iloc[n, 0]) == float:
            action = rally_data.iloc[n, 3]
        else:
            action = rally_data.iloc[n, 0]

        if action not in exceptions:
            text = action.split(' ')[1:]
            cases.add(" ".join(text))
    return cases

# sample set run을 위한 임시 정보
address='https://www.kovo.co.kr/media/popup_result.asp?season=018&g_part=201&r_round=3&g_num=112&r_set=2'
hometeam="EX"
awayteam="GS"
match_set='018_112_2'

#rally_data=kovo_crawling.get_detail(address)
#rally_data.to_csv('rally_data_sample2.csv', encoding='cp949')

rally_data=pd.read_csv('rally_data_sample2.csv', encoding='cp949', index_col=0)
print(rally_data[1:10])

# 초기값 세팅
current_score_H=0
current_score_A=0
rally_num=0
who_serve=0
serve_team=''
receive_team=''
rally_count=0
touch_num=0
touch_player=''
touch_player_name=''
touch_type=0
touch_type_detail=0
touch_result=0
previous_touch=0
following_touch=0

if type(rally_data.iloc[1,0]) == float:
    serve_team=awayteam
    receive_team=hometeam
    who_serve=1
else:
    serve_team=hometeam
    receive_team=awayteam
    who_serve=0

# Set별 중계기록을 Rally단위로 쪼개기
exceptional_cases=['경고', '교체', '세트퇴장', '투입', '타임']
team_cases=['팀실패','팀성공']

rally_data['score_HT']=rally_data['score_HT'].fillna(-1)
rally_data['score_AT']=rally_data['score_AT'].fillna(-1)

rallies=[]
rally_start=1
rally_end=0
valid_rally=True

for i in range(1,rally_data.shape[0]):
    if rally_data.iloc[i,1] >= 0:
        rally_end=i
        result_score_H=int(rally_data.iloc[i,1])
        result_score_A=int(rally_data.iloc[i,2])

        if type(rally_data.iloc[i,0]) != float:
            HT=rally_data.iloc[i,0].split(' ')
            if HT[-1] in exceptional_cases:
                valid_rally=False

        if type(rally_data.iloc[i,3]) != float:
            AT=rally_data.iloc[i,3].split(' ')
            if AT[-1] in exceptional_cases:
                valid_rally=False

        if rally_start==rally_end:
            valid_rally=False

        if valid_rally == True:
            rallies.append([rally_start,rally_end,current_score_H,current_score_A ])

        rally_start=i+1
        current_score_H=result_score_H
        current_score_A=result_score_A
        valid_rally=True

# 각 Rally에 대해 Touch별로 쪼개서 정보 저장
for i in rallies:
    print(i)
