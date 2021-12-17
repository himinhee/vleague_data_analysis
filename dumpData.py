##############################################################################
# 목적 : 2020-2021 여자부 전경기 KOVO 문자중계 기록 Crawling module
# Function : get_basic_info(address), get_detail(address)
# Input : 크롤링 대상 경기 리스트
# Process : kovo_crawling.py 호출을 통해 경기별로 경기기록 정보 받기
# Output : SQL DB에 경기기록 저장
# Version : 0.1 - 2021-12-13
# By 이민희
##############################################################################

# 0. DB CRUD module, kovo 실시간 중계 crawling module 사용
import dao
import kovo_crawling
import pandas as pd

# 1. DB의 경기 리스트 가져오기
match_set=dao.read_column("matches", ["match_set","round", "hometeam", "awayteam"])

# 2. 정규시즌 경기리스트를 이용하여 crawling 대상 사이트 주소 리스트 생성

# 2021-2022 season : season=017, 2021-2022 season : season=018
# g_part=201
# r_round=1~6
# g_num= 남자부는 홀수, 여자부는 짝수 (라운드당 남자부는 21경기, 여자부는 15경기) -> db에 저장된 정보 이용
# address 예시='https://www.kovo.co.kr/media/popup_result.asp?season=018&g_part=201&r_round=1&g_num=2&r_set=1'

address_list=[]
for i in match_set:
    if i[1][1]=='R':
        codes=i[0].split('_')
        address='https://www.kovo.co.kr/media/popup_result.asp?season={}&g_part=201&r_round={}&g_num={}&r_set={}'.format(codes[0],i[1][0],codes[1],codes[2])
        address_list.append(address)
        hometeam = i[2]
        awayteam = i[3]

# 3. Crawling을 통해 경기 정보 가져와서 DB 저장
# -> 함수화하고 address_list 반복문으로 전환해야 함!

# 3-1 Crawling으로 데이터 가져오기
scores_HT, scores_AT, rotation_HT, rotation_AT=kovo_crawling.get_basic_info(address)

# 3-2 matches table 추가 항목 : maxset score_HT score_AT match W/L set W/L
add_matches=dict()
add_matches['maxset']=scores_HT[-1]+scores_AT[-1]

if scores_HT[-1]>scores_AT[-1]:
    add_matches['match_winner']=hometeam
    add_matches['match_loser']=awayteam
else:
    add_matches['match_winner']=awayteam
    add_matches['match_loser']=hometeam

current=int(address[-1])
add_matches['score_HT']=scores_HT[current-1]
add_matches['score_AT']=scores_AT[current-1]

if add_matches['score_HT']>add_matches['score_AT']:
    add_matches['set_winner']=hometeam
    add_matches['set_loser']=awayteam
else:
    add_matches['set_winner']=awayteam
    add_matches['set_loser']=hometeam

# 3-3 matches table 추가 항목 : home rotation away rotation
# 3-3-1 DB로부터 player id 정보 가져오기
allplayers=dao.readall("code_player")
allplayers_df=pd.DataFrame(allplayers, columns=['player_id','current_team','player','backnum','position_real'])

# 3-3-2 rotation 정보에 player_id 추가
rot_HT=pd.merge(rotation_HT, allplayers_df[allplayers_df['current_team']==hometeam], on='player', how='inner')
rot_AT=pd.merge(rotation_AT, allplayers_df[allplayers_df['current_team']==awayteam], on='player', how='inner')

# 3-3-3 matches table에 추가할 내용을 add_matches에 넣기
# rotation 1~6 정보 추가
for i in range(1, 7):
    key_HT="home_rot"+str(i)
    key_AT="away_rot"+str(i)
    add_matches[key_HT]=rot_HT.iloc[i-1,3]
    add_matches[key_AT]=rot_AT.iloc[i-1,3]

# libero 정보 추가
add_matches['home_li1']=rot_HT.iloc[6,3]
add_matches['away_li1']=rot_AT.iloc[6,3]

if len(rot_HT.index)>7:
    add_matches['home_li2'] = rot_HT.iloc[7, 3]
if len(rot_HT.index)>7:
    add_matches['away_li2'] = rot_AT.iloc[7, 3]

# 3-4 "matches" table update
id=match_set[-1][0]
dao.update_matches(add_matches, "matches", id)

#rally_info=kovo_crawling.get_detail(address)