##############################################################################
# 목적 : 2020-2021 여자부 전경기 KOVO 문자중계 기록 Crawling module
# Function : get_basic_info(address), get_detail(address)
# Input : 크롤링 대상 경기 리스트
# Process : kovo_crawling.py 호출을 통해 경기별로 경기기록 정보 받기
# Output : SQL DB에 경기기록 저장
# Version : 0.5 - 2021-12-17
# By 이민희
##############################################################################

# 0. DB CRUD module, kovo 실시간 중계 crawling module 사용
import dao
import kovo_crawling
import pandas as pd
import create_liverecord

# 1. DB의 경기 리스트 가져오기
match_set=dao.read_column("matches", ["match_set","round", "hometeam", "awayteam"])

# 2. 정규시즌 경기리스트를 이용하여 crawling 대상 사이트 주소 리스트 생성

# 2021-2022 season : season=017, 2021-2022 season : season=018
# g_part=201
# r_round=1~6
# g_num= 남자부는 홀수, 여자부는 짝수 (라운드당 남자부는 21경기, 여자부는 15경기) -> db에 저장된 정보 이용
# address 예시='https://www.kovo.co.kr/media/popup_result.asp?season=018&g_part=201&r_round=1&g_num=2&r_set=1'

match_list=[]
for i in match_set:
    if i[1][1]=='R':
        codes=i[0].split('_')
        address='https://www.kovo.co.kr/media/popup_result.asp?season={}&g_part=201&r_round={}&g_num={}&r_set={}'.format(codes[0],i[1][0],codes[1],codes[2])
        match_list.append([address,i[0],i[2],i[3]])
    elif i[1]=='PO':
        codes=i[0].split('_')
        address='https://www.kovo.co.kr/media/popup_result.asp?season={}&g_part=202&r_round=1&g_num={}&r_set={}'.format(codes[0],codes[1],codes[2])
        match_list.append([address,i[0],i[2],i[3]])
    elif i[1]=='CS':
        codes=i[0].split('_')
        address='https://www.kovo.co.kr/media/popup_result.asp?season={}&g_part=203&r_round=1&g_num={}&r_set={}'.format(codes[0],codes[1],codes[2])
        match_list.append([address,i[0],i[2],i[3]])

# 3. Crawling을 통해 경기 일반 정보를 가져와서 DB 저장하는 함수 정의
def get_match_info(address, id, hometeam, awayteam):
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
        key_HT2=key_HT+"_name"
        key_AT2=key_AT+"_name"
        add_matches[key_HT]=rot_HT.iloc[i-1,3]
        add_matches[key_AT]=rot_AT.iloc[i-1,3]
        add_matches[key_HT2]=rot_HT.iloc[i-1,1]
        add_matches[key_AT2]=rot_AT.iloc[i-1,1]

    # libero 정보 추가
    add_matches['home_li1']=rot_HT.iloc[6,3]
    add_matches['away_li1']=rot_AT.iloc[6,3]
    add_matches['home_li1_name']=rot_HT.iloc[6,1]
    add_matches['away_li1_name']=rot_AT.iloc[6,1]

    if len(rot_HT.index)>7:
        add_matches['home_li2'] = rot_HT.iloc[7, 3]
        add_matches['home_li2_name'] = rot_HT.iloc[7, 1]
    if len(rot_AT.index)>7:
        add_matches['away_li2'] = rot_AT.iloc[7, 3]
        add_matches['away_li2_name'] = rot_AT.iloc[7,1]

    # 3-4 "matches" table update
    dao.update_matches(add_matches, "matches", id)

# 4. Crawling을 통해 실시간 중계 정보를 가져와서 DB 저장하는 함수 정의
# create_liverecord.py 참조

# 5. 정규시즌 경기리스트, 개별 경기 crawling function(from #3)을 이용하여 matches table update
# 6. 정규시즌 경기리스트, 개별 경기 crawling function(from #4)을 이용하여 liverecord table update
for i in match_list:
    address=i[0]
    match_set=i[1]
    hometeam=i[2]
    awayteam=i[3]
    print('liverecord: ',match_set)
#    get_match_info(address,match_set_id,hometeam,awayteam)
    create_liverecord.get_liverecord(address, match_set, hometeam, awayteam)

# 7. liverecord update

# rally_count : 1점을 내기까지의 rally 갯수 및 몇번째 rally인지 count
# touch_num : 1개 rally 내에 몇번째 touch인지 정보
# touch_1ahead : 바로 이전 touch가 같은 rally 안에 있을 경우, 그 touch_id 저장
# touch_2ahead : 2step 전의 touch가 같은 rally 안에 있을 경우, 그 touch_id 저장
# touch_1behind : 바로 다음 touch가 같은 rally 안에 있는 경우, 그 touch_id 저장
def add_count_info(match_set, hometeam, awayteam,df):
    # liverecord table 내 해당 경기 정보
    target_record = df[df.match_set == match_set].copy()
    target_record.reset_index(inplace=True)
    target_record.drop('index', axis=1, inplace=True)

    # 초기값 세팅
    rally_count = 0
    touch_num = 0
    who_touch = target_record.iloc[0, 3]
    previous_rally_num = 1
    data_size=target_record.shape[0]
    for i in range(data_size):
        new_rally_num= target_record.iloc[i, 2]
        if previous_rally_num != new_rally_num:
            rally_count=0
            touch_num=0
            previous_rally_num = new_rally_num
            if target_record.iloc[i-1,-1]==hometeam:
                who_touch=0
            else:
                who_touch=1

        new_touch = target_record.iloc[i, 3]
        # who_touch가 이전과 같으면 touch_num만 증가. rally_count는 그대로
        if who_touch==new_touch:
            touch_num=touch_num+1
        # who_touch가 이전과 다르면 touch_num은 다시 1, rally_count는 증가
        elif target_record.iloc[i,4]=='40':
            touch_num=0
            rally_count=rally_count+1
            who_touch = new_touch
        else:
            touch_num=1
            rally_count=rally_count+1
            who_touch = new_touch

        target_record.iloc[i,5] = rally_count
        target_record.iloc[i,6] = touch_num

        if (i > 0 ) & (target_record.iloc[i,2]==target_record.iloc[i-1,2]):
            target_record.loc[i,'touch_1ahead']=target_record.loc[i-1,'liverecord_id']

        if (i > 1 ) & (target_record.iloc[i,2]==target_record.iloc[i-2,2]):
            target_record.loc[i,'touch_2ahead']=target_record.loc[i-2,'liverecord_id']

        if (i < data_size - 1):
            this=target_record.iloc[i,2]
            next=target_record.iloc[i+1,2]
            if this == next:
                target_record.loc[i,'touch_1behind']=target_record.loc[i+1,'liverecord_id']
    new_record=target_record[['rally_count','touch_num','touch_1ahead','touch_2ahead','touch_1behind','liverecord_id']]
    new_record.fillna("", inplace=True)
    return new_record

# 전체 liverecord 가져오기
rows = dao.read_column('liverecord',
                       ['liverecord_id', 'match_set', 'rally_num', 'who_touch', 'touch_result', 'rally_count',
                        'touch_num', 'rally_win', 'touch_2ahead', 'touch_1ahead', 'touch_1behind'])
liverecord_df = pd.DataFrame(rows,
                 columns=['liverecord_id', 'match_set', 'rally_num', 'who_touch', 'touch_result', 'rally_count','touch_num', 'rally_win', 'touch_2ahead', 'touch_1ahead', 'touch_1behind'])

# 경기별로 rally & touch info 함수 호출 및 dataframe에 저장
count_df=pd.DataFrame()
for i in match_list:
    match_set=i[1]
    hometeam=i[2]
    awayteam=i[3]
    print('rally count update: ',match_set)
    target_record=add_count_info(match_set, hometeam, awayteam,liverecord_df)
    count_df=pd.concat([count_df, target_record])

dao.update_count_info(count_df)

# 8. match_set 별 팀범실(팀 포지션폴트) 산출 및 matches에 저장
teamerror_num = []
for i in match_list:
    address = i[0]
    match_set = i[1]
    hometeam = i[2]
    awayteam = i[3]
    print(address)
    # Crawling 및 결측치 처리
    rally_data = kovo_crawling.get_detail(address)
    rally_data['score_HT'] = rally_data['score_HT'].apply(lambda x: -1 if x == "" else x)
    rally_data['score_AT'] = rally_data['score_AT'].apply(lambda x: -1 if x == "" else x)
    rally_data['score_HT'] = rally_data['score_HT'].astype(int)
    rally_data['score_AT'] = rally_data['score_AT'].astype(int)

    # 팀범실 찾기
    teamerror_HT = rally_data[rally_data.action_HT == '팀 포지션폴트']['action_HT']
    teamerror_AT = rally_data[rally_data.action_AT == '팀 포지션폴트']['action_AT']
    teamerror_num.append([match_set, len(teamerror_HT), len(teamerror_AT)])

# 9. liverecord 내 touch_situation 정보 update
# 9.1 공격 차단 & 공격 유효브로킹 & 공격 디그 구분
# case 1) 공격 차단 : 공격 시도(touch_type=1 & touch_result=00)인 경우, touch_1behind가 있고 그 touch_type=4 & touch_result=10 => touch_result=30 으로 update
# case 2) 공격이 유효블로킹된 경우 : 공격 시도(touch_type=1 & touch_result=00)인 경우, touch_1behind가 있고 그 touch_type=4 & touch_result=50 => touch_result=40 으로 update
# case 3) 공격이 디그된 경우 : 공격 시도(touch_type=1 & touch_result=00)인 경우, touch_1behind가 있고 그 touch_type=2 & touch_result=10 => touch_result=60 으로 update


# 9.2 touch_situation -> TBD


# 10. liverecord 내 rotation 정보 update


# etc. text 정보의 모든 case 찾기
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

# all_cases = set()
# for i in match_list:
#     address=i[0]
#     match_set_id=i[1]
#     hometeam=i[2]
#     awayteam=i[3]
#     new_cases=gather_cases(address)
#     all_cases=all_cases.union(new_cases)
#     print(i)
