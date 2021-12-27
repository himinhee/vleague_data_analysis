import kovo_crawling
import dao
import pandas as pd

# sample set run을 위한 match_list 임시 데이터 -> rally_data crawling
address='https://www.kovo.co.kr/media/popup_result.asp?season=017&g_part=201&r_round=1&g_num=2&r_set=1'
hometeam="HD"
awayteam="GS"
match_set='017_2_1'

# Crawing 대상 경기의 기본 정보를 받아 Crawing 호출, 데이터 정형화, DB insert까지 수행
def get_liverecord(address, match_set, hometeam, awayteam):
    # http address를 이용하여 liverecord crawling 수행 - 함수 호출
    rally_data = kovo_crawling.get_detail(address)

    # 중계 기록 중 각 action에 대한 code table 가져오기
    rows=dao.read_column('code_touch',['Text','touch_type','touch_type_detail','touch_result'])
    code_touch=pd.DataFrame(data=rows, columns=['text','touch_type','touch_type_detail','touch_result'])

    # 대상 시즌 해당 경기 팀의 선수 정보 가져오기
    rows=dao.readall('player_current')
    players=pd.DataFrame(data=rows, columns=['player_id', 'current_team', 'name', 'backnum', 'position_real'])
    players=players[(players.current_team==hometeam) | (players.current_team==awayteam)]

    # 결측치 및 기타 데이터 전처리
    rally_data['score_HT']=rally_data['score_HT'].apply(lambda x: -1 if x=="" else x)
    rally_data['score_AT']=rally_data['score_AT'].apply(lambda x: -1 if x=="" else x)
    rally_data['score_HT']=rally_data['score_HT'].astype(int)
    rally_data['score_AT']=rally_data['score_AT'].astype(int)
    code_touch.text=code_touch.text.apply(lambda x: x.strip())

    # 초기값 세팅
    current_score_H=0
    current_score_A=0
    rally_num=0
    who_touch=0
    serve_team=''
    receive_team=''
    rally_count=0
    touch_num=0
    touch_player=''
    touch_player_name=''
    touch_type=0
    touch_type_detail=0
    touch_result=0

    # 세트의 첫 서브팀 확인 - who_touch=0: Hometeam serve로 시작
    if rally_data.iloc[1,0] == "":
        who_touch=1
    else:
        who_touch=0

    # Set별 중계기록을 Rally단위로 쪼개기
    exceptional_cases=['경고', '교체', '세트퇴장', '투입', '타임']
    team_cases=['팀실패','팀성공', '팀득점', '팀 포지션폴트', '포지션폴트']

    rallies=[]
    rally_start=1
    rally_end=0
    valid_rally=True
    idx=0
    for i in range(1,rally_data.shape[0]):
        if rally_data.iloc[i,1] >= 0:
            rally_end=i
            result_score_H=rally_data.iloc[i,1]
            result_score_A=rally_data.iloc[i,2]

            if rally_data.iloc[i,0] != "":
                HT=rally_data.iloc[i,0].split(' ')
                if HT[-1] in exceptional_cases:
                    valid_rally=False

            if rally_data.iloc[i,3] != "":
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

    # Touch의 Text 정보를 이용하여 선수 및 touch 정보를 반환
    def get_touch_info(info_list, team, players, code_touch):
        touch_player_num = int(info_list[j].split(' ')[0].split('.')[0])
        touch_player_name = info_list[j].split(' ')[0].split('.')[1]
        touch_player = players[(players['current_team'] == team) & (players['backnum'] == touch_player_num)]['player_id'].tolist()[0]
        touch_player_position = players[(players['current_team'] == team) & (players['backnum'] == touch_player_num)]['position_real'].tolist()[0]
        touch_text = " ".join(info_list[j].split(' ')[1:])
        touch = code_touch[code_touch['text'] == touch_text][['touch_type', 'touch_type_detail','touch_result']]
        touch_type = touch.iloc[0, 0]
        touch_type_detail = touch.iloc[0, 1]
        touch_result = touch.iloc[0, 2]
        touch_info=[touch_player, touch_player_name, touch_player_position, touch_type, touch_type_detail, touch_result]
        return touch_info

    # 각 Rally에 대해 Touch별로 쪼개서 정보 DB에 저장
    for i in rallies:
        current_score_H = i[2]
        current_score_A = i[3]
        rally_num=rally_num+1
        rally_count = 0
        touch_num = 0
        HT=rally_data[i[0]:i[1]+1].action_HT.tolist()
        AT=rally_data[i[0]:i[1]+1].action_AT.tolist()

        if who_touch==0:
            serve_team=hometeam
            receive_team=awayteam
        else:
            serve_team=awayteam
            receive_team=hometeam

        new_score_HT = rally_data.iloc[i[1], 1]
        new_score_AT = rally_data.iloc[i[1], 2]

        if current_score_A < new_score_AT:
            rally_win=awayteam
            rally_lose=hometeam
        else:
            rally_win=hometeam
            rally_lose=awayteam

        touch_info=[]
        other_info=[match_set, rally_num, current_score_H, current_score_A, serve_team, receive_team, rally_win, rally_lose]
        for j in range(len(HT)):
            if (HT[j]!="") & (HT[j] not in team_cases):
                idx = idx + 1
                record_id=match_set+'_'+"{0:0=3d}".format(idx)
                who_touch=0
                print(HT[j])
                touch_info= get_touch_info(HT, hometeam, players, code_touch)
                touch_info.append(who_touch)
                touch_info.append(record_id)
                info_list = touch_info + other_info
                dao.create_liverecord(info_list)

            elif (AT[j]!="") & (AT[j] not in team_cases):
                idx = idx + 1
                record_id=match_set+'_'+"{0:0=3d}".format(idx)
                who_touch=1
                print(AT[j])
                touch_info= get_touch_info(AT, awayteam, players, code_touch)
                touch_info.append(who_touch)
                touch_info.append(record_id)
                info_list = touch_info + other_info
                dao.create_liverecord(info_list)

            rally_count=rally_count+1
            touch_num=touch_num+1

        # 다음 rally의 serve team 찾기
        if current_score_A < new_score_AT:
            who_touch=1
        else:
            who_touch=0