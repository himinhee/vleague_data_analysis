##############################################################################
# 목적 : 실시간 KOVO 문자중계 기록 Crawling module
# Main Functions : get_basic_info(address), get_detail(address)
# Input parameter : 대상 사이트 http address
# Output : rotation info(dataframe), 중계기록(dataframe)
# Version : 0.1 - 2021-12-07
# By 이민희
##############################################################################
from bs4 import BeautifulSoup
import requests
import pandas as pd

# Function - Get html data from the target http address
def get_html(address):
    source = requests.get(address)
    soup = BeautifulSoup(source.content, 'html.parser')
    return soup

# Function - Crawling & Return team & set rotation info
def get_basic_info(address):
    # 1. Get html data from the target http address
    soup = get_html(address)

    # 2. Set & Team & Rotation data
    match_data = soup.find('table','inner_table').find_all('tr')
    data = soup.find('div', 'wrp_live_bottom').find('div', 'center')
    position = data.find('div', 'position').find_all('ul')

    # 3. Set & Team info -> list
    home_team=match_data[1].find_all('td')
    away_team=match_data[2].find_all('td')

    teams = []
    HT_score=[]
    AT_score=[]

    # Team info
    teams.append(home_team[0].text)
    teams.append(away_team[0].text)

    # Set info
    for i in range(1,7):
        HT_score.append(int(home_team[i].text))
        AT_score.append(int(away_team[i].text))

    # 4. Rotation info -> dataframe
    # body > div class=wrp_live > div class=wrp_live_bottom > div class=center > div id=tab1 > div class=position
    # 1) Home Team Libero Info : ul class=p_li li01 > li
    # 2) Away Team Libero Info : ul class=p_li li02 > li
    # 3) HT rotation 후위(5,6,1번) : ul class=p_left left01
    # 4) HT rotation 전위(4,3,2번) : ul class=p_left left02
    # 5) AT rotation 전위(2,3,4번) : ul class=p_right right02
    # 6) AT rotation 후위(1,6,5번) : ul class=p_right right01

    # Home team libero info
    temp = []
    for i in position[0].find_all('li'):
        temp.append(i.text.split(':')[1].strip())

    libero_HT = pd.DataFrame(list(zip(['li1', 'li2'], temp)), columns=['position', 'player'])

    # Away team libero info
    temp = []
    for i in position[1].find_all('li'):
        temp.append(i.text.split(':')[1].strip())

    libero_AT = pd.DataFrame(list(zip(['li1', 'li2'], temp)), columns=['position', 'player'])

    # position의 rotation 번호 순서
    dataorder_HT = [5, 6, 1, 4, 3, 2]
    dataorder_AT = [2, 3, 4, 1, 6, 5]

    # HT 의 rotation 정보는 position 의 3 & 4 번째 데이터
    temp = []
    for i in position[2].find_all('li'):
        temp.append(i.text)
    for i in position[3].find_all('li'):
        temp.append(i.text)

    order_HT = pd.DataFrame(list(zip(dataorder_HT, temp)), columns=['position', 'player'])
    order_HT.sort_values(by='position', inplace=True)

    # AT 의 rotation 정보는 position 의 5 & 6 번째 데이터
    temp = []
    for i in position[4].find_all('li'):
        temp.append(i.text)
    for i in position[5].find_all('li'):
        temp.append(i.text)

    order_AT = pd.DataFrame(list(zip(dataorder_AT, temp)), columns=['position', 'player'])
    order_AT.sort_values(by='position', inplace=True)

    rotation_HT = pd.concat([order_HT, libero_HT])
    rotation_AT = pd.concat([order_AT, libero_AT])

    rotation_HT['team']=teams[0]
    rotation_AT['team']=teams[1]

    rotation_HT=rotation_HT.reset_index()
    rotation_AT=rotation_AT.reset_index()

    rotation_HT.drop('index', axis=1, inplace=True)
    rotation_AT.drop('index', axis=1, inplace=True)

    return HT_score, AT_score, rotation_HT, rotation_AT

# Crawling & Return set detail info
def get_detail(address):
    # 1. Get html data from the target http address
    soup = get_html(address)
    data = soup.find('div', 'wrp_live_bottom').find('div', 'center')
    rally_info=data.find('div','wrp_liverecord').find('div','wrp_con').find_all('li')

    # 2. html to dataframe
    # 각 li tag 안에는 4개의 spam tagg 존재
    # span 1 : hometeam touch info
    # span 2 : hometeam score
    # span 3 : awayteam score
    # span 4 : awayteam touch info

    data=[]
    for i in rally_info:
        line=i.find_all('span')
        temp=[]
        for j in line:
            temp.append(j.text.strip())
        data.append(temp)

    rally_df=pd.DataFrame(data, columns=['action_HT','score_HT','score_AT','action_AT'])
    return rally_df

    # 랠리와 랠리의 구분 : 득점 행이 null이 아닌 경우

    # Replay 처리 로직 필요 ; Replay의 경우, 이전 rally 정보를 지우고 원복해야 함

