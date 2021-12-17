# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import kovo_crawling

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

if __name__ == '__main__':
    # 일단 한 세트의 데이터만 가져와서 테스트 해보기
    address ='https://www.kovo.co.kr/media/popup_result.asp?season=017&g_part=201&r_round=1&g_num=2&r_set=1'
    rotation_HT, rotation_AT=kovo_crawling.get_basic_info(address)
    print(rotation_HT)
    print(rotation_AT)

    rally_info=kovo_crawling.get_detail(address)

    # 이후 작업은...
    # 실시간 interactive query webservice?
    # 선수 평가 시스템 - deep learning 기반 득점 기여도 환산?
    # 단순 선형 회귀 계수와 deep learning 기반 측정 결과 비교?