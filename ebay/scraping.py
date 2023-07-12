import os
import sys
import re
import time
import math
import requests
from requests.sessions import Session
from requests.models import Response
from urllib.robotparser import RobotFileParser
import unicodedata
import urllib.parse
import traceback
from typing import Union

import pandas as pd
from pandas.core.frame import DataFrame
from bs4 import BeautifulSoup

DOWNLOAD_DELAY = 2
BASE_URL = 'https://www.ebay.com/sch/i.html'
INPUT_PATH = 'inputs/keyboard_list.xlsx'
OUTPUT_JL_PATH = 'outputs/results.jl'
OUTPUT_PATH = 'outputs/results.xlsx'
OUTPUT_COLUMNS = ['maker', 'model number', 'title', 'condition', 'price', 'postage', 'import fees',
                'duty', 'url']
HTML_PARSER = 'lxml'
ITEM_NUM_IN_PAGE = 60

# robots.txtの読み込み
rp = RobotFileParser()
ROBOTS_URL = urllib.parse.urljoin(BASE_URL, '/robots.txt')
rp.set_url(ROBOTS_URL)
rp.read()
print(f'Read robots.txt: "{ROBOTS_URL}"')

def get_response(session: Session, url: str, success_message: str='',
                 delay: Union[int, float]=DOWNLOAD_DELAY):
    user_agent = session.headers['User-Agent']
    if rp.can_fetch(useragent=user_agent, url=url):
        try:
            response = session.get(url)
            response.raise_for_status()
            if success_message:
                print(success_message)
            time.sleep(delay)
            return response
        except requests.exceptions.RequestException as e:
            print(f'Error: {e} (URL "{url}")')
            return False
    else:
        print(f'URL "{url}" is prohibitied by robots.txt.')
        return False


def main():
    search_criteria_list = read_excel(INPUT_PATH)

    with requests.Session() as session:
        for search_criteria in search_criteria_list:
            if response := get_list_page(session, search_criteria):
                detail_urls = fetch_detail_urls(session, response)
                item_infos = fetch_item_infos(session, detail_urls)
                item_infos = modify_item_infos(item_infos, search_criteria)
                overwrite_jl(OUTPUT_JL_PATH, item_infos)

    write_excel(OUTPUT_PATH, OUTPUT_JL_PATH)
        #search_criteria = {'keyword': 'kawai', '最低価格': '10000'}
        #response = get_list_page(session, search_criteria)
        #detail_urls = fetch_detail_urls(session, response)
        #item_infos = fetch_item_infos(session, detail_urls)


def read_excel(input_path: str) -> list[dict]:
    """
    Excelファイルを読み込んで検索条件dictのリストを返す。

    Args:
        input_path (str): _description_

    Returns:
        list[dict]: _description_
    """
    df_dict = pd.read_excel(input_path, sheet_name=None, header=1)
    for i_df in df_dict.values():
        maker = i_df['メーカー'].iloc[0]
        i_df['メーカー'].fillna(maker, inplace=True)
        i_df['keyword'] = i_df['メーカー'] + '+' + i_df['製品型番']
    tmp_df = pd.concat(df_dict.values())

    # tmp_df = tmp_df.drop(['メーカー', '製品型番'], axis=1)
    result = tmp_df.to_dict('records')
    print('Finished to read input excel file.')
    return result


def get_list_page(session: Session, criteria: dict, item_num_in_page: int=ITEM_NUM_IN_PAGE) -> Response:
    """
    一覧ページの1ページ目を取得する。

    Args:
        session (Session): _description_
        criteria (dict): _description_

    Returns:
        str: _description_
    """
    base_url = BASE_URL
    option_dict = {
        'keyword': f'_nkw={criteria["keyword"]}',
        'lowest_price': f'_udlo={criteria["最低価格"]}',
        'item_num_in_page': f'_ipg={item_num_in_page}',
        'display_format': f'_dmd=1',
    }
    page_option = '&'.join(option_dict.values())
    url = base_url + '?' + page_option
    return get_response(session, url,
                        success_message=f'First list page "{url}" access succeeded.')


def fetch_detail_urls(session: Session, response: Response, item_num_in_page: int=ITEM_NUM_IN_PAGE) -> list:
    first_url = response.url
    soup = BeautifulSoup(response.text, HTML_PARSER)
    heading = soup.select_one('#mainContent .srp-controls__count').text
    total_item_num = int(re.search(r'(\d+)件', heading)[1])
    total_page_num = math.ceil(total_item_num / item_num_in_page)
    print('Number of detail pages: ' + str(total_item_num))

    urls = []
    undisplayed_item_num = total_item_num
    for page_num in range(1, total_page_num+1):
        # 表示件数が少ないとき、「一部の語句に一致する検索結果」が表示されてしまうので、
        # 「一部の語句に一致する検索結果」のaタグをa_elementsに含めないようにする対策。
        if undisplayed_item_num > item_num_in_page:
            item_num_in_current_page = item_num_in_page
            undisplayed_item_num -= item_num_in_page
        else:
            item_num_in_current_page = undisplayed_item_num
            undisplayed_item_num = 0

        try:
            srp_list_element = soup.select_one('.srp-results.srp-list')
            item_elements = srp_list_element.select('li.s-item.s-item__pl-on-bottom')
            a_elements = [i.select_one('.s-item__info a.s-item__link') for i in item_elements]
            a_elements = a_elements[:item_num_in_current_page] # 「一部の語句に一致する検索結果」を除外
            urls.extend([i.get('href') for i in a_elements])
        except Exception as e:
            handle_scraping_error(e, response)

        if page_num < total_page_num:
            response = get_next_page(session, first_url, page_num+1)
            if response:
                soup = BeautifulSoup(response.text, HTML_PARSER)
            else:
                continue

    return urls


def get_next_page(session: Session, first_url: str, page_num: int) -> Response:
    param_key = '_pgn'
    next_url = first_url + f'&{param_key}={page_num}'
    return get_response(session, next_url,
                        success_message=f'Next list page "{next_url}" access succeeded.')


def fetch_item_infos(session: Session, detail_urls: list) -> DataFrame:
    len_detail_urls = len(detail_urls)

    item_infos = pd.DataFrame(columns=OUTPUT_COLUMNS)
    for i, i_url in enumerate(detail_urls, start=1):
        if not (response := get_response(session, i_url)):
            continue # continueだけでいいのか？ログ出力とか

        soup = BeautifulSoup(response.text, HTML_PARSER)

        try:
            info = scrape(soup)

        except Exception as e:
            handle_scraping_error(e, response)

        info['url'] = response.url.split('?')[0]

        info_df = pd.DataFrame(info, index=[0])
        item_infos = pd.concat([item_infos, info_df], ignore_index=True)
        print(f'Item {i}/{len_detail_urls}:', info)

    return item_infos


def scrape(soup: BeautifulSoup) -> dict:
    info = {}
    info['title'] = unicodedata.normalize('NFKD', soup.select_one('h1.x-item-title__mainTitle').text.strip())
    info['condition'] = soup.select_one('.x-item-condition-value .clipped').text
    price_string = soup.select_one('.x-buybox__price-section .x-price-approx').text
    info['price'] = int(re.sub(r'\D', '', price_string))
    shipping_element = soup.select_one('.vim.d-shipping-minview')
    row_elements = shipping_element.select('.ux-layout-section__row')
    for row_element in row_elements:
        row_text = row_element.text
        charge = get_charge(row_text)
        if charge:
            if '送料' in row_text:
                info['postage'] = charge
            elif '輸入手数料' in row_text:
                info['import fees'] = charge
            elif '関税' in row_text:
                info['duty'] = charge

    return info


def get_charge(string: str) -> int:
    if '無料' in string:
        return 0
    else:
        match = re.search(r'JPY\s*([\d,]+)', string)
        if match:
            num = int(match[1].replace(',', ''))
            return num
        else:
            return None


def modify_item_infos(item_infos: DataFrame, search_criteria: dict) -> list[dict]:
    item_infos['maker'] = search_criteria['メーカー']
    item_infos['model number'] = search_criteria['製品型番']

    return item_infos


def overwrite_jl(jl_path: str, item_infos: DataFrame):
    if item_infos.empty:
        return

    with open(jl_path, 'a') as f:
        f.write(item_infos.to_json(orient='records', force_ascii=False, lines=True))


def write_excel(excel_path: str, jl_path: str):
    if not os.path.exists(jl_path):
        print(f'File {jl_path} does not exist.')
        return

    item_infos = pd.read_json(jl_path, orient='records', lines=True)
    makers = item_infos['maker'].unique()
    with pd.ExcelWriter(excel_path) as writer:
        for i_maker in makers:
            item_infos[item_infos['maker'] == i_maker].to_excel(writer, sheet_name=i_maker)
    print('Finished to write output excel file.')


def handle_scraping_error(e: Exception, response: Response):
    with open('scraping_error.html', 'w') as f:
        f.write(response.text)
    traceback.print_exc()
    print(f'scraping error in URL "{response.url}"')
    sys.exit(1)


if __name__ == '__main__':
    main()
