import os
import sys
import re
import math
import unicodedata
import traceback
from typing import Callable, Tuple, Any
import json
import argparse

import pandas as pd

from scraper import Scraper, Item

DOWNLOAD_DELAY = 2
BASE_URL = 'https://www.ebay.com/sch/i.html'
INPUT_PATH = 'inputs/keyboard_list.xlsx'
OUTPUT_JL_PATH = 'outputs/results.jl'
OUTPUT_PATH = 'outputs/results.xlsx'
OUTPUT_COLUMNS = ['maker', 'model number', 'keyword', 'title', 'condition', 'price', 'postage',
                  'import fees', 'duty', 'url']
HTML_PARSER = 'lxml'
ITEM_NUM_IN_PAGE = 60


def main():
    args = get_args()

    search_criteria_list = read_excel(INPUT_PATH)

    scraped_keywords = set()
    if args.restart:
        scraped_keywords = read_jl(OUTPUT_JL_PATH)

    with Scraper(BASE_URL, HTML_PARSER, DOWNLOAD_DELAY) as scraper:
        for search_criteria in search_criteria_list:
            if search_criteria['keyword'] in scraped_keywords:
                print(f'INFO: Skip scraped keyword "{search_criteria["keyword"]}"')
                continue

            first_list_page_url = get_first_list_page_url(scraper, search_criteria)
            detail_urls = fetch_detail_urls(scraper, first_list_page_url)
            item_infos = fetch_item_infos(scraper, detail_urls)
            modify_item_infos(item_infos, search_criteria)
            overwrite_jl(OUTPUT_JL_PATH, item_infos)

    write_excel(OUTPUT_PATH, OUTPUT_JL_PATH)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--restart', action='store_true')
    return parser.parse_args()


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
        i_df['keyword'] = (i_df['メーカー'] + '+' + i_df['製品型番']).replace(' ', '+')
    tmp_df = pd.concat(df_dict.values())

    # tmp_df = tmp_df.drop(['メーカー', '製品型番'], axis=1)
    result = tmp_df.to_dict('records')
    print('Finished to read input excel file.')
    return result


def read_jl(jl_path):
    existed_keywords = set()
    with open(jl_path, 'r') as file:
        for line in file:
            obj = json.loads(line)
            existed_keywords.add(obj['keyword'])
    return existed_keywords


def get_first_list_page_url(scraper: Scraper, criteria: dict,
                            item_num_in_page: int=ITEM_NUM_IN_PAGE) -> bool:
    """
    一覧ページの1ページ目を取得する。

    Args:
        session (Session): _description_
        criteria (dict): _description_

    Returns:
        str: _description_
    """
    base_url = scraper.base_url
    option_dict = {
        'keyword': f'_nkw={criteria["keyword"]}',
        'lowest_price': f'_udlo={criteria["最低価格"]}',
        'item_num_in_page': f'_ipg={item_num_in_page}',
        'display_format': f'_dmd=1',
    }
    page_option = '&'.join(option_dict.values())
    return base_url + '?' + page_option


def fetch_detail_urls(scraper: Scraper, first_list_page_url: str,
                      item_num_in_page: int=ITEM_NUM_IN_PAGE) -> list:
    try:
        urls = try_func(scrape_detail_urls, kwargs={'scraper': scraper,
                                                    'first_list_page_url': first_list_page_url,
                                                    'item_num_in_page': item_num_in_page})
    except Exception as e:
        handle_scraping_error(e, scraper)

    return urls


def scrape_detail_urls(scraper: Scraper, first_list_page_url: str,
                      item_num_in_page: int=ITEM_NUM_IN_PAGE) -> list[str]:
    scraper.get(first_list_page_url,
                success_message=f'First list page "{first_list_page_url}" access succeeded.')
    heading = scraper.select_one('#mainContent .srp-controls__count').text
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

        srp_list_element = scraper.select_one('.srp-results.srp-list')
        item_elements = srp_list_element.select('li.s-item.s-item__pl-on-bottom')
        a_elements = [i.select_one('.s-item__info a.s-item__link') for i in item_elements]
        a_elements = a_elements[:item_num_in_current_page] # 「一部の語句に一致する検索結果」を除外
        urls.extend([i.get('href').split('?')[0] for i in a_elements])

        if page_num < total_page_num:
            get_next_page(scraper, first_list_page_url, page_num+1)

    return urls


def get_next_page(scraper: Scraper, first_url: str, page_num: int) -> bool:
    param_key = '_pgn'
    next_url = first_url + f'&{param_key}={page_num}'
    return scraper.get(next_url, success_message=f'Next list page "{next_url}" access succeeded.')


def fetch_item_infos(scraper: Scraper, detail_urls: list) -> Item:
    len_detail_urls = len(detail_urls)

    item_infos = Item(OUTPUT_COLUMNS)
    for i, i_url in enumerate(detail_urls, start=1):
        try:
            item_info = try_func(scrape_item_info, kwargs={'scraper': scraper, 'url': i_url})
        except Exception as e:
            handle_scraping_error(e, scraper)

        item_info['url'] = scraper.url # リダイレクトされている場合もあるのでi_urlではなく、scraper.url

        item_infos.add_row(item_info)
        print(f'Item {i}/{len_detail_urls}:', item_info)

    return item_infos


def scrape_item_info(scraper: Scraper, url: str) -> dict:
    scraper.get(url)

    info = {}
    info['title'] = unicodedata.normalize('NFKD', scraper.select_one('h1.x-item-title__mainTitle').text.strip())
    info['condition'] = scraper.select_one('.x-item-condition-value .clipped').text
    price_string = scraper.select_one('.x-buybox__price-section .x-price-approx').text
    info['price'] = int(re.sub(r'\D', '', price_string))
    shipping_element = scraper.select_one('.vim.d-shipping-minview')
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


def modify_item_infos(item_infos: Item, search_criteria: dict):
    item_infos['maker'] = search_criteria['メーカー']
    item_infos['model number'] = search_criteria['製品型番']
    item_infos['keyword'] = search_criteria['keyword']


def overwrite_jl(jl_path: str, item_infos: Item):
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


def try_func(func: Callable, args: Tuple=(), kwargs: dict={}, max_retry: int=3) -> Any:
    for i in range(max_retry):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == max_retry - 1:
                raise e
            print(f'INFO: {func.__name__} failed. Try again.')


def handle_scraping_error(e: Exception, scraper: Scraper):
    with open('scraping_error.html', 'w') as f:
        f.write(scraper.text)
    traceback.print_exc()
    print(f'scraping error in URL "{scraper.url}"')
    sys.exit(1)


if __name__ == '__main__':
    main()
