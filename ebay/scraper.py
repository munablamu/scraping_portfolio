import time
import requests
from urllib.robotparser import RobotFileParser
import urllib.parse
from typing import Union, Callable, Tuple

import pandas as pd
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self, base_url: str, html_parser: str='lxml', download_delay: Union[float, int]=2):
        self.session = requests.Session()
        self.soup = None
        self.url = None
        self.text = None
        self.base_url = base_url
        self.html_parser = html_parser
        self.download_delay = download_delay
        self.read_robots_txt()


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()


    def read_robots_txt(self):
        self.rp = RobotFileParser()
        robots_url= urllib.parse.urljoin(self.base_url, '/robots.txt')
        self.rp.set_url(robots_url)
        self.rp.read()
        print(f'Read robots.txt: "{robots_url}"')


    def get(self, url: str, success_message: str=''):
        self.url = url
        user_agent = self.session.headers['User-Agent']
        if not self.rp.can_fetch(useragent=user_agent, url=self.url):
            raise Exception(f'Error: Access to URL "{self.url}" is prohibited by robots.txt.')

        response = self.session.get(self.url)
        self.url = response.url # リダイレクトに対応
        time.sleep(self.download_delay)
        if response.status_code != 200:
            raise Exception(f'Error: Failed to get URL "{self.url}" (status code: {response.status_code})')

        if success_message:
            print(success_message)
        self.soup = BeautifulSoup(response.text, self.html_parser)
        self.text = response.text


    def select_one(self, selector):
        return self.soup.select_one(selector)


    def select(self, selector):
        return self.soup.select(selector)


    def find_all(self, name=None, attrs={}, recursive=True, text=None, limit=None, **kwargs):
        return self.soup.find_all(name=name, attrs=attrs, recursive=recursive, text=text, limit=limit, **kwargs)


    def find(self, name=None, attrs={}, recursive=True, text=None, **kwargs):
        return self.soup.find(name=name, attrs=attrs, recursive=recursive, text=text, **kwargs)


class Item:
    def __init__(self, columns):
        self.df = pd.DataFrame(columns=columns)
        self.columns = columns


    def __setitem__(self, key, value):
        self.df[key] = value


    @property
    def empty(self):
        return self.df.empty


    def add_row(self, data):
        if not set(data.keys()).issubset(set(self.columns)):
            raise ValueError('Data contains columns not in Item.')
        new_df = pd.DataFrame(data, index=[0])
        self.df = pd.concat([self.df, new_df], ignore_index=True)


    def to_json(self, path_or_buf=None, orient='columns', **kwargs):
        return self.df.to_json(path_or_buf=path_or_buf, orient=orient, **kwargs)
