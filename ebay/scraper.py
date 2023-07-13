import time
import requests
from urllib.robotparser import RobotFileParser
import urllib.parse
from typing import Union, Callable, Tuple

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


    def get(self, url: str, success_message: str='') -> bool:
        user_agent = self.session.headers['User-Agent']
        if self.rp.can_fetch(useragent=user_agent, url=url):
            try:
                response = self.session.get(url)
                response.raise_for_status()
                if success_message:
                    print(success_message)
                time.sleep(self.download_delay)
                self.soup = BeautifulSoup(response.text, self.html_parser)
                self.url = response.url
                self.text = response.text
                return True
            except requests.exceptions.RequestException as e:
                print(f'Error: {e} (URL "{url}")')
                return False
        else:
            print(f'ERROR: URL "{url}" is prohibitied by robots.txt.')
            return False


    def select_one(self, selector):
        return self.soup.select_one(selector)


    def select(self, selector):
        return self.soup.select(selector)


    def find_all(self, name=None, attrs={}, recursive=True, text=None, limit=None, **kwargs):
        return self.soup.find_all(name=name, attrs=attrs, recursive=recursive, text=text, limit=limit, **kwargs)


    def find(self, name=None, attrs={}, recursive=True, text=None, **kwargs):
        return self.soup.find(name=name, attrs=attrs, recursive=recursive, text=text, **kwargs)
