import re
from datetime import datetime

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from yahoo_news.items import NewsTopicsItem
from yahoo_news.utils import MongoMixin


class NewsTopicsSpider(CrawlSpider, MongoMixin):
    name = "news_topics"
    allowed_domains = ["news.yahoo.co.jp"]
    start_urls = ["https://news.yahoo.co.jp/topics"]

    rules = (
        Rule(LinkExtractor(restrict_css='#contentsWrap > div:nth-of-type(1) li a'),
                           callback='parse_pickup_article',
                           process_request='process_request_before_parse_pickup_article'),
    )


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.setup_mongo(
            mongodb_uri=crawler.settings.get('MONGODB_URI'),
            mongodb_database=crawler.settings.get('MONGODB_DATABASE'),
            mongodb_collection=cls.name
        )
        return spider


    def close_mongo(self, spider):
        self.close_mongo()


    def process_request_before_parse_pickup_article(self, request, response):
        key = self.extract_key(request.url)
        if not self.collection.find_one({'key': key}):
            return request
        else:
            self.logger.info(f'URL {request.url} already processed, skipping')


    def parse_pickup_article(self, response):
        item = NewsTopicsItem()
        item['key'] = self.extract_key(response.url)
        item['title'] = response.css('head title::text').get().replace(' - Yahoo!ニュース', '')
        pubdate = response.css('head meta[name="pubdate"]::attr("content")').get()
        item['post_time'] = self.pubdate2datetime(pubdate)
        vender = response.css('article > div > span > a > span > span::text').get()
        if vender is None:
            vender = response.css('article > div:first-of-type > div > p > a::text').get()
        item['vender'] = vender
        item['description'] = self.normalize_spaces(response.css('.highLightSearchTarget::text').get())
        item['article_url'] = response.css('article > div > span > a::attr("href")').get()
        yield item


    def pubdate2datetime(self, pubdate: str) -> str:
        dt = datetime.strptime(pubdate, "%Y-%m-%dT%H:%M:%S%z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")


    def extract_key(self, url: str) -> str:
        return url.split('/')[-1]


    def normalize_spaces(self, s: str) -> str:
        return re.sub(r'\s+', ' ', s).strip()
