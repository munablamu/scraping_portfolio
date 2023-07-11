# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsTopicsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    key = scrapy.Field()
    title = scrapy.Field()
    post_time = scrapy.Field()
    vender = scrapy.Field()
    description = scrapy.Field()
    article_url = scrapy.Field()
