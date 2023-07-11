# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from yahoo_news.utils import MongoMixin


class MongoPipeline(MongoMixin):
    """
    ItemをMongoDBに保存するPipeline。
    """
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongodb_uri=crawler.settings.get('MONGODB_URI'),
            mongodb_database=crawler.settings.get('MONGODB_DATABASE'),
        )


    def __init__(self, mongodb_uri, mongodb_database):
        self.mongo_uri = mongodb_uri
        self.mongo_db = mongodb_database


    def open_spider(self, spider):
        """
        Spiderの開始時にMongoDBに接続する。

        Args:
            spider (_type_): _description_
        """
        mongodb_collection = spider.name
        self.setup_mongo(self.mongo_uri, self.mongo_db, mongodb_collection)


    def close_spider(self, spider):
        """
        Spiderの終了時にMongoDBへの接続を切断する。

        Args:
            spider (_type_): _description_
        """
        self.close_mongo()


    def process_item(self, item, spider):
        """
        Itemをコレクションに追加する。

        Args:
            item (_type_): _description_
            spider (_type_): _description_
        """
        self.collection.insert_one(dict(item))
        return item


class YahooNewsPipeline:
    def process_item(self, item, spider):
        return item
