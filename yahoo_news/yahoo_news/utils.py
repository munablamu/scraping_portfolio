import sys
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MongoMixin:
    def setup_mongo(self, mongodb_uri, mongodb_database, mongodb_collection):
        self.mongo_logger = logging.getLogger(__name__)
        self.client = MongoClient(mongodb_uri)
        try:
            self.client.admin.command('ismaster')
        except ConnectionFailure:
            self.mongo_logger.error('Could not connect to MongoDB server. Please make sure mongod process is running.')
            sys.exit(1)

        self.db = self.client[mongodb_database]
        self.collection = self.db[mongodb_collection]
        self.collection.create_index('key', unique=True)


    def close_mongo(self):
        self.client.close()
