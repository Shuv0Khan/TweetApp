import pymongo


class MongoDBProcessor:
    def __init__(self, db_url="mongodb://localhost:27017/", db_name="TwitterData"):
        self.db_url = db_url
        self.db_name = db_name

        self.my_client = pymongo.MongoClient(self.db_url)
        self.my_db = self.my_client[self.db_name]

    def save(self, collection, json_data):
        self.my_db[collection].insert_one(json_data)
