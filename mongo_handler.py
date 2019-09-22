from pymongo import MongoClient


class MongoHandler():
    def __init__(self, host, port, db):
        self.client = MongoClient(host, port)
        self.database = self.client[db]

    def insert_many(self, collection, data: []):
        self.database.get_collection(collection).insert_many(data)

    def insert(self, collection, data: {}):
        self.database.get_collection(collection).insert_one(data)
    
    def find_all(self, collection):
        return self.database.get_collection(collection).find()
