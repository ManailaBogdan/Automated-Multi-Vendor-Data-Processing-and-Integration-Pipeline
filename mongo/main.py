import pymongo
import json

conn = pymongo.MongoClient('mongodb://172.17.0.4:27017/')
db = conn['vendors']
collection = db['test_collection']
d = [
        {
                "name": "Alin",
                "age": 30
        },
        {
                "name": "Bogdan",
                "age": 29
        },
        {
                "name": "Alex",
                "age": 23
        }
]

try:
       collection.insert_many(d, ordered=False)
except Exception as e:
        print(e)


