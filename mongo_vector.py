import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')

db = client.log

collection = db.traffic_windowed_appearance

print(collection.estimated_document_count())

