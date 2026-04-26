"""
Run once after `docker-compose up` to create the TTL index on speed_layer.
  python infra/init_mongo.py
"""
from pymongo import MongoClient, ASCENDING

client = MongoClient("mongodb://localhost:27017")
db = client["subway_dash"]

db.speed_layer.create_index(
    [("inserted_at", ASCENDING)],
    expireAfterSeconds=86400,  # 24 hours
)

print(db.speed_layer.index_information())
