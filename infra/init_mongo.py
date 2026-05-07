"""
Run once after `docker-compose up` to create the TTL index on speed_layer.
  python infra/init_mongo.py
"""
import os
from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
db = client["subway_dash"]

# TTL index — auto-expire docs after 24 hours
db.speed_layer.create_index(
    [("inserted_at", ASCENDING)],
    expireAfterSeconds=86400,
)

# Compound index — supports the $sort stage in _LATEST_PER_STATION aggregation
db.speed_layer.create_index(
    [("station_complex_id", ASCENDING), ("inserted_at", DESCENDING)],
)

print(db.speed_layer.index_information())
