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

# Compound indexes — support latest-by-event-time dashboard reads while the TTL
# index continues to retain/expire history by insert time.
db.speed_layer.create_index(
    [("event_timestamp", DESCENDING), ("inserted_at", DESCENDING)],
)
db.speed_layer.create_index(
    [("station_complex_id", ASCENDING), ("event_timestamp", DESCENDING), ("inserted_at", DESCENDING)],
)

print(db.speed_layer.index_information())
