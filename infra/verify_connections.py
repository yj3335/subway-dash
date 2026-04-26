"""
Verify MongoDB and Cassandra are reachable.
  python infra/verify_connections.py
"""
from pymongo import MongoClient
from cassandra.cluster import Cluster

client = MongoClient("mongodb://localhost:27017")
print("MongoDB:", client.server_info()["version"])
client.close()

cluster = Cluster(["localhost"])
session = cluster.connect()
row = session.execute("SELECT release_version FROM system.local").one()
print("Cassandra:", row.release_version)
cluster.shutdown()
