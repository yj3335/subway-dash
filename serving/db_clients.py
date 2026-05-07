import os
from typing import Optional
from pymongo import MongoClient
from cassandra.cluster import Cluster

_mongo_client: Optional[MongoClient] = None
_cassandra_cluster: Optional[Cluster] = None
_cassandra_session = None


def get_mongo_collection(name: str):
    global _mongo_client
    if _mongo_client is None:
        uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
        _mongo_client = MongoClient(uri)
    return _mongo_client["subway_dash"][name]


def get_cassandra_session():
    global _cassandra_cluster, _cassandra_session
    if _cassandra_session is None:
        hosts = os.environ.get("CASSANDRA_HOSTS", "localhost").split(",")
        _cassandra_cluster = Cluster(hosts)
        _cassandra_session = _cassandra_cluster.connect()
    return _cassandra_session


def reset_cassandra_session():
    """Force reconnect on the next get_cassandra_session() call."""
    global _cassandra_cluster, _cassandra_session
    _cassandra_session = None
    if _cassandra_cluster:
        try:
            _cassandra_cluster.shutdown()
        except Exception:
            pass
        _cassandra_cluster = None


def close_connections():
    global _mongo_client, _cassandra_cluster, _cassandra_session
    if _mongo_client:
        _mongo_client.close()
        _mongo_client = None
    if _cassandra_cluster:
        _cassandra_cluster.shutdown()
        _cassandra_cluster = None
        _cassandra_session = None
