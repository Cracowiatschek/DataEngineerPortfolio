import collections
import collections.abc

collections.Mapping = collections.abc.Mapping
collections.MutableMapping = collections.abc.MutableMapping
collections.Sequence = collections.abc.Sequence


import psycopg
from pymongo import MongoClient


def create_pg_conn(host: str, user: str, password: str, dbname: str, port: int=5432):
    connect=psycopg.connect(host=host, user=user, password=password, dbname=dbname, port=port)
    return connect


class TableMetadataMismatchError(Exception):
    """Raised when source and target table metadata do not match."""
    pass


class NotAllViewsRefreshed(Exception):
    """Raised when not all views are refreshed."""
    pass


def create_mongo_conn(host: str, user: str, password: str, dbname: str, port: int=27017):
    uri = f"mongodb://{user}:{password}@{host}:{port}/{dbname}"
    connect = MongoClient(uri)
    return connect
