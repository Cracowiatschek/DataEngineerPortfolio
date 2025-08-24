import os

from utills.db import create_mongo_conn, TableMetadataMismatchError
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from dotenv import load_dotenv
from collections import deque, namedtuple


load_dotenv()


client = create_mongo_conn(host=os.getenv("DB_MONGO_HOST"), user=os.getenv("DB_MONGO_USER"),
                           password=os.getenv("DB_MONGO_PASSWORD"), dbname=os.getenv("DB_MONGO_NAME"),
                           port=int(os.getenv("DB_MONGO_PORT")))

