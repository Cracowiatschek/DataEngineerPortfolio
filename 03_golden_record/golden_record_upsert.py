import os
from datetime import datetime
from typing import Any
from data_model import GoldenCustomer
from utills.db import create_mongo_conn, create_pg_conn, NotAllViewsRefreshed
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from dotenv import load_dotenv
from collections import deque, namedtuple
from prefect.blocks.system import String




load_dotenv()


client = create_mongo_conn(host=os.getenv("DB_MONGO_HOST"), user=os.getenv("DB_MONGO_USER"),
                           password=os.getenv("DB_MONGO_PASSWORD"), dbname=os.getenv("DB_MONGO_NAME"),
                           port=int(os.getenv("DB_MONGO_PORT")))


@task
def get_data():

    query = String.load("daily-report-query").value

    with create_pg_conn(host=os.getenv("DB_PG_HOST"), user=os.getenv("DB_PG_USER"),
                        password=os.getenv("DB_PG_PASSWORD"), dbname=os.getenv("DB_PG_NAME"),
                        port=int(os.getenv("db_pg_port"))) as conn:
        cursor = conn.cursor()
        cursor.execute(query)