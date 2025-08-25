import os
from datetime import datetime
from typing import Any
from config.JSONConfig import JSONConfig
from utills.db import create_mongo_conn, create_pg_conn, TableMetadataMismatchError
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from dotenv import load_dotenv
from collections import deque, namedtuple
import copy


load_dotenv()


client = create_mongo_conn(host=os.getenv("DB_MONGO_HOST"), user=os.getenv("DB_MONGO_USER"),
                           password=os.getenv("DB_MONGO_PASSWORD"), dbname=os.getenv("DB_MONGO_NAME"),
                           port=int(os.getenv("DB_MONGO_PORT")))

MaterializedView = namedtuple("MaterializedView", ["schema", "view_name", "sources"])
Source = namedtuple("Source", ["schema", "table_name"])

@task
def make_queue(config: list[dict[str, Any]]) -> deque:
    queue = deque()
    for cfg in config:
        print(cfg)
        queue.append(MaterializedView(schema=cfg["schema"], view_name=cfg["materialized_view"],sources=[Source(schema=c["schema"], table_name=c["table"]) for c in cfg["sources"]]))
    return queue


@task
def get_refresh_datetime(view:MaterializedView) -> datetime:
    try:
        with create_pg_conn(host=os.getenv("DB_PG_HOST"), user=os.getenv("DB_PG_USER"),
                            password=os.getenv("DB_PG_PASSWORD"), dbname=os.getenv("DB_PG_NAME"),
                            port=int(os.getenv("DB_PG_PORT"))) as connect:
            cursor = connect.cursor()
            cursor.execute(f"SELECT MAX(last_refresh_date) FROM {view.schema}.{view.view_name}")
            result = cursor.fetchone()
    except Exception as e:
        print(e)
        raise
    return result[0]


@materialize(
    f"postgres://{os.getenv("DB_PG_HOST")}/{os.getenv("DB_PG_NAME")}/output_schema/table",
    asset_deps=[f"postgres://{os.getenv("DB_PG_HOST")}/{os.getenv("DB_PG_NAME")}/input_schema/table"]
)
def refresh_view(view:MaterializedView):
    try:
        with create_pg_conn(host=os.getenv("DB_PG_HOST"), user=os.getenv("DB_PG_USER"),
                            password=os.getenv("DB_PG_PASSWORD"), dbname=os.getenv("DB_PG_NAME"),
                            port=int(os.getenv("DB_PG_PORT"))) as connect:
            cursor = connect.cursor()
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view.schema}.{view.view_name}")
    except Exception as e:
        print(e)
        raise

@flow
def refresh_views():
    jsonconfig_block = JSONConfig.load("views-config").data
    queue = make_queue(jsonconfig_block)



if __name__ == '__main__':
    refresh_views()