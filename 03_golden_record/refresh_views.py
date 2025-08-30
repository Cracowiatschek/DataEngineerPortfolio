import os
from datetime import datetime
from typing import Any
from config.JSONConfig import JSONConfig
from utills.db import create_pg_conn, NotAllViewsRefreshed
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from prefect.blocks.system import Secret
from collections import deque, namedtuple


PG_HOST:str = Secret.load("pg-host").get()
PG_USER:str = Secret.load("pg-user").get()
PG_PASSWORD:str = Secret.load("pg-password").get()
PG_PORT:int = Secret.load("pg-port").get()
PG_DBNAME:str = Secret.load("pg-dbname").get()


MaterializedView = namedtuple("MaterializedView", ["schema", "view_name", "sources"])
Source = namedtuple("Source", ["schema", "table_name"])

@task
def make_queue(config: list[dict[str, Any]]) -> deque:
    queue = deque()
    for cfg in config:
        queue.append(MaterializedView(schema=cfg["schema"], view_name=cfg["materialized_view"],sources=[Source(schema=c["schema"], table_name=c["table"]) for c in cfg["sources"]]))
    return queue


@task
def get_refresh_datetime(view:MaterializedView) -> datetime:
    try:
        with create_pg_conn(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, port=PG_PORT) as connect:
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
    logger = get_run_logger()
    try:
        with create_pg_conn(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, port=PG_PORT) as connect:
            cursor = connect.cursor()
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view.schema}.{view.view_name}")
            logger.info(f"Refreshed view {view.schema}.{view.view_name}")
    except Exception as e:
        print(e)
        logger.error(f"View {view.schema}.{view.view_name} not refreshed")
        raise


@task
def check_refresh_datetime(datetime_to_check:dict) -> None:
    logger = get_run_logger()
    overlap = [i for i in datetime_to_check["before"] if i in datetime_to_check["after"]]
    if len(overlap) > 0:
        logger.error(f"{len(overlap)}/{len(datetime_to_check['before'])} are not refreshed")
        raise NotAllViewsRefreshed
    pass


@flow(retries=3, retry_delay_seconds=120)
def refresh_views():
    base_refresh = refresh_view

    jsonconfig_block = JSONConfig.load("views-config").data
    queue = make_queue(jsonconfig_block)

    datetime_to_check = dict(before=[], after=[])

    while len(queue) > 0:
        item = queue.popleft()
        datetime_to_check["before"].append(get_refresh_datetime(item))
        refresh_task = base_refresh.with_options(
            assets=[f"postgres://{os.getenv("DB_PG_HOST")}/{os.getenv("DB_PG_NAME")}/{item.schema}/{item.view_name}"],
            asset_deps=[
                f"postgres://{os.getenv("DB_PG_HOST")}/{os.getenv("DB_PG_NAME")}/{i.schema}/{i.table_name}" for i in item.sources
            ]
        )
        refresh_task(item)
        datetime_to_check["after"].append(get_refresh_datetime(item))

    check_refresh_datetime(datetime_to_check)


if __name__ == '__main__':
    refresh_views()