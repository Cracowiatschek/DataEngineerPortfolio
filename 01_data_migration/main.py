from utills.db import create_pg_cursor, TableMetadataMismatchError
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from dotenv import load_dotenv
from collections import deque, namedtuple
import json
import os


load_dotenv()
Object=namedtuple("Object", ["name", "schema_in", "schema_out"])

@task
def load_configuration(file_path:str) -> list:
    config = json.load(open(file_path, 'r'))
    return config


@task
def make_queue(config:list[dict]) -> deque:
    queue=deque()

    while len(config) > 0:
        for item in config:
            depends_on = all([depend_table in [i.name for i in queue] for depend_table in item["depends_on"]])
            if depends_on:
                queue.append(Object(item["name"], item["schema_in"], item["schema_out"]))
                config.remove(item)
    return queue



@materialize
def migrate(item:Object):
    logger = get_run_logger()
    try:
        with create_pg_cursor(host=os.getenv("DB_LOCAL_HOST"),  user=os.getenv("DB_LOCAL_USER"),
                              password=os.getenv("DB_LOCAL_PASSWORD"), dbname=os.getenv("DB_LOCAL_NAME"),
                              port=int(os.getenv("DB_LOCAL_PORT"))) as connect:
            local=connect.cursor()
            local.execute(f"select * from {item.schema_in}.{item.name}")
            data=local.fetchall()
            colnames=[desc[0] for desc in local.description]

            with create_pg_cursor(host=os.getenv("DB_VPS_HOST"), user=os.getenv("DB_VPS_USER"),
                                  password=os.getenv("DB_VPS_PASSWORD"), dbname=os.getenv("DB_VPS_NAME"),
                                  port=int(os.getenv("DB_VPS_PORT"))) as v_connect:
                placeholders = ", ".join(["%s"] * len(colnames))
                col_str = ", ".join(colnames)
                query = f"insert into {item.schema_out}.{item.name} ({col_str}) values ({placeholders})"

                vps=v_connect.cursor()
                vps.executemany(query, data)
                logger.info(f"Inserted {len(data)} rows into {item.schema_out}.{item.name} from {item.schema_in}.{item.name}.")
    except Exception as e:
        logger.error(e)
        v_connect.rollback()
        raise



@flow
def make_migration():
    base_migration = migrate

    config = load_configuration(os.getenv("CONFIG_PATH"))
    queue = make_queue(config)
    while len(queue) > 0:
        item = queue.popleft()
        base_migration.with_options(
            assets=[f"postgres://{os.getenv("DB_VPS_HOST")}/{os.getenv("DB_VPS_NAME")}/{item.schema_out}/{item.name}"],
            asset_deps=[f"postgres://{os.getenv("DB_LOCAL_HOST")}/{os.getenv("DB_LOCAL_NAME")}/{item.schema_in}/{item.name}"]
        )
        base_migration(item)

if __name__ == '__main__':
    make_migration()