from utills.db import create_pg_cursor
from prefect import task, flow
from prefect.assets import materialize
from dotenv import load_dotenv
from collections import deque, namedtuple
import json
import os


load_dotenv()

@task
def create_connection(host:str, user:str, password:str, dbname:str, port:int):
    return create_pg_cursor(host, user, password, dbname, port)


@task
def load_configuration(file_path:str) -> list:
    config = json.load(open(file_path, 'r'))
    return config


@task
def make_queue(config:list[dict]) -> deque:
    queue=deque()
    Object=namedtuple("Object", ["name", "schema_in", "schema_out"])

    while len(config) > 0:
        for item in config:
            depends_on = all([depend_table in [i.name for i in queue] for depend_table in item["depends_on"]])
            if depends_on:
                queue.append(Object(item["name"], item["schema_in"], item["schema_out"]))
                config.remove(item)
    return queue


@task
def check_metadata(cursor, schema:str, table_name:str) -> dict:
    cursor.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    meta = {}
    for row in cursor.fetchall():
        meta[row[0]] = row[1]
    return meta


@task
def get_data(cursor, schema:str, table_name:str):
    cursor.execute(f"SELECT * FROM {schema}.{table_name}")
    return cursor.fetchall()


# @task
# def load_data(cursor, schema:str, table_name:str):
#     cursor.execute_many(f"INSERT INTO {schema}.{table_name}")


@task
def migrate(queue:deque, cursor_in, cursor_out):
    try:
        while len(queue) > 0:
            item = queue.popleft()
            meta = check_metadata(cursor=cursor_in, schema=item.schema_in, table_name=item.name)
            for i in meta:
                print(f"{i}: {meta[i]}")
            # data = get_data(cursor=cursor_in, schema=item.schema_in, table_name=item.name)
            # print(data)
    except Exception as e:
        print(e)
    finally:
        cursor_in.close()
        cursor_out.close()


cfg = load_configuration(file_path=os.getenv("CONFIG_PATH"))
q = make_queue(config=cfg)
migrate(queue=q,
        cursor_in=create_pg_cursor(
            host=os.getenv("DB_LOCAL_HOST"),
            user=os.getenv("DB_LOCAL_USER"),
            password=os.getenv("DB_LOCAL_PASSWORD"),
            port=int(os.getenv("DB_LOCAL_PORT")),
            dbname=os.getenv("DB_LOCAL_DBNAME")
        ), cursor_out=create_pg_cursor(
            host=os.getenv("DB_LOCAL_HOST"),
            user=os.getenv("DB_LOCAL_USER"),
            password=os.getenv("DB_LOCAL_PASSWORD"),
            port=int(os.getenv("DB_LOCAL_PORT")),
            dbname=os.getenv("DB_LOCAL_DBNAME")
        ))