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
def migrate(queue:deque, cursor_in, cursor_out):
    pass