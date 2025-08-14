from utills.db import create_pg_cursor
from prefect import task, flow
from prefect.assets import materialize
from dotenv import load_dotenv


load_dotenv()

@task
def create_connection(host, user, password, dbname, port):
    return create_pg_cursor(host, user, password, dbname, port)


@task
def load_configuration():
    pass