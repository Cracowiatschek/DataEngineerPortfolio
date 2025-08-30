
from typing import Generator, List
from data_model import GoldenCustomer
from utills.db import create_mongo_conn, create_pg_conn
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from prefect.blocks.system import Secret
from collections import deque, namedtuple
from prefect.blocks.system import String


PG_HOST:str = Secret.load("pg-host").get()
PG_USER:str = Secret.load("pg-user").get()
PG_PASSWORD:str = Secret.load("pg-password").get()
PG_PORT:int = Secret.load("pg-port").get()
PG_DBNAME:str = Secret.load("pg-dbname").get()

MONGO_HOST:str = Secret.load("mongo-host").get()
MONGO_USER:str = Secret.load("mongo-user").get()
MONGO_PASSWORD:str = Secret.load("mongo-password").get()
MONGO_PORT:int = Secret.load("mongo-port").get()
MONGO_DBNAME:str = Secret.load("mongo-dbname").get()

client = create_mongo_conn(host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD, port=MONGO_PORT, dbname=MONGO_DBNAME)

@task
def get_data(batch_size: int = 1000) -> Generator[List[dict], None, None]:
    query = String.load("golden-query").value
    try:
        connection = create_pg_conn(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, port=PG_PORT)
        cursor = connection.cursor()
        cursor.execute(query)

        while batch := cursor.fetchmany(batch_size):
            #convert batch to list[dict]
            keys = [desc[0] for desc in cursor.description]
            records = [dict(zip(keys,row)) for row in batch]
            yield records
    except Exception as e:
        raise e


@flow
def make_golden_records(batch: List[dict]) -> List[GoldenCustomer]:
    golden_records = []
    last_rentals = []
if __name__ == '__main__':
    make_golden_record()