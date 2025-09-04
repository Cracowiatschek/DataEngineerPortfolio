import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from typing import List
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from utills.db import create_mongo_conn, create_pg_conn
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from prefect.blocks.system import Secret, String
from config.JSONConfig import JSONConfig


MONGO_HOST:str = Secret.load("mongo-host").get()
MONGO_USER:str = Secret.load("mongo-user").get()
MONGO_PASSWORD:str = Secret.load("mongo-password").get()
MONGO_PORT:int = Secret.load("mongo-port").get()
MONGO_DBNAME:str = Secret.load("mongo-dbname").get()


@task
def get_customers(collection: str, fields: List[str]) -> List[dict]:
    try:
        client = create_mongo_conn(host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD, port=MONGO_PORT,
                                   dbname=MONGO_DBNAME)
        database = client[MONGO_DBNAME]
        collection = database[collection]
        docs = collection.find({},{i:1 for i in fields})
        return list(docs)

    except Exception as e:
        print(e)
        raise


@flow
def manual_init_model():
    jsonconfig_block = JSONConfig.load("ml-pipeline-config").data

    data = get_customers(jsonconfig_block["collection"], jsonconfig_block["fields"])
    for i in data:
        if i["_id"] < 100:
            print(i)
        else:
            break


if __name__ == '__main__':
    manual_init_model()
