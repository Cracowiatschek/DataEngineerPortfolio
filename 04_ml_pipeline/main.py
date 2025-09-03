import collections.abc

from scipy.cluster.vq import kmeans

collections.Mapping = collections.abc.Mapping
collections.MutableMapping = collections.abc.MutableMapping
collections.Sequence = collections.abc.Sequence

from datetime import datetime
from typing import Generator, List
from pymongo import UpdateOne
import pandas as pd
import polars as pl
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, DBSCAN
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from utills.db import create_mongo_conn, create_pg_conn
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from prefect.blocks.system import Secret
from prefect.blocks.system import String


MONGO_HOST:str = Secret.load("mongo-host").get()
MONGO_USER:str = Secret.load("mongo-user").get()
MONGO_PASSWORD:str = Secret.load("mongo-password").get()
MONGO_PORT:int = Secret.load("mongo-port").get()
MONGO_DBNAME:str = Secret.load("mongo-dbname").get()



def get_customers() -> List[dict]:
    try:
        client = create_mongo_conn(host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD, port=MONGO_PORT,
                                   dbname=MONGO_DBNAME)
        database = client[MONGO_DBNAME]
        collection = database["CustomersGoldenRecord"]
        docs = collection.find({})
        return list(docs)

    except Exception as e:
        print(e)
        raise

data = pd.DataFrame(get_customers())
data = data[["_id", "overdue_score", "lifetime_value", "total_rental_count", "average_rental_duration",
             "average_rental_payment", "last_year_rental_count", "last_year_payments_sum", "last_payment"]].copy()

extended_data = data.copy()
cols = [i for i in extended_data.columns if i != "_id"]

new_features = {
    f"{col1}_div_{col2}": extended_data[col1] / extended_data[col2]
    for col1 in cols
    for col2 in cols if col1 != col2
}
extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)

new_features = {
    f"{col1}_mlt_{col2}": extended_data[col1] * extended_data[col2]
    for col1 in cols
    for col2 in cols if col1 != col2
}
extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)

new_features = {
    f"log_{col1}": np.log(extended_data[col1])
    for col1 in cols
}
extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)

new_features = {
    f"sqrt_{col1}": np.sqrt(extended_data[col1])
    for col1 in cols
}
extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)

new_features = {
    f"pow_{col1}": np.power(extended_data[col1], 2)
    for col1 in cols
}
extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)

extended_data.replace([np.inf, -np.inf], np.nan, inplace=True)
extended_data.fillna(0, inplace=True)

cols = [i for i in extended_data.columns if i != "_id"]
def model(data):
    scaler = StandardScaler()
    X = scaler.fit_transform(extended_data[cols].copy())
    kmeans = KMeans(n_clusters=2, random_state=42)
    labels = kmeans.fit_predict(X)
    data["segments"] = labels
    return data, silhouette_score(X, labels), X


def model2(data):
    scaler = StandardScaler()
    X = scaler.fit_transform(extended_data[cols].copy())
    dbscan = DBSCAN(eps=2)
    labels = dbscan.fit_predict(X)
    data["segments"] = labels
    return data, silhouette_score(X, labels), X
result = model(data)
print(result[1])

result = model2(data)
print(result[1])

pca = PCA(n_components=2)
X_pca = pca.fit_transform(result[2])

plt.figure(figsize=(8, 6))
scatter = plt.scatter(
    X_pca[:, 0], X_pca[:, 1],
    c=result[0]["segments"], cmap="viridis", s=80, alpha=0.7
)
plt.xlabel("PCA 1")
plt.ylabel("PCA 2")
plt.title("Klienci - PCA (kolory = klastry KMeans)")
plt.colorbar(scatter, label="Cluster")
plt.show()