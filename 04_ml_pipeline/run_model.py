import collections.abc

collections.Mapping = collections.abc.Mapping
collections.MutableMapping = collections.abc.MutableMapping
collections.Sequence = collections.abc.Sequence

from pymongo import UpdateOne
import numpy as np
import pandas as pd
from typing import List
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from utills.db import create_mongo_conn
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from config.Pickle import PickleBlock
from config.JSONConfig import JSONConfig



MONGO_HOST:str = Secret.load("mongo-host").get()
MONGO_USER:str = Secret.load("mongo-user").get()
MONGO_PASSWORD:str = Secret.load("mongo-password").get()
MONGO_PORT:int = Secret.load("mongo-port").get()
MONGO_DBNAME:str = Secret.load("mongo-dbname").get()


@task
def get_customers(collection: str, fields: List[str]) -> List[dict]:
    logger = get_run_logger()
    try:
        client = create_mongo_conn(host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD, port=MONGO_PORT,
                                   dbname=MONGO_DBNAME)
        database = client[MONGO_DBNAME]
        collection = database[collection]
        docs = collection.find({},{i:1 for i in fields})
        docs = list(docs)
        logger.info(f"Got {len(docs)} customers from GoldenRecord")
        return docs

    except Exception as e:
        print(e)
        logger.error(e)
        raise


@task
def enrich_data(data: List[dict]) -> pd.DataFrame:
    logger = get_run_logger()
    data = pd.DataFrame(data)
    columns = [i for i in data.columns if i != "_id"]

    new_features = {
        f"{col1}_div_{col2}": data[col1] / data[col2]
        for col1 in columns
        for col2 in columns if col1 != col2
    }
    extended_data = pd.concat([data, pd.DataFrame(new_features)], axis=1)
    logger.info("Enriched data about divided features")

    new_features = {
        f"{col1}_mlt_{col2}": extended_data[col1] * extended_data[col2]
        for col1 in columns
        for col2 in columns if col1 != col2
    }
    extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)
    logger.info("Enriched data about multiplication features")

    new_features = {
        f"log_{col1}": np.log(extended_data[col1])
        for col1 in columns
    }
    extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)
    logger.info("Enriched data about logarithm features")

    new_features = {
        f"sqrt_{col1}": np.sqrt(extended_data[col1])
        for col1 in columns
    }
    extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)
    logger.info("Enriched data about square root features")

    new_features = {
        f"pow_{col1}": np.power(extended_data[col1], 2)
        for col1 in columns
    }
    extended_data = pd.concat([extended_data, pd.DataFrame(new_features)], axis=1)
    logger.info("Enriched data about power features")

    extended_data.replace([np.inf, -np.inf], np.nan, inplace=True)
    extended_data.fillna(0, inplace=True)
    logger.info("Cleaned up data")

    return extended_data


@task
def standardize(data: pd.DataFrame) -> pd.DataFrame:
    scaler = StandardScaler()
    scaled = data[[i for i in data.columns if i != "_id"]]
    return scaler.fit_transform(scaled)


@task
def train_model(data: pd.DataFrame, labels: dict) -> dict:
    logger = get_run_logger()
    kmeans = KMeans(n_clusters=len(labels), random_state=42)
    labels = kmeans.fit_predict(data)
    score = silhouette_score(data, labels, metric="euclidean")
    if score >= 0.5:
        logger.info(f"Scored successful! Training model with silhouette score {score} for {len(labels)} centroids")
        return {"labels": labels, "score": score, "model": kmeans}
    else:
        logger.warning(f"Low quality! Training model with silhouette score {score} for {len(labels)} centroids")
        return {"labels": labels, "score": score}


@task
def score_data(data: pd.DataFrame, model, labels) -> dict:
    logger = get_run_logger()
    results = model.predict(data)
    score = silhouette_score(data, results, metric="euclidean")
    logger.info(f"Scored successful! Training model with silhouette score {score} for {len(labels)} centroids")
    return {"labels": results, "score": score}


@task
def map_categories(data: pd.DataFrame, labels: list, labels_description: dict) -> list:
    logger = get_run_logger()

    data["labels"] = labels

    setup_category = data[[col for col in data.columns if col != "_id"]].copy().groupby('labels').mean(numeric_only=True)
    setup_category['product'] = setup_category.prod(axis=1)
    setup_category = setup_category.groupby('labels')['product'].first()
    sorted_labels = setup_category.sort_values(ascending=True).index.tolist()
    logger.info(f"Sorted categories by scoring result")

    categories = {old_label: new_label for new_label, old_label in enumerate(sorted_labels)}
    categories_desc = {}
    for label in categories:
        categories[label] = labels_description[str(label)]["name"]
        categories_desc[label] = labels_description[str(label)]["description"]

    data["category"] = data["labels"].map(categories)
    data["category_description"] = data["labels"].map(categories_desc)
    logger.info(f"Categories setup complete")

    output = data[["_id", "category", "category_description"]]

    return output.to_dict(orient="records")


@task
def upsert_categories(data: list[dict]):
    logger = get_run_logger()
    try:
        client = create_mongo_conn(host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD, port=MONGO_PORT,
                                   dbname=MONGO_DBNAME)
        database = client[MONGO_DBNAME]
        collection = database["CustomersGoldenRecord"]

        operations = []
        for record in data:
            operations.append(
                UpdateOne({"_id": record["_id"]}, {"$set": {"category": {"name": record["category"], "category_description": record["category_description"]}}}, upsert=True)
            )

        if operations:
            collection.bulk_write(operations)
        logger.info(f"{len(operations)} records processed")
    except Exception as e:
        logger.error(f"Exception while updating records:\n{e}")
        raise e


@flow
def run_scoring():
    logger = get_run_logger()

    jsonconfig_block = JSONConfig.load("ml-pipeline-config").data
    labels = jsonconfig_block["labels"]
    logger.info(f"Got config")
    try:
        model = PickleBlock.load(jsonconfig_block["model"])
        model = model.load_model()
        logger.info(f"Model loaded successfully!")
    except Exception as e:
        print(e)
        model = None
        logger.info(f"Model failed to load! Model will be created in the next steps.")

    data = get_customers(jsonconfig_block["collection"], jsonconfig_block["fields"])
    enriched_data = enrich_data(data)
    # standardized_data = standardize(enriched_data) #actualy it's not necessarily

    if model is None:
        result_model = train_model(data=enriched_data, labels=labels)
        if result_model["score"] >= 0.5:
            PickleBlock.save_model(model=result_model["model"], name=jsonconfig_block["model"], overwrite=True)
            logger.info(f"Model saved successfully!")
        else:
            raise
    else:
        result_model = score_data(data=enriched_data, model=model, labels=labels )
        if result_model["score"] < 0.5:
            result_model = train_model(data=enriched_data, labels=labels)
            if result_model["score"] >= 0.5:
                PickleBlock.save_model(model=result_model["model"], name=jsonconfig_block["model"], overwrite=True)
                logger.info(f"Model saved successfully!")
            else:
                raise

    result = map_categories(pd.DataFrame(data), result_model["labels"], labels)
    upsert_categories(result)


if __name__ == '__main__':
    run_scoring()
