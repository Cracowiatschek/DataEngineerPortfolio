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
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
from utills.db import create_mongo_conn, create_pg_conn
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from prefect.blocks.system import Secret, String


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
             "average_rental_payment"]].copy() #, "last_year_rental_count", "last_year_payments_sum", "last_payment"

extended_data = data.copy()
cols = [i for i in extended_data.columns if i != "_id"]
def_cols = [i for i in extended_data.columns if i != "_id"]

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


# -------------------------------
# 1. Standaryzacja danych
# -------------------------------
scaler = StandardScaler()
X_scaled = extended_data[cols]  # X = Twój DataFrame lub numpy array

# PCA do wizualizacji (tylko do rysowania)
pca = PCA(n_components=2, random_state=42)
X_pca = pca.fit_transform(X_scaled)


# -------------------------------
# 2. Funkcja do oceny i wizualizacji
# -------------------------------
def evaluate_and_plot(model, X, X_pca, model_name):
    labels = model.fit_predict(X)

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    if n_clusters < 2:
        print(f"{model_name}: tylko {n_clusters} klaster, pomijam")
        return None, labels

    score = silhouette_score(X, labels)
    print(f"{model_name}: silhouette = {score:.3f}, liczba klastrów = {n_clusters}")

    # PCA plot
    plt.figure(figsize=(6, 5))
    plt.scatter(X_pca[:, 0], X_pca[:, 1], c=labels, cmap="tab10", s=20)
    plt.title(f"{model_name} (silhouette={score:.3f})")
    plt.xlabel("PCA 1")
    plt.ylabel("PCA 2")
    plt.show()

    return score, labels


# -------------------------------
# 3. Testowane modele
# -------------------------------
results = {}
labels_dict = {}

# KMeans
for k in[ 4]:
    kmeans = KMeans(n_clusters=k, random_state=42)
    score, labels = evaluate_and_plot(kmeans, X_scaled, X_pca, f"KMeans (k={k})")
    results[f"KMeans_{k}"] = score
    labels_dict[f"KMeans_{k}"] = labels

# # Gaussian Mixture
# for k in [3, 4, 5]:
#     gmm = GaussianMixture(n_components=k, random_state=42)
#     labels = gmm.fit_predict(X_scaled)
#     score = silhouette_score(X_scaled, labels)
#     print(f"GMM (k={k}): silhouette = {score:.3f}")
#     plt.figure(figsize=(6, 5))
#     plt.scatter(X_pca[:, 0], X_pca[:, 1], c=labels, cmap="tab10", s=20)
#     plt.title(f"GMM (k={k}) (silhouette={score:.3f})")
#     plt.xlabel("PCA 1")
#     plt.ylabel("PCA 2")
#     plt.show()
#     results[f"GMM_{k}"] = score
#     labels_dict[f"GMM_{k}"] = labels
#
# # Agglomerative
# for k in [3, 4, 5]:
#     agg = AgglomerativeClustering(n_clusters=k)
#     score, labels = evaluate_and_plot(agg, X_scaled, X_pca, f"Agglomerative (k={k})")
#     results[f"Agglomerative_{k}"] = score
#     labels_dict[f"Agglomerative_{k}"] = labels
#
# # OPTICS
# optics = OPTICS(min_samples=10)
# score, labels = evaluate_and_plot(optics, X_scaled, X_pca, "OPTICS")
# results["OPTICS"] = score
# labels_dict["OPTICS"] = labels
#
# # Mean Shift
# ms = MeanShift()
# score, labels = evaluate_and_plot(ms, X_scaled, X_pca, "Mean Shift")
results["MeanShift"] = score
labels_dict["MeanShift"] = labels


# -------------------------------
# 4. Najlepszy model
# -------------------------------
best_model = max(results, key=lambda k: results[k] if results[k] is not None else -1)
print(f"\n🏆 Najlepszy model: {best_model} (silhouette = {results[best_model]:.3f})")

best_labels = labels_dict[best_model]

# -------------------------------
# 5. Heatmapa + barplot liczności segmentów
# -------------------------------
df_viz = extended_data.copy()
df_viz['cluster'] = best_labels

# średnie wartości w segmentach
mean_values = df_viz.groupby('cluster')[def_cols].mean()

# liczność segmentów
counts = df_viz['cluster'].value_counts().sort_index()

# rysowanie gridu
fig, axes = plt.subplots(1, 2, figsize=(18, 6), gridspec_kw={'width_ratios': [3, 1]})

# Heatmapa
sns.heatmap(mean_values, annot=True, fmt=".2f", cmap="coolwarm", ax=axes[0])
axes[0].set_title("Średnie wartości zmiennych wg segmentu")
axes[0].set_xlabel("Zmienne numeryczne")
axes[0].set_ylabel("Segment (klaster)")

# Bar plot liczności
sns.barplot(x=counts.index, y=counts.values, palette="viridis", ax=axes[1])
for i, v in enumerate(counts.values):
    axes[1].text(i, v + 0.5, str(v), ha='center', va='bottom')
axes[1].set_title("Liczność segmentów")
axes[1].set_xlabel("Segment (klaster)")
axes[1].set_ylabel("Liczba klientów")

plt.tight_layout()
plt.show()
