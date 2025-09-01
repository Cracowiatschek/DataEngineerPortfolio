from pymongo import MongoClient
import dotenv
import os

dotenv.load_dotenv()


uri = f"mongodb://{os.getenv("DB_MONGO_USER")}:{os.getenv("DB_MONGO_PASSWORD")}@{os.getenv("DB_MONGO_HOST")}:{int(os.getenv("DB_MONGO_PORT"))}/{os.getenv("DB_MONGO_NAME")}"
conn = MongoClient(uri)


db= conn.get_database(os.getenv("DB_MONGO_NAME"))
coll = db.get_collection("CustomersGoldenRecord")

res = coll.find({"country": "Poland"})
for doc in res:
    print(doc)