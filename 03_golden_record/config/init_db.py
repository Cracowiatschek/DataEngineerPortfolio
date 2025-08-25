import os
from utills.db import create_mongo_conn
from dotenv import load_dotenv

load_dotenv()


client = create_mongo_conn(host=os.getenv("DB_MONGO_HOST"), user=os.getenv("DB_MONGO_USER"),
                           password=os.getenv("DB_MONGO_PASSWORD"), dbname=os.getenv("DB_MONGO_NAME"),
                           port=int(os.getenv("DB_MONGO_PORT")))



database=client[os.getenv("DB_MONGO_NAME")]
database.create_collection('CustomersGoldenRecord')
client.close()
