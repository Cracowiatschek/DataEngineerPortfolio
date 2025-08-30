import os
from dotenv import load_dotenv
from prefect.blocks.system import Secret


load_dotenv()

Secret(value=os.getenv("DB_MONGO_HOST")).save("mongo-host", overwrite=True)
Secret(value=os.getenv("DB_MONGO_USER")).save("mongo-user", overwrite=True)
Secret(value=os.getenv("DB_MONGO_PASSWORD")).save("mongo-password", overwrite=True)
Secret(value=os.getenv("DB_MONGO_PORT")).save("mongo-port", overwrite=True)
Secret(value=os.getenv("DB_MONGO_NAME")).save("mongo-dbname", overwrite=True)
