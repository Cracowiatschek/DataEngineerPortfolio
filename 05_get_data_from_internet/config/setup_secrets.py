import os
from dotenv import load_dotenv
from prefect.blocks.system import Secret


load_dotenv()

Secret(value=os.getenv("DB_PG_HOST")).save("pg-host", overwrite=True)
Secret(value=os.getenv("DB_PG_USER")).save("pg-user", overwrite=True)
Secret(value=os.getenv("DB_PG_PASSWORD")).save("pg-password", overwrite=True)
Secret(value=os.getenv("DB_PG_PORT")).save("pg-port", overwrite=True)
Secret(value=os.getenv("DB_PG_NAME")).save("pg-dbname", overwrite=True)
