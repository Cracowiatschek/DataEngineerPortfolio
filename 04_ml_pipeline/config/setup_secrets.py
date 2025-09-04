import os
from dotenv import load_dotenv
from prefect.blocks.system import Secret


load_dotenv()

Secret(value=os.getenv("SG_API_KEY")).save("sendgrid-api-key", overwrite=True)

