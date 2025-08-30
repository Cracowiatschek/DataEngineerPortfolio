from config.CityLocation import CityLocation
import os
from datetime import datetime
from typing import Any
from utills.db import create_pg_conn, NotAllViewsRefreshed
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from dotenv import load_dotenv
from collections import deque, namedtuple



@flow
def enrich_data():
    config_block = CityLocation.load("city-location-path").object
    print(config_block)


if __name__ == "__main__":
    enrich_data()