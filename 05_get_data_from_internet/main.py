from typing import List
from utills.db import create_pg_conn
from prefect import task, flow, get_run_logger
from prefect.blocks.system import String
from prefect.assets import materialize
from prefect.blocks.system import Secret
from collections import deque, namedtuple
from geopy.geocoders import Nominatim
import time



PG_HOST:str = Secret.load("pg-host").get()
PG_USER:str = Secret.load("pg-user").get()
PG_PASSWORD:str = Secret.load("pg-password").get()
PG_PORT:int = Secret.load("pg-port").get()
PG_DBNAME:str = Secret.load("pg-dbname").get()

City = namedtuple("City", ["id", "city", "country", "longitude", "latitude"])


@task
def get_cities_without_coordinates():

    query = String.load("cities-without-coordinates").value

    try:
        with create_pg_conn(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, port=PG_PORT) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall() # select c.city_id, c.city, ctr.country

            return result
    except Exception as e:
        raise e


@task
def make_queue(query_result)-> deque:
    queue = deque()
    for city in query_result:
        queue.append(City(city[0], city[1], city[2], None, None))

    return queue


@materialize(
    f"https://nominatim.openstreetmap.org/search"
)
def get_coordinates(cities: deque[City]) -> List[City]:
    logger = get_run_logger()
    coordinates = []
    geolocator = Nominatim(user_agent=f"{PG_USER}_agent")

    while len(cities) > 0:
        city = cities.popleft()
        try:
            location = geolocator.geocode(f"{city.city}, {city.country}")
            if location:
                logger.info(f"City: {city.city} ({city.country}) - lat: {location.latitude}, lon: {location.longitude}")
                coordinates.append(City(id=city.id,city=city.city,country=city.country,
                                        longitude=location.longitude,latitude=location.latitude))
            else:
                logger.warning(f"City: {city.city} ({city.country}) - coordinates not found - {location}")
        except Exception as e:
            logger.error(f"Error for {city}: {e}")
        time.sleep(1)

    return coordinates


@materialize(
    f"postgres://{PG_HOST}/{PG_DBNAME}/dvd_rental/city",
    asset_deps=[f"postgres://localhost/postgres/public/city" ,f"https://nominatim.openstreetmap.org/search"]
)
def save_coordinates(coordinates: List[City]) -> None:
    logger = get_run_logger()
    create_query = String.load("create-tmp-cities").value
    logger.info(f"Creating query:\n {create_query}")

    update_query = String.load("update-coordinates").value
    logger.info(f"Updating query:\n {update_query}")

    try:
        with create_pg_conn(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, port=PG_PORT) as conn:
            cursor = conn.cursor()
            cursor.execute(create_query) # create tmp_cities (city_id int, longtiude float, latiude float)

            with cursor.copy("COPY tmp_cities (city_id, longitude, latitude) FROM STDIN") as copy:
                for c in coordinates:
                    copy.write_row((c.id, c.longitude, c.latitude))

            cursor.execute(update_query)
            logger.info(f"Updated {len(coordinates)} rows in dvd_rental.city")

    except Exception as e:
        logger.error(f"Error for {coordinates}: {e}")
        conn.rollback()
        raise e


@flow
def enrich_data():
    logger = get_run_logger()
    cities = get_cities_without_coordinates()
    logger.info(f"Number of cities without coordinates: {len(cities)}")
    if len(cities) > 0:
        cities = make_queue(cities)
        coordinates = get_coordinates(cities)
        logger.info(f"Coordinates obtained for {len(cities)} cities.")
        save_coordinates(coordinates)
    else:
        logger.info(f"No cities without coordinates.")


if __name__ == "__main__":
    enrich_data()