from datetime import datetime
from typing import Generator, List
from pymongo import UpdateOne
from data_model import GoldenCustomer, GoldenRental, Source
from utills.db import create_mongo_conn, create_pg_conn
from prefect import task, flow, get_run_logger
from prefect.assets import materialize
from prefect.blocks.system import Secret
from prefect.blocks.system import String


PG_HOST:str = Secret.load("pg-host").get()
PG_USER:str = Secret.load("pg-user").get()
PG_PASSWORD:str = Secret.load("pg-password").get()
PG_PORT:int = Secret.load("pg-port").get()
PG_DBNAME:str = Secret.load("pg-dbname").get()

MONGO_HOST:str = Secret.load("mongo-host").get()
MONGO_USER:str = Secret.load("mongo-user").get()
MONGO_PASSWORD:str = Secret.load("mongo-password").get()
MONGO_PORT:int = Secret.load("mongo-port").get()
MONGO_DBNAME:str = Secret.load("mongo-dbname").get()


@task
def get_data(batch_size: int = 1000) -> Generator[List[dict], None, None]:
    logger = get_run_logger()
    query = String.load("golden-query").value
    logger.info(f"Get query from prefect.blocks:\n{query}")
    try:
        connection = create_pg_conn(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DBNAME, port=PG_PORT)
        cursor = connection.cursor()
        cursor.execute(query)

        while batch := cursor.fetchmany(batch_size):
            #convert batch to list[dict]
            keys = [desc[0] for desc in cursor.description]
            records = [dict(zip(keys,row)) for row in batch]
            logger.info(f"Got {len(records)} records in batch")
            yield records
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        raise e


@task
def make_golden_records(batch: List[dict]) -> List[GoldenCustomer]:
    logger = get_run_logger()
    golden_records = []

    for record in batch:
        last_rentals = record.get("last_rentals", [])

        try:
            golden_record = GoldenCustomer(
                id = record.get("_id"),
                first_name = record.get("first_name"),
                last_name = record.get("last_name"),
                is_active = record.get("is_active"),
                full_address = record.get("full_address"),
                address = record.get("address"),
                district = record.get("district"),
                city = record.get("city"),
                country = record.get("country"),
                latitude = record.get("latitude"),
                longitude = record.get("longitude"),
                phone = record.get("phone"),
                email = record.get("email"),
                postal_code = record.get("postal_code"),
                assistant_name=record.get("assistant_name"),
                assistant_email=record.get("assistant_email"),
                overdue_score=record.get("overdue_score"),
                most_recent_store=record.get("most_recent_store"),
                last_rental_film=record.get("last_rental_film"),
                last_rental_date=record.get("last_rental_date"),
                lifetime_value=record.get("lifetime_value"),
                total_rental_count=record.get("total_rental_count"),
                average_rental_duration=record.get("average_rental_duration"),
                average_rental_payment=record.get("average_rental_payment"),
                average_film_duration=record.get("average_film_duration"),
                last_year_rental_count=record.get("last_year_rental_count"),
                last_year_payments_sum=record.get("last_year_payments_sum"),
                last_payment=record.get("last_payment"),
                most_recent_film_category=record.get("most_recent_film_category"),
                second_most_recent_film_category=record.get("second_most_recent_film_category"),
                third_most_recent_film_category=record.get("third_most_recent_film_category"),
                most_recent_film_title=record.get("most_recent_film_title"),
                most_recent_film_actor=record.get("most_recent_film_actor"),
                most_recent_film_year=record.get("most_recent_film_year"),
                last_ten_rentals=[GoldenRental(**r) for r in last_rentals],
                last_consolidation_date=datetime.now(),
                sources=[
                    Source(
                        _id = record.get("_id"),
                        sub_id=None,
                        path=f"postgres://{PG_HOST}/{PG_DBNAME}/v_dvd_rental/customers_mv",
                        fields=["_id", "first_name", "last_name", "is_active", "full_address", "address", "district",
                                "city", "country", "latitude", "longitude", "phone", "email", "postal_code",
                                "assistant_name", "assistant_email"],
                        last_refreshed=record.get("cm_refresh_date")
                    ),
                    Source(
                        _id=record.get("_id"),
                        sub_id=None,
                        path=f"postgres://{PG_HOST}/{PG_DBNAME}/v_dvd_rental/customer_aggr_mv",
                        fields=["overdue_score", "most_recent_store", "last_rental_film", "last_rental_date",
                                "lifetime_value", "total_rental_count", "average_rental_duration",
                                "average_rental_payment", "average_film_duration", "last_year_rental_count",
                                "last_year_payments_sum", "last_payment", "most_recent_film_category",
                                "second_most_recent_film_category", "third_most_recent_film_category",
                                "most_recent_film_title", "most_recent_film_actor",
                                "most_recent_film_year",],
                        last_refreshed=record.get("cam_refresh_date")
                    ),
                    Source(
                        _id=record.get("_id"),
                        sub_id=[r["_id"] for r in last_rentals],
                        path=f"postgres://{PG_HOST}/{PG_DBNAME}/v_dvd_rental/last_rentals_mv",
                        fields=["last_ten_rentals"],
                        last_refreshed=record.get("lrm_refresh_date")
                    )
                ]
            )

            golden_records.append(golden_record)
        except Exception as e:
            logger.error(f"Validation error for record:\n {record}\n{e}")
    logger.info(f"{len(golden_records)} records in batch is valid")
    return golden_records


@materialize(
    f"mongodb://{MONGO_HOST}/{MONGO_DBNAME}/CustomersGoldenRecord",
    asset_deps=[f"postgres://{PG_HOST}/{PG_DBNAME}/v_dvd_rental/customers_mv",
                f"postgres://{PG_HOST}/{PG_DBNAME}/v_dvd_rental/customer_aggr_mv",
                f"postgres://{PG_HOST}/{PG_DBNAME}/v_dvd_rental/last_rentals_mv"]
)
def materialize_golden_record(records: List[GoldenCustomer]):
    logger = get_run_logger()
    try:
        client = create_mongo_conn(host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD, port=MONGO_PORT,
                                   dbname=MONGO_DBNAME)
        database = client[MONGO_DBNAME]
        collection = database["CustomersGoldenRecord"]

        operations = []
        for record in records:
            operations.append(
                UpdateOne({"_id": record.id}, {"$set": record.model_dump()}, upsert=True)
            )

        if operations:
            collection.bulk_write(operations)
        logger.info(f"{len(operations)} records processed")
    except Exception as e:
        logger.error(f"Exception while updating records:\n{e}")
        raise e


@flow
def upsert_golden_records():
    for batch in get_data(batch_size=1000):
        golden_records = make_golden_records(batch)
        materialize_golden_record(golden_records)


if __name__ == '__main__':
    upsert_golden_records()
