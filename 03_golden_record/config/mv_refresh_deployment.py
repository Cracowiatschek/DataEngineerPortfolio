from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Cracowiatschek/DataEngineerPortfolio.git",
        entrypoint="03_golden_record/refresh_views.py:refresh_views",
    ).deploy(
        name="refresh-customer-materialized-views",
        work_pool_name="postgres",
        cron="0 1 * * *",
    )