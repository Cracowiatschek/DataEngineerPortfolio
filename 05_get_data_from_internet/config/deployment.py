from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Cracowiatschek/DataEngineerPortfolio.git",
        entrypoint="05_get_data_from_internet/main.py:enrich_data",
    ).deploy(
        name="get-cities-coordinates",
        work_pool_name="postgres",
        cron="0 12 * * 5",
    )