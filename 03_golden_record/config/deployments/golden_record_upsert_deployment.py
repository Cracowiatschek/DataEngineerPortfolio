from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Cracowiatschek/DataEngineerPortfolio.git",
        entrypoint="03_golden_record/golden_record_upsert.py:upsert_golden_records",
    ).deploy(
        name="golden-record-upsert",
        work_pool_name="postgres",
        cron="15 1 * * *",
    )