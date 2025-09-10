from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Cracowiatschek/DataEngineerPortfolio.git",
        entrypoint="04_ml_pipeline/run_model.py:run_scoring",
    ).deploy(
        name="categorize-customers",
        work_pool_name="postgres",
        cron="30 1 * * *",
    )