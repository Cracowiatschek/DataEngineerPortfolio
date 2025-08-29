from prefect.blocks.system import String


sql_block = String(value=open("golden_query.sql", encoding='utf-8').read())
sql_block.save("golden-query", overwrite=True)
