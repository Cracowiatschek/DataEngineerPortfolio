from prefect.blocks.system import String


sql_block = String(value=open("get_cities_without_coordinates.sql", encoding='utf-8').read())
sql_block.save("cities-without-coordinates", overwrite=True)


sql_block = String(value=open("tmp_cities.sql", encoding='utf-8').read())
sql_block.save("create-tmp-cities", overwrite=True)

sql_block = String(value=open("update_cities_from_tmp.sql", encoding='utf-8').read())
sql_block.save("update-coordinates", overwrite=True)
