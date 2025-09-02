import json
from CityLocation import CityLocation


with open("json/config.json", 'r', encoding="utf-8") as f:
    config_data = json.load(f)

config_block = CityLocation(object=config_data)

config_block.save("city-location-path", overwrite=True)
