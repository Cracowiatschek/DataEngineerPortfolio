import json
from JSONConfig import JSONConfig


with open("views_config.json", 'r', encoding="utf-8") as f:
    config_data = json.load(f)

config_block = JSONConfig(data=config_data)

config_block.save("views-config", overwrite=True)
