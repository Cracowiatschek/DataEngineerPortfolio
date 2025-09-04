import json
from JSONConfig import JSONConfig


with open("json/config.json", 'r', encoding="utf-8") as f:
    config_data = json.load(f)


config_block = JSONConfig(data=config_data)

config_block.save("ml-pipeline-config", overwrite=True)
