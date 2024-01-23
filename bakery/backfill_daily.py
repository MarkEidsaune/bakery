import os
import json
from pymongo import MongoClient
import pandas as pd

from data.alpha import Alpha

# Get list of symbols
path = os.path.join(os.path.expanduser('~'), "git/bakery/bakery/data/config.json")
with open(path, "rb") as f:
    config = json.loads(f.read().decode())
client = MongoClient(config["mongo"]["conn_str"])

# ETL active list
coll = client["bakery"]["alpha_active_list"]
alpha = Alpha(path)
result = alpha.etl_active_list()

# Get list of active symbols
symbols = [s["symbol"] for s in list(coll.find({}, {"symbol": 1, "_id": 0}))]

# ETL daily candles
result = alpha.etl_daily(symbols)
