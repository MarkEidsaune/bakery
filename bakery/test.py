### Data Tests
# import os
# from data.alpha import Alpha

# path = os.path.join(os.path.expanduser('~'), "git/bakery/bakery/data/config.json")
# alpha = Alpha(path)

# Active list
# result = alpha.etl_active_list()

# Daily
# symbols = ["TSLA", "MSFT", "IBM", "AAPL"]
# result = alpha.etl_daily(symbols=symbols)

# Hourly
# symbols = ["NVDA", "META"]
# years = list(range(2022, 2024))
# months = list(range(1, 13))
# result = alpha.etl_hourly(symbols=symbols, years=years, months=months)


### Dash Tests
import os
import json
import streamlit as st
from pymongo import MongoClient
import pandas as pd

path = os.path.join(os.path.expanduser('~'), "git/bakery/bakery/data/config.json")

with open(path, "rb") as f:
    config = json.loads(f.read().decode())

client = MongoClient(config["mongo"]["conn_str"])
db = client["bakery"]
coll = db["alpha_daily"]

query = {"symbol": "TSLA"}
df = pd.DataFrame(list(coll.find(query)))

print(df.head(25))