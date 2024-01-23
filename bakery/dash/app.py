import os
import json
from pymongo import MongoClient
import pandas as pd
import streamlit as st
from bokeh.plotting import figure
from bokeh.palettes import Category20

path = os.path.join(os.path.expanduser('~'), "git/bakery/bakery/data/config.json")

with open(path, "rb") as f:
    config = json.loads(f.read().decode())

client = MongoClient(config["mongo"]["conn_str"])
db = client["bakery"]
coll = db["alpha_daily"]

query = {}
df = pd.DataFrame(list(coll.find(query)))

st.dataframe(df)

p = figure(
    title="Stonks",
    x_axis_label="Date",
    y_axis_label="Adjusted Close"
)
num_symbols = 4
palette = Category20[4]
for i, symbol in enumerate(df["symbol"].unique()):
    x = df.loc[df["symbol"] == symbol, "dttm"]
    y = df.loc[df["symbol"] == symbol, "adj_close"]
    p.line(x, y, line_color=palette[i])

st.bokeh_chart(p, use_container_width=True)