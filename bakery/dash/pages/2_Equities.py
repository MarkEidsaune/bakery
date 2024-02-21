import os
import json
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
from bokeh.plotting import ColumnDataSource, figure 
from bokeh.models import BooleanFilter, CDSView, ColumnDataSource, HoverTool

st.set_page_config(
    page_icon=":moneybag:",
)
st.write("# Equities :moneybag:")
symbol = st.text_input("Symbol", placeholder="Enter Symbol...", value="ANF")

# Get date range
default_end = datetime.now()
default_start = default_end - timedelta(weeks=52)
min_dt = datetime(2010, 1, 1)
date1 = st.date_input(
    label="Start Date",
    value=default_start,
    min_value=min_dt,
    max_value=default_end,
    format="YYYY/MM/DD",
    disabled=False,
    label_visibility="visible"
)
date2 = st.date_input(
    label="End Date",
    value=default_end,
    min_value=min_dt,
    max_value=default_end,
    format="YYYY/MM/DD",
    disabled=False,
    label_visibility="visible"
)

# Init MongoDB
path = os.path.join(os.path.expanduser('~'), "git/bakery/bakery/data/config.json")
with open(path, "rb") as f:
    config = json.loads(f.read().decode())
client = MongoClient(config["mongo"]["conn_str"])
db = client["bakery"]
alpha_daily_coll = db["alpha_daily"]

# Convert dates to datetimes (for mongo)
range_start = datetime.combine(date1, datetime.min.time())
range_end = datetime.combine(date2, datetime.min.time())

# Get data
symbol_query = {"symbol": symbol, "dttm": {"$gte": range_start, "$lte": range_end}}
fields_query = {"_id": 0, "split_coef": 0, "div_amt": 0}
try:
    df = pd.DataFrame(list(alpha_daily_coll.find(symbol_query, fields_query)))
    # Clean data
    df.rename(columns={"dttm": "Date", "symbol": "Symbol", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)
    df["Date"] = df["Date"].dt.date
    source = ColumnDataSource(data=df)
    is_inc = [close > open for (close, open) in zip(source.data["Close"], source.data["Open"])]
    is_dec = [not x for x in is_inc]
    inc_view = CDSView(source=source, filters=[BooleanFilter(is_inc)])
    dec_view = CDSView(source=source, filters=[BooleanFilter(is_dec)])

    # Color palette
    palette = ["#a6cee3","#1f78b4","#b2df8a","#33a02c","#fb9a99","#e31a1c","#fdbf6f","#ff7f00","#cab2d6","#6a3d9a","#ffff99","#b15928"]

    # Candlestick plot
    w = 16*60*60*1000
    hover = HoverTool(
        tooltips=[("Symbol", "@Symbol"), ("Date", "@Date{%F}"), ("Volume", "@Volume"), ("Open", "@Open"), ("High", "@High"), ("Low", "@Low"), ("Close", "@Close")],
        formatters={"@Date": "datetime"},
        mode='vline'
    )
    tools = "reset"
    p = figure(x_axis_type="datetime", tools=tools, width=1000, height=400,
            title=f"{symbol} Candlesticks", background_fill_color="white")
    p.add_tools(hover)
    p.xaxis.major_label_orientation = 0.8
    p.segment("Date", "High", "Date", "Low", color=palette[5], source=source, view=dec_view)
    p.vbar("Date", w, "Open", "Close", fill_color=palette[4], line_color=palette[5], line_width=1, source=source, view=dec_view)
    p.segment("Date", "High", "Date", "Low", color=palette[3], source=source, view=inc_view)
    p.vbar("Date",  w, "Open", "Close", fill_color=palette[2], line_color=palette[3], line_width=1, source=source, view=inc_view)

    st.bokeh_chart(p, use_container_width=True)
except KeyError as e:
    st.write("Empty response from database. Make sure you entered the symbol correctly.")