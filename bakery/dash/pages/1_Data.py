import os
import json
from pymongo import MongoClient
import pandas as pd
import streamlit as st
from bokeh.models import ColorBar, LinearColorMapper, BasicTicker, DatetimeTickFormatter
from bokeh.plotting import figure, ColumnDataSource

st.set_page_config(
    page_icon=":bar_chart:",
)
st.write("# Data :bar_chart:")

def extract(limit=100, sort_by="MarketCapitalization"):
    # Init MongoDB
    path = os.path.join(os.path.expanduser('~'), "git/bakery/bakery/data/config.json")
    with open(path, "rb") as f:
        config = json.loads(f.read().decode())
    client = MongoClient(config["mongo"]["conn_str"])
    db = client["bakery"]

    # Get company overviews
    alpha_company_overview_coll = db["alpha_company_overview"]
    cursor = alpha_company_overview_coll.find({}).sort(sort_by, -1).limit(limit)
    overview_pdf = pd.DataFrame(list(cursor))

    # Get active list
    alpha_daily_active_ls_coll = db["alpha_daily_active_ls"]
    cursor = alpha_daily_active_ls_coll.find({})
    daily_active_ls_pdf = pd.DataFrame(list(cursor))

    return (overview_pdf, daily_active_ls_pdf)

@st.cache_data
def transform(overview_pdf, daily_active_ls_pdf):

    # Get list of symbols from overview_pdf
    top_symbols = overview_pdf["Symbol"].to_list()

    # Transform data
    df_explode = daily_active_ls_pdf.drop(columns="_id") \
        .explode(column="ls") \
        .reset_index(drop=True) \
        .rename(columns={"ls": "symbol"})
    df_pivot = df_explode \
        .pivot_table(index="dttm", columns="symbol", aggfunc="size", fill_value=0) \
        .sort_values(by="dttm")

    df_pivot.index.names = ["Date"]
    df_pivot.columns.name = "Symbol"

    df_pivot = df_pivot[top_symbols]
    
    dates = list([dt.strftime("%m/%d/%Y") for dt in df_pivot.index])
    symbols = list(df_pivot.columns)

    return (overview_pdf, pd.DataFrame(df_pivot.stack(), columns=["active"]).reset_index(), dates, symbols)

def plot(overview_pdf, active_pdf, dates, symbols):
    
    source = ColumnDataSource(data=active_pdf)

    colors = ["#bcbd22", "#ff9896"]
    mapper = LinearColorMapper(palette=colors, low=0, high=1)

    TOOLS = "hover,save,pan,box_zoom,reset,wheel_zoom"

    p = figure(
        title="Active Equities Heatmap",
        x_axis_type="datetime",  
        x_range=dates, y_range=symbols,
        width=1200, height=900,
        tools=TOOLS, toolbar_location="below",
        tooltips=[("symbol", "@Symbol"), ("date", "@Date"), ("active", "@active")]
    )

    p.grid.grid_line_color = None
    p.axis.axis_line_color = None
    p.axis.major_tick_line_color = None
    p.axis.major_label_text_font_size = "4px"
    p.axis.major_label_standoff = 0
    p.xaxis.formatter = DatetimeTickFormatter(years="%Y", months="%m/%Y", days="%d/%m/%Y")
    p.xaxis.major_label_orientation = 3.14 / 3

    p.rect(x="Date", y="Symbol", width=1, height=1,
        source=source,
        fill_color={"field": "active", "transform": mapper},
        line_color=None)

    color_bar = ColorBar(color_mapper=mapper, major_label_text_font_size="4px",
                         ticker=BasicTicker(desired_num_ticks=2))
    p.add_layout(color_bar, "right")

    return p, overview_pdf
   
p, overview_pdf = plot(*transform(*extract()))
# st.table(overview_pdf)
st.bokeh_chart(p, use_container_width=True)