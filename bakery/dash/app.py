from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from cassandra.cluster import Cluster
import pandas as pd

# Connect to DB
session = Cluster().connect()

# Get a list of symbols for the dropdown
def list_factory(colnames, rows):
    return [row[0] for row in rows]
session.row_factory = list_factory

query = 'select symbol from bakery.symbols;'
symbols = session.execute(query, timeout=None)
symbols = [symbol for symbol in symbols]

app = Dash(__name__)

app.layout = html.Div([
    html.H4('Bakery Dashboard'),
    dcc.Dropdown(symbols, 'MSFT', id='symbol-dropdown'),
    dcc.Graph(id='candles')
])

@app.callback(
    Output('candles', 'figure'),
    Input('symbol-dropdown', 'value')
)
def update_candles(value):
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    query = f'''
        select
            symbol, dt, open_price, high_price, low_price, close_price
        from
            bakery.prices_daily
        where
            symbol = '{value}'
        ;
    '''
    result = session.execute(query, timeout=None)
    df = result._current_rows
    df['dt'] = pd.to_datetime(df['dt'].astype(str))

    fig = go.Figure(go.Candlestick(
        x=df['dt'],
        open=df['open_price'],
        high=df['high_price'],
        low=df['low_price'],
        close=df['close_price']
    ))

    fig.update_layout(
        xaxis_rangeslider_visible=True
    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
