import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Initialize the Dash app
app = dash.Dash(__name__)

# Create database connection
engine = create_engine('postgresql://airflow:airflow@postgres:5432/stockdata')

# Layout
app.layout = html.Div([
    html.H1('Stock Market Dashboard'),
    
    # Stock selector dropdown
    html.Div([
        html.Label('Select Stock:'),
        dcc.Dropdown(
            id='stock-selector',
            options=[
                {'label': stock, 'value': stock} for stock in 
                ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'TSLA', 'NVDA', 'NFLX', 'AMD', 'INTC']
            ],
            value='AAPL'
        )
    ]),
    
    # Time range selector
    html.Div([
        html.Label('Select Time Range:'),
        dcc.RadioItems(
            id='time-range',
            options=[
                {'label': '1 Hour', 'value': '1H'},
                {'label': '4 Hours', 'value': '4H'},
                {'label': '1 Day', 'value': '1D'}
            ],
            value='1H'
        )
    ]),
    
    # Graphs
    html.Div([
        dcc.Graph(id='price-graph'),
        dcc.Graph(id='volume-graph'),
        dcc.Graph(id='change-graph')
    ]),
    
    # Auto-refresh interval
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # refresh every minute
        n_intervals=0
    )
])

@app.callback(
    [Output('price-graph', 'figure'),
     Output('volume-graph', 'figure'),
     Output('change-graph', 'figure')],
    [Input('stock-selector', 'value'),
     Input('time-range', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_graphs(selected_stock, time_range, n):
    # Calculate time range
    now = datetime.now()
    if time_range == '1H':
        start_time = now - timedelta(hours=1)
    elif time_range == '4H':
        start_time = now - timedelta(hours=4)
    else:  # 1D
        start_time = now - timedelta(days=1)
    
    # Query data from PostgreSQL
    query = f"""
    SELECT window.start as timestamp, avg_price, avg_volume, avg_change
    FROM stock_metrics
    WHERE symbol = '{selected_stock}'
    AND window.start >= '{start_time}'
    ORDER BY window.start
    """
    df = pd.read_sql(query, engine)
    
    # Create price figure
    price_fig = go.Figure()
    price_fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['avg_price'],
        mode='lines',
        name='Price'
    ))
    price_fig.update_layout(
        title=f'{selected_stock} Price',
        xaxis_title='Time',
        yaxis_title='Price ($)'
    )
    
    # Create volume figure
    volume_fig = go.Figure()
    volume_fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['avg_volume'],
        name='Volume'
    ))
    volume_fig.update_layout(
        title=f'{selected_stock} Volume',
        xaxis_title='Time',
        yaxis_title='Volume'
    )
    
    # Create change percent figure
    change_fig = go.Figure()
    change_fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['avg_change'],
        mode='lines',
        name='Change %',
        line=dict(
            color='green',
            width=2
        )
    ))
    change_fig.update_layout(
        title=f'{selected_stock} Price Change %',
        xaxis_title='Time',
        yaxis_title='Change %'
    )
    
    return price_fig, volume_fig, change_fig

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True) 