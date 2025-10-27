import os
import requests
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
import streamlit as st

load_dotenv()
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL = os.getenv("INTERVAL", "1h")
LIMIT = int(os.getenv("LIMIT", 100))


@st.cache_data
def get_binance_ohlcv():
    url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
    data = requests.get(url).json()
    df = pd.DataFrame(
        data,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df[["open", "high", "low", "close", "volume"]] = df[
        ["open", "high", "low", "close", "volume"]
    ].astype(float)
    return df[["timestamp", "open", "high", "low", "close", "volume"]]


st.set_page_config(page_title="Gráfico BTC/USDT", layout="wide")
st.title(f"📈 Gráfico de {SYMBOL} ({INTERVAL})")

df = get_binance_ohlcv()

fig = go.Figure(
    data=[
        go.Candlestick(
            x=df["timestamp"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            increasing_line_color="green",
            decreasing_line_color="red",
        )
    ]
)
fig.update_layout(
    xaxis_rangeslider_visible=False,
    title=f"{SYMBOL} - {INTERVAL} - Últimas {LIMIT} velas",
    xaxis_title="Fecha",
    yaxis_title="Precio (USDT)",
    template="plotly_dark",
)

st.plotly_chart(fig, use_container_width=True)
