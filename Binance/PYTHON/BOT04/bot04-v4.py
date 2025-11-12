import os
import requests
import pandas as pd
from dotenv import load_dotenv
from telegram import Bot

# === CARGAR VARIABLES ===
load_dotenv()
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL = os.getenv("INTERVAL", "1h")
LIMIT = int(os.getenv("LIMIT", 100))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError(
        "❌ TELEGRAM_TOKEN o TELEGRAM_CHAT_ID no están definidos en el entorno."
    )

bot = Bot(token=TELEGRAM_TOKEN)


# === CARGAR VELAS DE BINANCE ===
def get_binance_ohlcv(symbol=SYMBOL, interval=INTERVAL, limit=LIMIT):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    response = requests.get(url)
    data = response.json()
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


# === DETECCIÓN DE PATRONES ===
def detect_patterns(df):
    signals = []
    for i in range(2, len(df)):
        o1, h1, l1, c1 = df.iloc[i - 2][["open", "high", "low", "close"]]
        o2, h2, l2, c2 = df.iloc[i - 1][["open", "high", "low", "close"]]
        o3, h3, l3, c3 = df.iloc[i][["open", "high", "low", "close"]]

        # === PATRONES DE UNA VELA ===
        body = abs(c3 - o3)
        upper_wick = h3 - max(c3, o3)
        lower_wick = min(c3, o3) - l3
        total_range = h3 - l3

        if body < total_range * 0.3 and lower_wick > body * 2:
            signals.append((i, "Hammer"))
        elif body < total_range * 0.3 and upper_wick > body * 2:
            signals.append((i, "Shooting Star"))
        elif body < total_range * 0.1:
            signals.append((i, "Doji"))
        elif body < total_range * 0.3 and lower_wick > body * 2 and c3 < o3:
            signals.append((i, "Hanging Man"))

        # === PATRONES DE DOS VELAS ===
        if c2 < o2 and c3 > o3 and c3 > o2 and o3 < c2:
            signals.append((i, "Bullish Engulfing"))
        elif c2 > o2 and c3 < o3 and c3 < o2 and o3 > c2:
            signals.append((i, "Bearish Engulfing"))
        elif c2 < o2 and c3 > o3 and c3 > (o2 + c2) / 2:
            signals.append((i, "Piercing Line"))
        elif c2 > o2 and c3 < o3 and c3 < (o2 + c2) / 2:
            signals.append((i, "Dark Cloud Cover"))

        # === PATRONES DE TRES VELAS ===
        if (
            c1 < o1
            and abs(c2 - o2) < total_range * 0.2
            and c3 > o3
            and c3 > (o1 + c1) / 2
        ):
            signals.append((i, "Morning Star"))
        elif (
            c1 > o1
            and abs(c2 - o2) < total_range * 0.2
            and c3 < o3
            and c3 < (o1 + c1) / 2
        ):
            signals.append((i, "Evening Star"))
        if c1 < o1 and c2 < o2 and c3 < o3:
            signals.append((i, "Three Black Crows"))
        if c1 > o1 and c2 > o2 and c3 > o3:
            signals.append((i, "Three White Soldiers"))
        if h3 < h2 and l3 > l2:
            signals.append((i, "Inside Bar"))
        if h3 > h2 and l3 < l2:
            signals.append((i, "Outside Bar"))

    return signals


# === ALERTAS POR TELEGRAM ===
def send_alerts(signals, df):
    for i, pattern in signals:
        ts = df.iloc[i]["timestamp"]
        price = df.iloc[i]["close"]
        msg = f"📍 Patrón detectado: *{pattern}*\n🕒 Vela: {ts}\n💰 Precio: {price:.2f}"
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode="Markdown")


# === FLUJO PRINCIPAL ===
def main():
    df = get_binance_ohlcv()
    signals = detect_patterns(df)
    if signals:
        send_alerts(signals, df)
    else:
        print("No se detectaron patrones.")


if __name__ == "__main__":
    main()
