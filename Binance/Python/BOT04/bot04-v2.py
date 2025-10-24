import os
import requests
import pandas as pd
from dotenv import load_dotenv
from telegram import Bot

# === CARGAR VARIABLES ===
load_dotenv()  # Carga .env
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL = os.getenv("INTERVAL", "1h")
LIMIT = int(os.getenv("LIMIT", 100))

# Cargar desde entorno del sistema (.bashrc)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("❌ TELEGRAM_TOKEN o TELEGRAM_CHAT_ID no están definidos en el entorno.")

bot = Bot(token=TELEGRAM_TOKEN)

# === FUNCIÓN PARA CARGAR VELAS DE BINANCE ===
def get_binance_ohlcv(symbol=SYMBOL, interval=INTERVAL, limit=LIMIT):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    return df[["timestamp", "open", "high", "low", "close", "volume"]]

# === USO DEL BOT ===
def main():
    df = get_binance_ohlcv()
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"✅ Cargadas {len(df)} velas de {SYMBOL} ({INTERVAL})")
    print(df.tail())

if __name__ == "__main__":
    main()

