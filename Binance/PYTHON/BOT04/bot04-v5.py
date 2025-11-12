import os
import asyncio
import aiohttp
import pandas as pd
from dotenv import load_dotenv
from telegram import Bot
from collections import Counter

# === CARGAR VARIABLES ===
load_dotenv()
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL = os.getenv("INTERVAL", "1h")
LIMIT = int(os.getenv("LIMIT", 100))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
bot = Bot(token=TELEGRAM_TOKEN)


# === CARGAR VELAS DE BINANCE (asíncrono) ===
async def get_binance_ohlcv():
    url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
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


# === DETECTAR PATRONES (sincrónico) ===
def detect_patterns(df):
    signals = []
    for i in range(2, len(df)):
        o1, h1, l1, c1 = df.iloc[i - 2][["open", "high", "low", "close"]]
        o2, h2, l2, c2 = df.iloc[i - 1][["open", "high", "low", "close"]]
        o3, h3, l3, c3 = df.iloc[i][["open", "high", "low", "close"]]

        body = abs(c3 - o3)
        upper = h3 - max(c3, o3)
        lower = min(c3, o3) - l3
        range_ = h3 - l3

        # === PATRONES DE UNA VELA ===
        if body < range_ * 0.3 and lower > body * 2:
            signals.append("Hammer")
        elif body < range_ * 0.3 and upper > body * 2:
            signals.append("Shooting Star")
        elif body < range_ * 0.1:
            signals.append("Doji")
        elif body < range_ * 0.3 and lower > body * 2 and c3 < o3:
            signals.append("Hanging Man")

        # === PATRONES DE DOS VELAS ===
        if c2 < o2 and c3 > o3 and c3 > o2 and o3 < c2:
            signals.append("Bullish Engulfing")
        elif c2 > o2 and c3 < o3 and c3 < o2 and o3 > c2:
            signals.append("Bearish Engulfing")
        elif c2 < o2 and c3 > o3 and c3 > (o2 + c2) / 2:
            signals.append("Piercing Line")
        elif c2 > o2 and c3 < o3 and c3 < (o2 + c2) / 2:
            signals.append("Dark Cloud Cover")

        # === PATRONES DE TRES VELAS ===
        if c1 < o1 and abs(c2 - o2) < range_ * 0.2 and c3 > o3 and c3 > (o1 + c1) / 2:
            signals.append("Morning Star")
        elif c1 > o1 and abs(c2 - o2) < range_ * 0.2 and c3 < o3 and c3 < (o1 + c1) / 2:
            signals.append("Evening Star")
        if c1 < o1 and c2 < o2 and c3 < o3:
            signals.append("Three Black Crows")
        if c1 > o1 and c2 > o2 and c3 > o3:
            signals.append("Three White Soldiers")
        if h3 < h2 and l3 > l2:
            signals.append("Inside Bar")
        if h3 > h2 and l3 < l2:
            signals.append("Outside Bar")

    return Counter(signals)


# === ENVIAR ALERTA POR TELEGRAM (agrupado) ===
async def send_summary(counter):
    if not counter:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text="📭 No se detectaron patrones."
        )
        return

    reversals = {
        "Hammer",
        "Hanging Man",
        "Shooting Star",
        "Doji",
        "Bullish Engulfing",
        "Bearish Engulfing",
        "Piercing Line",
        "Dark Cloud Cover",
        "Morning Star",
        "Evening Star",
    }
    continuations = {
        "Three White Soldiers",
        "Three Black Crows",
        "Inside Bar",
        "Outside Bar",
    }

    msg = f"📊 *Resumen de patrones en {LIMIT} velas de {SYMBOL} ({INTERVAL}):*\n\n"
    msg += "🔄 *Patrones de Reversión:*\n"
    for pattern in sorted(reversals):
        if pattern in counter:
            msg += f"• {pattern}: {counter[pattern]} veces\n"

    msg += "\n📈 *Patrones de Continuación:*\n"
    for pattern in sorted(continuations):
        if pattern in counter:
            msg += f"• {pattern}: {counter[pattern]} veces\n"

    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode="Markdown")


# === FLUJO PRINCIPAL ===
async def main():
    df = await get_binance_ohlcv()
    counter = detect_patterns(df)
    await send_summary(counter)


if __name__ == "__main__":
    asyncio.run(main())
