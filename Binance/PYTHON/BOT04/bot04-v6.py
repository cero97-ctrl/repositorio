import os
import asyncio
import aiohttp
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
bot = Bot(token=TELEGRAM_TOKEN)


# === CARGAR VELAS DE BINANCE ===
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


# === DETECTAR PATRONES ===
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

        if body < range_ * 0.3 and lower > body * 2:
            signals.append((i, "Hammer"))
        elif body < range_ * 0.3 and upper > body * 2:
            signals.append((i, "Shooting Star"))
        elif body < range_ * 0.1:
            signals.append((i, "Doji"))
        elif body < range_ * 0.3 and lower > body * 2 and c3 < o3:
            signals.append((i, "Hanging Man"))
        if c2 < o2 and c3 > o3 and c3 > o2 and o3 < c2:
            signals.append((i, "Bullish Engulfing"))
        elif c2 > o2 and c3 < o3 and c3 < o2 and o3 > c2:
            signals.append((i, "Bearish Engulfing"))
        elif c2 < o2 and c3 > o3 and c3 > (o2 + c2) / 2:
            signals.append((i, "Piercing Line"))
        elif c2 > o2 and c3 < o3 and c3 < (o2 + c2) / 2:
            signals.append((i, "Dark Cloud Cover"))
        if c1 < o1 and abs(c2 - o2) < range_ * 0.2 and c3 > o3 and c3 > (o1 + c1) / 2:
            signals.append((i, "Morning Star"))
        elif c1 > o1 and abs(c2 - o2) < range_ * 0.2 and c3 < o3 and c3 < (o1 + c1) / 2:
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


# === EVALUAR CONTEXTO ===
def evaluate_context(df, signals):
    avg_volume = df["volume"].mean()
    highs = df["high"].rolling(window=10).max()
    lows = df["low"].rolling(window=10).min()

    table = []
    for i, pattern in signals:
        row = df.iloc[i]
        ts = row["timestamp"]
        price = row["close"]
        vol = row["volume"]
        vol_alto = vol > avg_volume
        soporte = abs(row["low"] - lows[i]) < (row["high"] - row["low"]) * 0.2
        resistencia = abs(row["high"] - highs[i]) < (row["high"] - row["low"]) * 0.2
        prev = df.iloc[i - 3 : i]
        tendencia = "lateral"
        if all(prev["close"].diff().dropna() > 0):
            tendencia = "alcista"
        elif all(prev["close"].diff().dropna() < 0):
            tendencia = "bajista"

        if (
            pattern
            in {
                "Hammer",
                "Bullish Engulfing",
                "Morning Star",
                "Piercing Line",
                "Three White Soldiers",
            }
            and soporte
            and vol_alto
            and tendencia == "bajista"
        ):
            recomendacion = "✅ Compra"
        elif (
            pattern
            in {
                "Shooting Star",
                "Bearish Engulfing",
                "Evening Star",
                "Dark Cloud Cover",
                "Three Black Crows",
            }
            and resistencia
            and vol_alto
            and tendencia == "alcista"
        ):
            recomendacion = "🔻 Venta"
        else:
            recomendacion = "⚠️ No clara"

        table.append(
            {
                "Fecha": ts.strftime("%Y-%m-%d %H:%M"),
                "Patrón": pattern,
                "Precio": f"{price:.2f}",
                "Volumen alto": "Sí" if vol_alto else "No",
                "Soporte": "Sí" if soporte else "No",
                "Resistencia": "Sí" if resistencia else "No",
                "Tendencia previa": tendencia,
                "Recomendación": recomendacion,
            }
        )
    return table


# === ENVIAR TABLA POR TELEGRAM ===
async def send_table(table):
    if not table:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text="📭 No se detectaron patrones."
        )
        return

    msg = f"📊 *Patrones detectados en {LIMIT} velas de {SYMBOL} ({INTERVAL}):*\n\n"
    msg += "Fecha | Patrón | Precio | Vol | Sup | Res | Tendencia | Recomendación\n"
    msg += "------|--------|--------|-----|-----|-----|-----------|----------------\n"
    for row in table:
        msg += f"{row['Fecha']} | {row['Patrón']} | {row['Precio']} | {row['Volumen alto']} | {row['Soporte']} | {row['Resistencia']} | {row['Tendencia previa']} | {row['Recomendación']}\n"

    chunks = [msg[i : i + 4000] for i in range(0, len(msg), 4000)]
    for chunk in chunks:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=chunk, parse_mode="Markdown"
        )


# === EXPORTAR A CSV ===
def export_to_csv(table):
    filename = f"patrones_{SYMBOL}_{INTERVAL}.csv"
    df = pd.DataFrame(table)
    df.to_csv(filename, index=False)


# === FLUJO PRINCIPAL ===
async def main():
    df = await get_binance_ohlcv()
    signals = detect_patterns(df)
    table = evaluate_context(df, signals)
    export_to_csv(table)
    await send_table(table)


if __name__ == "__main__":
    asyncio.run(main())
