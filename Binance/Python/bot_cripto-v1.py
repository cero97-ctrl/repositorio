import pandas as pd
import requests
import ta
import os
import argparse
from dotenv import load_dotenv

# === CARGAR VARIABLES DE ENTORNO ===
load_dotenv()
telegram_token = os.getenv('TELEGRAM_TOKEN')
chat_id = os.getenv('TELEGRAM_CHAT_ID')

# === ARGUMENTOS DE LÍNEA DE COMANDOS ===
parser = argparse.ArgumentParser(description="Bot de señales para criptomonedas")
parser.add_argument('--symbol', type=str, default='BTCUSDT', help='Par de criptomonedas (ej. ETHUSDT, BNBUSDT)')
parser.add_argument('--interval', type=str, default='1h', help='Intervalo de tiempo (ej. 15m, 1h, 4h, 1d)')
args = parser.parse_args()
symbol = args.symbol.upper()
interval = args.interval

# === CONFIGURACIÓN DE MERCADO ===
limit = 200

# === FUNCIONES DE BINANCE ===
def get_klines(symbol, interval, limit):
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    return df

# === INDICADORES TÉCNICOS ===
def calculate_indicators(df):
    df['EMA_50'] = ta.trend.ema_indicator(df['close'], window=50)
    df['EMA_200'] = ta.trend.ema_indicator(df['close'], window=200)
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    macd = ta.trend.macd(df['close'])
    df['MACD'] = macd.macd()
    df['MACD_signal'] = macd.macd_signal()
    return df

# === PATRONES DE VELAS ===
def is_hammer(open, close, high, low):
    body = abs(close - open)
    lower_shadow = min(open, close) - low
    upper_shadow = high - max(open, close)
    return lower_shadow > body * 2 and upper_shadow < body

def is_bullish_engulfing(prev_open, prev_close, curr_open, curr_close):
    return prev_close < prev_open and curr_close > curr_open and curr_open < prev_close and curr_close > prev_open

def is_doji(open, close, high, low):
    body = abs(close - open)
    range_ = high - low
    return body < range_ * 0.1

def is_shooting_star(open, close, high, low):
    body = abs(close - open)
    upper_shadow = high - max(open, close)
    lower_shadow = min(open, close) - low
    return upper_shadow > body * 2 and lower_shadow < body

def is_bearish_engulfing(prev_open, prev_close, curr_open, curr_close):
    return prev_close > prev_open and curr_close < curr_open and curr_open > prev_close and curr_close < prev_open

# === EVALUACIÓN DE SEÑALES ===
def evaluate_trade(df):
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    long_conditions = [
        latest['EMA_50'] > latest['EMA_200'],
        latest['RSI'] < 30 and latest['RSI'] > previous['RSI'],
        latest['MACD'] > latest['MACD_signal'],
        latest['volume'] > previous['volume']
    ]

    short_conditions = [
        latest['EMA_50'] < latest['EMA_200'],
        latest['RSI'] > 70 and latest['RSI'] < previous['RSI'],
        latest['MACD'] < latest['MACD_signal'],
        latest['volume'] > previous['volume']
    ]

    hammer = is_hammer(latest['open'], latest['close'], latest['high'], latest['low'])
    engulfing_bull = is_bullish_engulfing(previous['open'], previous['close'], latest['open'], latest['close'])
    doji_low = is_doji(latest['open'], latest['close'], latest['high'], latest['low']) and latest['RSI'] < 30

    shooting_star = is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'])
    engulfing_bear = is_bearish_engulfing(previous['open'], previous['close'], latest['open'], latest['close'])
    doji_high = is_doji(latest['open'], latest['close'], latest['high'], latest['low']) and latest['RSI'] > 70

    if all(long_conditions):
        return "📈 Señal técnica de entrada en LARGO (compra)"
    elif all(short_conditions):
        return "📉 Señal técnica de entrada en CORTO (venta)"
    elif hammer:
        return "🕯️ Vela tipo MARTILLO detectada: posible reversión alcista"
    elif engulfing_bull:
        return "🕯️ Vela ENVOLVENTE ALCISTA detectada: posible entrada en largo"
    elif doji_low:
        return "🕯️ DOJI en zona de sobreventa: posible rebote"
    elif shooting_star:
        return "🕯️ Vela tipo ESTRELLA FUGAZ detectada: posible reversión bajista"
    elif engulfing_bear:
        return "🕯️ Vela ENVOLVENTE BAJISTA detectada: posible entrada en corto"
    elif doji_high:
        return "🕯️ DOJI en zona de sobrecompra: posible caída inminente"
    else:
        return "⏳ No hay señal clara de entrada en este momento"

# === ALERTA TELEGRAM ===
def send_telegram_message(message):
    url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}
    requests.post(url, data=payload)

# === EJECUCIÓN PRINCIPAL ===
def run_bot():
    df = get_klines(symbol, interval, limit)
    df = calculate_indicators(df)
    signal = evaluate_trade(df)
    send_telegram_message(f"[{symbol} - {interval}] {signal}")

# === INICIO ===
if __name__ == "__main__":
    run_bot()

