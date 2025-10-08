import pandas as pd
import requests
import ta
import time

# === CONFIGURACIÓN ===
symbol = 'BTCUSDT'
interval = '1h'
limit = 200
telegram_token = 'TU_TOKEN_AQUI'
chat_id = 'TU_CHAT_ID_AQUI'

# === FUNCIONES ===
def get_klines(symbol, interval, limit):
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    return df

def calculate_indicators(df):
    df['EMA_50'] = ta.trend.ema_indicator(df['close'], window=50)
    df['EMA_200'] = ta.trend.ema_indicator(df['close'], window=200)
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    macd = ta.trend.macd(df['close'])
    df['MACD'] = macd.macd()
    df['MACD_signal'] = macd.macd_signal()
    return df

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

    if all(long_conditions):
        return "📈 Señal de entrada en LARGO (compra)"
    elif all(short_conditions):
        return "📉 Señal de entrada en CORTO (venta)"
    else:
        return "⏳ No hay señal clara de entrada en este momento"

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

# === LOOP (opcional para ejecución continua) ===
if __name__ == "__main__":
    run_bot()
    # Para ejecución continua cada hora:
    # while True:
    #     run_bot()
    #     time.sleep(3600)  # Espera 1 hora

