import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from telegram import Bot

# === CONFIGURACIÓN ===
TELEGRAM_TOKEN = 'TU_TOKEN_AQUI'
CHAT_ID = 'TU_CHAT_ID_AQUI'
SYMBOL = 'BTCUSDT'
RANGE_LOOKBACK = 50
ATR_PERIOD = 14
ABSORPTION_RATIO = 0.3
REJECTION_THRESHOLD = 0.7

bot = Bot(token=TELEGRAM_TOKEN)

# === FUNCIONES DE ALERTA ===
def send_telegram_alert(message):
    bot.send_message(chat_id=CHAT_ID, text=message)

# === FUNCIONES DE ANÁLISIS ===
def calculate_atr(df, period=ATR_PERIOD):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def detect_range(df):
    recent = df[-RANGE_LOOKBACK:]
    max_high = recent['high'].max()
    min_low = recent['low'].min()
    atr = calculate_atr(recent).iloc[-1]
    range_size = max_high - min_low

    if range_size < atr * 2:
        return True, min_low, max_high
    return False, None, None

def detect_absorption(df, level, direction='support'):
    recent = df[-5:]
    signals = []
    for i, row in recent.iterrows():
        body = abs(row['close'] - row['open'])
        lower_wick = row['open'] - row['low'] if row['close'] > row['open'] else row['close'] - row['low']
        upper_wick = row['high'] - row['close'] if row['close'] > row['open'] else row['high'] - row['open']
        if direction == 'support' and row['low'] <= level and body / (lower_wick + 1e-6) < ABSORPTION_RATIO:
            signals.append((i, 'Absorción en soporte'))
        elif direction == 'resistance' and row['high'] >= level and body / (upper_wick + 1e-6) < ABSORPTION_RATIO:
            signals.append((i, 'Absorción en resistencia'))
    return signals

def detect_rejection(df, level, direction='resistance'):
    recent = df[-5:]
    signals = []
    for i, row in recent.iterrows():
        upper_wick = row['high'] - max(row['open'], row['close'])
        lower_wick = min(row['open'], row['close']) - row['low']
        total_range = row['high'] - row['low']
        if direction == 'resistance' and row['high'] >= level and upper_wick / total_range > REJECTION_THRESHOLD:
            signals.append((i, 'Rechazo en resistencia'))
        elif direction == 'support' and row['low'] <= level and lower_wick / total_range > REJECTION_THRESHOLD:
            signals.append((i, 'Rechazo en soporte'))
    return signals

# === VISUALIZACIÓN ===
def plot_range(df, low, high, signals):
    plt.figure(figsize=(12,6))
    plt.plot(df['close'], label='Precio')
    plt.axhline(low, color='green', linestyle='--', label='Soporte')
    plt.axhline(high, color='red', linestyle='--', label='Resistencia')
    for idx, msg in signals:
        plt.annotate(msg, xy=(idx, df['close'].iloc[idx]), xytext=(idx, df['close'].iloc[idx]+50),
                     arrowprops=dict(facecolor='yellow', shrink=0.05), fontsize=9)
    plt.title(f'Rango detectado en {SYMBOL}')
    plt.legend()
    plt.tight_layout()
    plt.savefig('rango_detectado.png')
    plt.close()

# === EJECUCIÓN DE ÓRDENES (simulada) ===
def execute_trade(direction, price):
    msg = f"🟢 Ejecutando orden {direction.upper()} en {price:.2f}"
    print(msg)
    send_telegram_alert(msg)
    # Aquí iría la lógica real con Binance API

# === FLUJO PRINCIPAL ===
def prepare_alerts(df):
    in_range, low, high = detect_range(df)
    if in_range:
        msg = f"🔔 Rango detectado entre {low:.2f} y {high:.2f}"
        print(msg)
        send_telegram_alert(msg)

        absorptions = detect_absorption(df, low, 'support') + detect_absorption(df, high, 'resistance')
        rejections = detect_rejection(df, high, 'resistance') + detect_rejection(df, low, 'support')
        signals = absorptions + rejections

        for idx, alert in signals:
            msg = f"🧠 Alerta en vela {idx}: {alert}"
            print(msg)
            send_telegram_alert(msg)

        plot_range(df, low, high, signals)

        # Ruptura con volumen
        last = df.iloc[-1]
        avg_volume = df['volume'].rolling(window=20).mean().iloc[-1]
        if last['close'] > high and last['volume'] > avg_volume:
            msg = f"🚀 Ruptura alcista con volumen en {last['close']:.2f}"
            send_telegram_alert(msg)
            execute_trade('long', last['close'])
        elif last['close'] < low and last['volume'] > avg_volume:
            msg = f"⚠️ Ruptura bajista con volumen en {last['close']:.2f}"
            send_telegram_alert(msg)
            execute_trade('short', last['close'])

# === EJEMPLO DE USO ===
# df = pd.read_csv('BTCUSDT_1h.csv')  # o desde tu API
# prepare_alerts(df)

