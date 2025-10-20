# common_utils.py

import pandas as pd
import requests
import ta
import logging
import os
import sys
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv

# === NUEVA FUNCIÓN PARA PRECIO ACTUAL ===
def get_current_price_ticker(symbol):
    """Obtiene el precio actual de un símbolo usando el endpoint del ticker."""
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return float(data['price'])
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener el ticker de precio para {symbol}: {e}")
    except (KeyError, ValueError) as e:
        logging.error(f"Error al procesar la respuesta del ticker para {symbol}: {e}")
    return None

# === FUNCIONES DE BINANCE ===
def get_klines(symbol, interval, limit):
    logging.info(f"Obteniendo datos de velas para {symbol}...")
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos de Binance: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])

    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)

    # --- LÓGICA MEJORADA: AÑADIR VELA EN PROGRESO ---
    # 1. Obtener el precio actual para la vela en curso
    current_price = get_current_price_ticker(symbol)
    if current_price and not df.empty:
        last_closed_candle = df.iloc[-1].copy()
        
        # 2. Crear una nueva fila para la vela actual
        # El 'open' es el 'close' de la vela anterior.
        # El 'timestamp' es el de la vela anterior más el intervalo.
        new_candle = {
            'timestamp': last_closed_candle['close_time'] + 1,
            'open': last_closed_candle['close'],
            'high': max(last_closed_candle['close'], current_price),
            'low': min(last_closed_candle['close'], current_price),
            'close': current_price,
            'volume': 0 # El volumen en tiempo real no es fácil de obtener, lo dejamos en 0
        }
        df = pd.concat([df, pd.DataFrame([new_candle])], ignore_index=True)
        logging.info(f"Vela en progreso añadida con precio actual: {current_price}")

    return df

# === FUNCIONES DE INDICADORES ===
def calculate_indicators(df, volume_sma_period, atr_window, boll_window):
    logging.info("Calculando indicadores técnicos...")

    df['EMA_50'] = ta.trend.ema_indicator(df['close'], window=50)
    df['EMA_200'] = ta.trend.ema_indicator(df['close'], window=200)
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)

    macd_indicator = ta.trend.MACD(df['close'])
    df['MACD'] = macd_indicator.macd()
    df['MACD_signal'] = macd_indicator.macd_signal()

    df['volume_sma'] = ta.trend.sma_indicator(df['volume'], window=volume_sma_period)

    df['ATR'] = ta.volatility.average_true_range(
        high=df['high'], low=df['low'], close=df['close'], window=atr_window)
    df['Boll_Middle'] = ta.volatility.bollinger_mavg(df['close'], window=boll_window)
    df['Boll_Upper'] = ta.volatility.bollinger_hband(df['close'], window=boll_window)
    df['Boll_Lower'] = ta.volatility.bollinger_lband(df['close'], window=boll_window)

    logging.info("Indicadores calculados: EMAs, RSI, MACD, Volumen SMA, ATR, Bollinger Bands.")
    return df

# === PATRONES DE VELAS ===
def is_hammer(open_price, close_price, high, low, body_multiplier=2.0):
    body = abs(close_price - open_price)
    if body == 0: return False
    lower_shadow = min(open_price, close_price) - low
    upper_shadow = high - max(open_price, close_price)
    return lower_shadow > (body * body_multiplier) and upper_shadow < body

def is_shooting_star(open_price, close_price, high, low, body_multiplier=2.0):
    body = abs(close_price - open_price)
    if body == 0: return False
    upper_shadow = high - max(open_price, close_price)
    lower_shadow = min(open_price, close_price) - low
    return upper_shadow > body * body_multiplier and lower_shadow < body

# === ESTADO ===
STATE_FILE = "state.json"

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return None

def clear_state():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

# === FUNCIONES POC ===
def check_poc_zone(latest, poc, tolerance=0.005):
    if poc <= 0:
        return False
    poc_upper = poc * (1 + tolerance)
    poc_lower = poc * (1 - tolerance)
    return latest['high'] >= poc_lower and latest['low'] <= poc_upper

# === TELEGRAM ===
def escape_markdown_v2(text):
    text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + char if char in escape_chars else char for char in text])

def send_telegram_message(message, telegram_token, chat_id, pre_escaped=False):
    if not telegram_token or not chat_id:
        logging.warning("Token o chat_id no configurados.")
        return
    
    text_to_send = message if pre_escaped else escape_markdown_v2(message)
    url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': text_to_send, 'parse_mode': 'MarkdownV2'}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        logging.error(f"Error al enviar mensaje Telegram: {e}")

# === REGISTRO DE TRADES ===
def ensure_trades_log_exists(file_path):
    """Asegura que el archivo de log de trades exista y tenga encabezado."""
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            # Encabezado extendido para simulación de backtest
            f.write('timestamp,symbol,type,entry,stop_loss,take_profit,atr,rr,notes,outcome,pnl_percentage,exit_price,exit_time\n')

def record_trade(file_path, symbol, ttype, entry, stop_loss, take_profit, atr, rr, notes='', timestamp=None):
    """Registra una operación en el archivo CSV."""
    ensure_trades_log_exists(file_path)
    # Usar el timestamp de la vela del backtest si se proporciona, si no, el actual.
    ts = timestamp if timestamp is not None else datetime.utcnow().isoformat()
    # Dejamos las columnas de resultado vacías por ahora
    line = f'{ts},{symbol},{ttype},{entry},{stop_loss},{take_profit},{atr},{rr},{notes}\n'
    with open(file_path, 'a') as f:
        f.write(line)
    logging.info(f"Trade registrado en {file_path}: {symbol} {ttype} entry={entry} rr={rr}")

# === CONFIGURACIÓN ===
def load_config():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Bot de trading para criptomonedas.")
    
    # Parámetros generales
    parser.add_argument("--symbol", type=str, default=os.getenv('SYMBOL', "BTCUSDT"))
    parser.add_argument("--interval", type=str, default=os.getenv('INTERVAL', "1h"))
    parser.add_argument("--limit", type=int, default=int(os.getenv('LIMIT', 250)))
    parser.add_argument("--log", default=os.getenv('LOG_LEVEL', "INFO"), choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_argument("--sleep", type=int, default=int(os.getenv('SLEEP', 3600)))
    
    # Parámetros de indicadores y patrones
    parser.add_argument("--volume-sma-period", type=int, default=int(os.getenv('VOLUME_SMA_PERIOD', 20)))
    parser.add_argument("--hammer-multiplier", type=float, default=float(os.getenv('HAMMER_MULTIPLIER', 2.0)))
    parser.add_argument("--shooting-star-multiplier", type=float, default=float(os.getenv('SHOOTING_STAR_MULTIPLIER', 2.0)))
    parser.add_argument("--volume-multiplier", type=float, default=float(os.getenv('VOLUME_MULTIPLIER', 1.5)))
    parser.add_argument("--atr-window", type=int, default=int(os.getenv('ATR_WINDOW', 14)))
    parser.add_argument("--bollinger-window", type=int, default=int(os.getenv('BOLLINGER_WINDOW', 20)))
    parser.add_argument("--poc", type=float, default=float(os.getenv('POC', 0.0)))
    
    # Parámetros de gestión de riesgo y Wyckoff
    parser.add_argument("--risk-stop-mult", type=float, default=float(os.getenv('RISK_STOP_MULT', 1.5)))
    parser.add_argument("--risk-tp-mult", type=float, default=float(os.getenv('RISK_TP_MULT', 2.5)))
    parser.add_argument("--trades-log-file", type=str, default=os.getenv('TRADES_LOG_FILE', "trades_log.csv"))
    parser.add_argument("--wyckoff", action='store_true', default=str(os.getenv('WYCKOFF', 'false')).lower() in ('true', '1', 't'))
    parser.add_argument("--wyckoff-volume-mult", type=float, default=float(os.getenv('WYCKOFF_VOLUME_MULT', 1.3)))
    parser.add_argument("--wyckoff-atr-thresh", type=float, default=float(os.getenv('WYCKOFF_ATR_THRESH', 1.1)))

    # Parámetros de gestión de riesgo para trading-v6.py
    parser.add_argument("--rr-ratio", type=float, default=float(os.getenv('RR_RATIO', 2.0)), help="Ratio Riesgo/Beneficio para Take Profit (usado en v6)")
    parser.add_argument("--sl-buffer", type=float, default=float(os.getenv('SL_BUFFER', 0.002)), help="Buffer porcentual para Stop Loss (usado en v6)")

    # Parámetros de Backtesting
    parser.add_argument("--backtest", action='store_true', default=str(os.getenv('BACKTEST', 'false')).lower() in ('true', '1', 't'))
    parser.add_argument("--backtest-file", type=str, default=os.getenv('BACKTEST_FILE', "historical_data.csv"))

    args = parser.parse_args()
    telegram_token = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()

    return args, telegram_token, chat_id