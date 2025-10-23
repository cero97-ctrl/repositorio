# common_utils.py

import pandas as pd
import numpy as np
import requests
import ta
import logging
import time
import os
import sys
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import sys

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

# --- NUEVA FUNCIÓN: Convertir intervalo a milisegundos ---
def interval_to_ms(interval_str):
    unit = interval_str[-1]
    value = int(interval_str[:-1])
    if unit == 'm': return value * 60 * 1000
    if unit == 'h': return value * 60 * 60 * 1000
    if unit == 'd': return value * 24 * 60 * 60 * 1000
    logging.error(f"Intervalo desconocido: {interval_str}")
    return 0 # Debería manejar todos los casos válidos

# === FUNCIONES DE BINANCE ===
def get_klines(symbol, interval, limit=None, start_time_ms=None, end_time_ms=None):
    logging.info(f"Obteniendo datos de velas para {symbol} en intervalo {interval}...")
    base_url = 'https://api.binance.com/api/v3/klines'
    all_klines = []

    if start_time_ms is not None and end_time_ms is not None:
        # --- LÓGICA PARA OBTENER DATOS POR RANGO DE FECHAS (CON PAGINACIÓN) ---
        logging.info(f"Obteniendo datos desde {datetime.fromtimestamp(start_time_ms/1000)} hasta {datetime.fromtimestamp(end_time_ms/1000)}")
        current_start_time = start_time_ms
        while current_start_time < end_time_ms:
            params = {
                'symbol': symbol, 'interval': interval, 'limit': 1000,
                'startTime': current_start_time, 'endTime': end_time_ms
            }
            try:
                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                if not data: break
                all_klines.extend(data)
                current_start_time = data[-1][0] + interval_to_ms(interval)
                time.sleep(0.1)
            except requests.exceptions.RequestException as e:
                logging.error(f"Error en paginación de Binance: {e}")
                break
        
        if all_klines:
            temp_df = pd.DataFrame(all_klines)
            temp_df.drop_duplicates(subset=[0], inplace=True)
            temp_df.sort_values(by=[0], inplace=True)
            all_klines = temp_df.values.tolist()

    elif limit is not None:
        # --- LÓGICA ORIGINAL PARA OBTENER DATOS POR LÍMITE ---
        logging.info(f"Obteniendo las últimas {limit} velas.")
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            all_klines = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error al obtener datos de Binance por límite: {e}")
            return pd.DataFrame()
    else:
        logging.error("Debe proporcionar 'limit' o un rango de fechas ('start_time_ms' y 'end_time_ms').")
        return pd.DataFrame()

    if not all_klines:
        logging.warning("No se descargaron datos de Binance.")
        return pd.DataFrame()

    df = pd.DataFrame(all_klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])

    # --- Limpieza y conversión de datos ---
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)

    # La lógica de la "vela en progreso" se ha movido a dashboard.py para que sea condicional.
    # Esta función ahora solo se encarga de descargar los datos.

    return df

# === FUNCIONES DE INDICADORES ===
def calculate_indicators(df, volume_sma_period, atr_window, boll_window):
    logging.info("Calculando indicadores técnicos...")
    
    # --- CORRECCIÓN CRÍTICA PARA EVITAR SettingWithCopyWarning ---
    # Creamos una copia explícita del DataFrame para asegurar que todos los
    # indicadores se calculen y añadan correctamente sin ambigüedades.
    df = df.copy()

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

# === GESTIÓN DE RIESGO ===
def compute_risk_levels(entry_price, atr, direction='long', stop_mult=1.5, tp_mult=2.5, sl_buffer=0.0):
    """
    Calcula stop_loss, take_profit y risk:reward (rr) dependiendo de la dirección.
    Retorna (stop_loss, take_profit, rr).
    """
    if pd.isna(atr) or atr <= 0:
        return None, None, None, None # Añadido None para entry_price

    # Ajustar entry_price para el cálculo del riesgo si se usa sl_buffer
    # Esto es más relevante si el SL se basa en un punto fijo (ej. low de la vela)
    # y el entry_price es el close.
    
    if direction == 'long':
        # stop_loss = entry_price - atr * stop_mult # Basado en ATR
        stop_loss = entry_price * (1 - sl_buffer) # Basado en porcentaje del precio de entrada
        risk = entry_price - stop_loss
        take_profit = entry_price + (risk * tp_mult)
    else:  # short
        # stop_loss = entry_price + atr * stop_mult # Basado en ATR
        stop_loss = entry_price * (1 + sl_buffer) # Basado en porcentaje del precio de entrada
        risk = stop_loss - entry_price
        take_profit = entry_price - (risk * tp_mult)

    if risk <= 0:
        return None, None, None, None

    rr = round((take_profit - entry_price) / risk, 2) if direction == 'long' else round((entry_price - take_profit) / risk, 2)
    
    return entry_price, round(stop_loss, 8), round(take_profit, 8), rr

def format_risk_management_message(signal_text, entry_price, stop_loss, take_profit, rr_ratio):
    """Formatea un mensaje de gestión de riesgo para Telegram."""
    if entry_price is None or stop_loss is None or take_profit is None or rr_ratio is None:
        return f"{signal_text}\n\n⚠️ No se pudo calcular la gestión de riesgo (riesgo inválido)."

    return (f"{signal_text}\n\n"
            f"🎯 *Gestión de Riesgo (R:R 1:{rr_ratio})*\n"
            f"- Entrada: `${entry_price:.8f}`\n"
            f"- Stop Loss: `${stop_loss:.8f}`\n"
            f"- Take Profit: `${take_profit:.8f}`")

def get_atr_mean_for_volatility(df, window=50):
    """Calcula la media del ATR para determinar la volatilidad."""
    if 'ATR' not in df.columns or len(df) < window:
        return np.nan
    atr_mean = df['ATR'].rolling(window=window, min_periods=1).mean().iloc[-1]
    return atr_mean


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

def is_bullish_engulfing(latest, previous):
    """Detecta un patrón envolvente alcista."""
    # La vela anterior debe ser bajista y la actual alcista.
    if previous['close'] >= previous['open'] or latest['close'] <= latest['open']:
        return False
    # El cuerpo de la vela actual debe envolver el cuerpo de la anterior.
    return latest['open'] < previous['close'] and latest['close'] > previous['open']

def is_bearish_engulfing(latest, previous):
    """Detecta un patrón envolvente bajista."""
    if previous['close'] <= previous['open'] or latest['close'] >= latest['open']:
        return False
    return latest['open'] > previous['close'] and latest['close'] < previous['open']

def is_three_white_soldiers(df_slice, body_ratio_threshold=0.3):
    """
    Detecta el patrón 'Three White Soldiers' con una lógica más flexible.
    Busca 3 velas alcistas consecutivas con cierres crecientes y cuerpos decentes.
    """
    if len(df_slice) < 3:
        return False
    c1, c2, c3 = df_slice.iloc[-3], df_slice.iloc[-2], df_slice.iloc[-1]
    
    # 1. Las 3 velas deben ser alcistas
    are_all_bullish = (c1['close'] > c1['open']) and (c2['close'] > c2['open']) and (c3['close'] > c3['open'])
    # 2. Cada cierre debe ser más alto que el anterior
    are_closes_higher = (c2['close'] > c1['close']) and (c3['close'] > c2['close'])
    # 3. El cuerpo de cada vela debe ser de un tamaño razonable (evitar Dojis)
    c1_body_ok = (c1['close'] - c1['open']) > ((c1['high'] - c1['low']) * body_ratio_threshold)
    c2_body_ok = (c2['close'] - c2['open']) > ((c2['high'] - c2['low']) * body_ratio_threshold)
    c3_body_ok = (c3['close'] - c3['open']) > ((c3['high'] - c3['low']) * body_ratio_threshold)

    return are_all_bullish and are_closes_higher and c1_body_ok and c2_body_ok and c3_body_ok

def is_three_black_crows(df_slice, body_ratio_threshold=0.3):
    """
    Detecta el patrón 'Three Black Crows' con una lógica más flexible.
    Busca 3 velas bajistas consecutivas con cierres decrecientes y cuerpos decentes.
    """
    if len(df_slice) < 3:
        return False
    c1, c2, c3 = df_slice.iloc[-3], df_slice.iloc[-2], df_slice.iloc[-1]
    
    are_all_bearish = (c1['close'] < c1['open']) and (c2['close'] < c2['open']) and (c3['close'] < c3['open'])
    are_closes_lower = (c2['close'] < c1['close']) and (c3['close'] < c2['close'])
    c1_body_ok = (c1['open'] - c1['close']) > ((c1['high'] - c1['low']) * body_ratio_threshold)
    c2_body_ok = (c2['open'] - c2['close']) > ((c2['high'] - c2['low']) * body_ratio_threshold)
    c3_body_ok = (c3['open'] - c3['close']) > ((c3['high'] - c3['low']) * body_ratio_threshold)

    return are_all_bearish and are_closes_lower and c1_body_ok and c2_body_ok and c3_body_ok

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

# === FUNCIONES COMUNES PARA BOTS ===
def execute_single_run_common(args, telegram_token, chat_id, evaluate_trade_func, check_confirmation_func):
    """
    Ejecuta un único ciclo de análisis para un bot de trading.
    :param args: Argumentos de configuración del bot.
    :param telegram_token: Token de Telegram.
    :param chat_id: ID del chat de Telegram.
    :param evaluate_trade_func: Función específica del bot para evaluar señales.
    :param check_confirmation_func: Función específica del bot para verificar confirmaciones.
    """
    logging.info(f"Analizando {args.symbol} {args.interval}...")
    df = get_klines(args.symbol, args.interval, args.limit)
    if df.empty or len(df) < 2:
        logging.warning("Datos insuficientes.")
        return

    df = calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)

    pending_state = load_state()
    if pending_state:
        signal_message = check_confirmation_func(df, pending_state, args)
    else:
        signal_message = evaluate_trade_func(df, args)

    message = f"--- Análisis para {args.symbol} ({args.interval}) ---\n\n{signal_message}"
    logging.info(message)

    # Solo enviar a Telegram si hay una señal activa y no es solo un mensaje de espera o baja volatilidad
    if "⏳" not in signal_message and "Volatilidad baja" not in signal_message and "❌ Sin confirmación" not in signal_message:
        send_telegram_message(message, telegram_token, chat_id, pre_escaped=True)

def run_bot_main_loop(args, telegram_token, chat_id, evaluate_trade_func, check_confirmation_func, startup_message_text):
    """
    Bucle principal para la ejecución de un bot de trading.
    :param args: Argumentos de configuración del bot.
    :param telegram_token: Token de Telegram.
    :param chat_id: ID del chat de Telegram.
    :param evaluate_trade_func: Función específica del bot para evaluar señales.
    :param check_confirmation_func: Función específica del bot para verificar confirmaciones.
    :param startup_message_text: Mensaje de inicio para Telegram.
    """
    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    send_telegram_message(startup_message_text, telegram_token, chat_id, pre_escaped=True)
    logging.info("Mensaje de inicio enviado a Telegram.")

    while True:
        try:
            execute_single_run_common(args, telegram_token, chat_id, evaluate_trade_func, check_confirmation_func)
            logging.info(f"Análisis completado. Esperando {args.sleep} segundos para el próximo ciclo.")
            time.sleep(args.sleep)
        except KeyboardInterrupt:
            logging.info("Bot detenido manualmente. Limpiando estado...")
            clear_state()
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error inesperado en el ciclo principal: {e}")
            time.sleep(60) # Esperar un minuto antes de reintentar en caso de error grave

def setup_logging_and_config():
    args, telegram_token, chat_id = load_config()
    logging.basicConfig(level=args.log.upper(), format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
    return args, telegram_token, chat_id

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

    # Parámetros para Wyckoff Multi-Timeframe (v1, v2, v3)
    parser.add_argument("--wyckoff-professional", action='store_true', default=str(os.getenv('WYCKOFF_PROFESSIONAL', 'false')).lower() in ('true', '1', 't'))
    parser.add_argument("--htf-interval", type=str, default=os.getenv('HTF_INTERVAL', "4h"))
    parser.add_argument("--htf-limit", type=int, default=int(os.getenv('HTF_LIMIT', 120)))
    parser.add_argument("--htf-vol-mult", type=float, default=float(os.getenv('HTF_VOL_MULT', 1.2)))
    # Añadimos los de v2 y v3 para que no den error en el futuro
    parser.add_argument("--htf-lookback", type=int, default=int(os.getenv('HTF_LOOKBACK', 100)))
    parser.add_argument("--htf-range-thresh", type=float, default=float(os.getenv('HTF_RANGE_THRESH', 0.10)))
    parser.add_argument("--htf-climactic-vol-mult", type=float, default=float(os.getenv('HTF_CLIMACTIC_VOL_MULT', 2.5)))
    parser.add_argument("--htf-test-vol-mult", type=float, default=float(os.getenv('HTF_TEST_VOL_MULT', 0.8)))
    parser.add_argument("--htf-breakout-vol-mult", type=float, default=float(os.getenv('HTF_BREAKOUT_VOL_MULT', 1.5)))

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