# trading-v6.py (versión mejorada con ATR, Bollinger, POC, Backtesting y carga completa de parámetros desde dotenv)

import pandas as pd
import requests
import ta
import time
import logging
import os
import sys
import json
import argparse
from dotenv import load_dotenv  # Soporte para dotenv

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

    # === NUEVOS INDICADORES ===
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

# === CONFIRMACIÓN ===
def check_confirmation(df, state, volume_multiplier):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")
    confirmation_volume_ok = latest['volume'] > latest['volume_sma'] * volume_multiplier

    message = ""
    if pattern in ["hammer"] and latest['close'] > previous['high']:
        message = f"✅ Confirmación alcista del patrón {pattern}"
    elif pattern in ["shooting_star"] and latest['close'] < previous['low']:
        message = f"✅ Confirmación bajista del patrón {pattern}"
    else:
        message = f"❌ Sin confirmación para patrón {pattern}"

    clear_state()
    return message

# === FUNCIONES POC ===
def check_poc_zone(latest, poc, tolerance):
    if poc <= 0:
        return False
    poc_upper = poc * (1 + tolerance)
    poc_lower = poc * (1 - tolerance)
    return latest['high'] >= poc_lower and latest['low'] <= poc_upper

# === EVALUACIÓN DE SEÑALES ===
def evaluate_trade(df, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    atr_mean = df['ATR'].rolling(args.atr_mean_period).mean().iloc[-1]
    if latest['ATR'] < atr_mean:
        logging.info("ATR bajo: mercado sin volatilidad significativa, no se generan señales.")
        return "⚠️ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = check_poc_zone(latest, args.poc, args.poc_tolerance)

    # --- Lógica de Gestión de Riesgo ---
    def add_risk_management(signal_text, direction):
        entry_price = latest['close']
        if direction == 'long':
            stop_loss = latest['low'] * (1 - args.sl_buffer) # Un pequeño buffer por debajo del mínimo
            risk = entry_price - stop_loss
            take_profit = entry_price + (risk * args.rr_ratio)
        else: # short
            stop_loss = latest['high'] * (1 + args.sl_buffer) # Un pequeño buffer por encima del máximo
            risk = stop_loss - entry_price
            take_profit = entry_price - (risk * args.rr_ratio)
        
        return f"{signal_text}\n\n🎯 **Gestión de Riesgo (R:R 1:{args.rr_ratio})**\n- Entrada: `${entry_price:.2f}`\n- Stop Loss: `${stop_loss:.2f}`\n- Take Profit: `${take_profit:.2f}`"

    # --- ESTRATEGIA: CRUCE DE EMAs (Golden/Death Cross) ---
    # Golden Cross (Cruce Dorado) -> Señal de Compra
    if previous['EMA_50'] <= previous['EMA_200'] and latest['EMA_50'] > latest['EMA_200']:
        signal_text = "📈 **Golden Cross** detectado (EMA 50 cruza por encima de EMA 200)"
        signals.append(add_risk_management(signal_text, 'long'))

    # Death Cross (Cruce de la Muerte) -> Señal de Venta
    if previous['EMA_50'] >= previous['EMA_200'] and latest['EMA_50'] < latest['EMA_200']:
        signal_text = "📉 **Death Cross** detectado (EMA 50 cruza por debajo de EMA 200)"
        signals.append(add_risk_management(signal_text, 'short'))

    if is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier):
        signal_text = "🕯️ Martillo detectado"
        if latest['close'] <= latest['Boll_Lower']:
            signal_text += " tocando banda inferior de Bollinger 📉"
        if poc_zone:
            signal_text += f" en ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥"
        if latest['ATR'] > atr_mean:
            signal_text += " con alta volatilidad 🔥"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📈"
        signals.append(add_risk_management(signal_text, 'long'))
        pending_state = {"pattern": "hammer", "price": latest['close']}

    if is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier):
        signal_text = "🕯️ Estrella Fugaz detectada"
        if latest['close'] >= latest['Boll_Upper']:
            signal_text += " tocando banda superior de Bollinger 📈"
        if poc_zone:
            signal_text += f" en ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️"
        if latest['ATR'] > atr_mean:
            signal_text += " con fuerte volatilidad ⚡"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📉"
        signals.append(add_risk_management(signal_text, 'short'))
        pending_state = {"pattern": "shooting_star", "price": latest['close']}

    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(latest['timestamp'], unit='ms').isoformat()
        save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    return "\n".join(signals) if signals else "⏳ Sin señales claras."

# === TELEGRAM ===
def escape_markdown_v2(text):
    """Escapa los caracteres especiales para el modo MarkdownV2 de Telegram."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + char if char in escape_chars else char for char in text])

def send_telegram_message(message, telegram_token, chat_id, pre_escaped=False):
    if not telegram_token or not chat_id:
        logging.warning("Token o chat_id no configurados.")
        return

    # Escapamos el mensaje solo si no viene ya formateado
    text_to_send = message if pre_escaped else escape_markdown_v2(message)

    url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': text_to_send, 'parse_mode': 'MarkdownV2'}
    try:
        logging.info("Enviando notificación a Telegram...")
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        logging.error(f"Error al enviar mensaje Telegram: {e}")

# === MODO BACKTESTING ===
def run_backtest(args):
    logging.info(f"Iniciando backtest con datos: {args.backtest_file}")
    try:
        df = pd.read_csv(args.backtest_file)
    except Exception as e:
        logging.error(f"Error al cargar CSV: {e}")
        return

    if not {'open','high','low','close','volume'}.issubset(df.columns):
        logging.error("El CSV debe contener columnas: open, high, low, close, volume")
        return

    df = calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
    total_signals = 0

    for i in range(51, len(df)):
        sub_df = df.iloc[:i+1]
        signal = evaluate_trade(sub_df, args)
        if "⏳" not in signal and "Volatilidad baja" not in signal:
            total_signals += 1
            print(f"[{i}] {signal} @ {sub_df.iloc[-1]['close']}")

    print(f"\n✅ Backtest completado. Señales detectadas: {total_signals}")

# === EJECUCIÓN ===
def execute_single_run(args, telegram_token, chat_id):
    logging.info(f"Analizando {args.symbol} {args.interval}...")
    df = get_klines(args.symbol, args.interval, args.limit)
    if df.empty or len(df) < 2:
        logging.warning("Datos insuficientes.")
        return

    df = calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)

    pending_state = load_state()
    if pending_state:
        signal = check_confirmation(df, pending_state, args.volume_multiplier)
    else:
        signal = evaluate_trade(df, args)

    message = f"--- Análisis para {args.symbol} ({args.interval}) ---\n\n{signal}"
    logging.info(message)

    if "⏳" not in signal and "Volatilidad baja" not in signal:
        send_telegram_message(message, telegram_token, chat_id, pre_escaped=False)

def load_config():
    """Carga la configuración desde .env y argumentos de línea de comandos."""
    load_dotenv()

    parser = argparse.ArgumentParser(description="Bot de trading con ATR, Bollinger, POC, Backtesting y dotenv.")
    
    # La precedencia es: Argumento CLI > Variable de Entorno > Valor por defecto
    parser.add_argument("--symbol", type=str, default=os.getenv('SYMBOL', "BTCUSDT"))
    parser.add_argument("--interval", type=str, default=os.getenv('INTERVAL', "1h"))
    parser.add_argument("--limit", type=int, default=int(os.getenv('LIMIT', 202)))
    parser.add_argument("--log", default=os.getenv('LOG_LEVEL', "INFO"), choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_argument("--sleep", type=int, default=int(os.getenv('SLEEP', 3600)))
    parser.add_argument("--volume-sma-period", type=int, default=int(os.getenv('VOLUME_SMA_PERIOD', 20)))
    parser.add_argument("--hammer-multiplier", type=float, default=float(os.getenv('HAMMER_MULTIPLIER', 2.0)))
    parser.add_argument("--shooting-star-multiplier", type=float, default=float(os.getenv('SHOOTING_STAR_MULTIPLIER', 2.0)))
    parser.add_argument("--volume-multiplier", type=float, default=float(os.getenv('VOLUME_MULTIPLIER', 1.5)))
    parser.add_argument("--atr-window", type=int, default=int(os.getenv('ATR_WINDOW', 14)))
    parser.add_argument("--bollinger-window", type=int, default=int(os.getenv('BOLLINGER_WINDOW', 20)))
    parser.add_argument("--poc", type=float, default=float(os.getenv('POC', 0.0)))
    parser.add_argument("--poc-tolerance", type=float, default=float(os.getenv('POC_TOLERANCE', 0.005)), help="Tolerancia porcentual para la zona POC.")
    parser.add_argument("--atr-mean-period", type=int, default=int(os.getenv('ATR_MEAN_PERIOD', 50)), help="Periodo para la media móvil del ATR.")
    parser.add_argument("--backtest", action='store_true', help="Activa el modo backtesting.")
    parser.add_argument("--rr-ratio", type=float, default=float(os.getenv('RR_RATIO', 2.0)), help="Ratio Riesgo/Beneficio para Take Profit.")
    parser.add_argument("--sl-buffer", type=float, default=float(os.getenv('SL_BUFFER', 0.002)), help="Buffer porcentual para el Stop Loss (ej: 0.002 para 0.2%).")
    parser.add_argument("--backtest-file", type=str, default=os.getenv('BACKTEST_FILE', "historical_data.csv"))

    args = parser.parse_args()

    telegram_token = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()

    return args, telegram_token, chat_id

# === MAIN ===
if __name__ == "__main__":
    args, telegram_token, chat_id = load_config()
    
    # Configurar logging después de cargar la configuración para usar el nivel de log correcto
    logging.basicConfig(
        level=args.log.upper(), 
        format='%(asctime)s - %(levelname)s - %(message)s', 
        stream=sys.stdout
    )

    logging.info("==========================================")
    logging.info("Iniciando bot con la siguiente configuración:")
    logging.info(f"Símbolo: {args.symbol}")
    logging.info(f"Intervalo: {args.interval}")
    logging.info(f"POC: {args.poc}")
    logging.info(f"ATR Window: {args.atr_window}")
    logging.info(f"Bollinger Window: {args.bollinger_window}")
    logging.info(f"Volumen SMA Period: {args.volume_sma_period}")
    logging.info(f"Tolerancia POC: {args.poc_tolerance}")
    logging.info(f"Periodo Media ATR: {args.atr_mean_period}")
    logging.info(f"Ratio Riesgo/Beneficio: {args.rr_ratio}")
    logging.info(f"Buffer de Stop Loss: {args.sl_buffer}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    # Enviar mensaje de inicio a Telegram
    startup_message = (
        f"🚀 *Bot de Trading Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"POC configurado en: `{args.poc}`\n\n"
        "El bot está en línea y funcionando correctamente\\."
    )
    send_telegram_message(startup_message, telegram_token, chat_id, pre_escaped=True)
    logging.info("Mensaje de inicio enviado a Telegram.")

    while True:
        try:
            execute_single_run(args, telegram_token, chat_id)
            logging.info(f"Análisis completado. Esperando {args.sleep} segundos para el próximo ciclo.")
            time.sleep(args.sleep)
        except KeyboardInterrupt:
            logging.info("Bot detenido manualmente. Limpiando estado...")
            clear_state()
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error inesperado en el ciclo principal: {e}")
            time.sleep(60) # Esperar un minuto antes de reintentar en caso de error grave
