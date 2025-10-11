import pandas as pd
import requests
import ta
import time
import logging
import os
import sys
import json
import argparse

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
        return pd.DataFrame() # Retorna un DataFrame vacío en caso de error

    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    # Convertir columnas a tipos numéricos
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)
    logging.info("Datos de velas obtenidos y procesados.")
    return df

# === FUNCIONES DE INDICADORES ===
def calculate_indicators(df, volume_sma_period):
    logging.info("Calculando indicadores técnicos...")
    df['EMA_50'] = ta.trend.ema_indicator(df['close'], window=50)
    df['EMA_200'] = ta.trend.ema_indicator(df['close'], window=200)
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    macd_indicator = ta.trend.MACD(df['close'])
    df['MACD'] = macd_indicator.macd()
    df['MACD_signal'] = macd_indicator.macd_signal()
    sma_vol_indicator = ta.volume.SMAIndicator(volume=df['volume'], window=volume_sma_period)
    df['volume_sma'] = sma_vol_indicator.sma_indicator()
    logging.info("Indicadores calculados: EMAs, RSI, MACD, Volume SMA.")
    return df

# === FUNCIONES DE PATRONES DE VELAS ===
def is_hammer(open_price, close_price, high, low, body_multiplier=2.0):
    body = abs(close_price - open_price)
    if body == 0: return False
    lower_shadow = min(open_price, close_price) - low
    upper_shadow = high - max(open_price, close_price)
    return lower_shadow > (body * body_multiplier) and upper_shadow < body

def is_bullish_engulfing(prev_open, prev_close, curr_open, curr_close):
    return prev_close < prev_open and curr_close > curr_open and curr_open < prev_close and curr_close > prev_open

def is_doji(open_price, close_price, high, low):
    body = abs(close_price - open_price)
    price_range = high - low
    return price_range > 0 and body / price_range < 0.1

def is_shooting_star(open_price, close_price, high, low, body_multiplier=2.0):
    body = abs(close_price - open_price)
    if body == 0: return False
    upper_shadow = high - max(open_price, close_price)
    lower_shadow = min(open_price, close_price) - low
    return upper_shadow > body * body_multiplier and lower_shadow < body

def is_bearish_engulfing(prev_open, prev_close, curr_open, curr_close):
    return prev_close > prev_open and curr_close < curr_open and curr_open > prev_close and curr_close < prev_open

# === GESTIÓN DE ESTADO ===
STATE_FILE = "state.json"

def save_state(state):
    logging.info(f"Guardando estado pendiente: {state}")
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def load_state():
    if os.path.exists(STATE_FILE):
        logging.info("Archivo de estado encontrado. Cargando estado pendiente.")
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return None

def clear_state():
    if os.path.exists(STATE_FILE):
        logging.info("Limpiando estado pendiente.")
        os.remove(STATE_FILE)

# === EVALUACIÓN DE CONFIRMACIÓN ===
def check_confirmation(df, state, volume_multiplier):
    """Evalúa la vela de confirmación después de una señal de patrón con lógica estricta."""
    logging.info(f"Evaluando vela de confirmación para el estado: {state}")
    latest = df.iloc[-1]
    previous = df.iloc[-2] # La vela que generó la señal (el patrón)
    pattern = state.get("pattern")
    message = ""

    # Condición de volumen para la vela de confirmación
    confirmation_volume_ok = latest['volume'] > latest['volume_sma'] * volume_multiplier

    if pattern in ["hammer", "bullish_engulfing", "doji_oversold"]:
        # Confirmación alcista fuerte: vela verde Y cierre por encima del máximo anterior.
        if latest['close'] > latest['open'] and latest['close'] > previous['high']:
            vol_msg = "con volumen fuerte." if confirmation_volume_ok else "aunque con volumen débil."
            message = f"✅ CONFIRMACIÓN FUERTE para '{pattern}': La vela cerró por encima del máximo anterior {vol_msg}"
        else:
            message = f"❌ CONFIRMACIÓN DÉBIL O NULA para '{pattern}': La vela no superó el máximo anterior. ¡Señal invalidada!"

    elif pattern in ["shooting_star", "bearish_engulfing", "doji_overbought"]:
        # Confirmación bajista fuerte: vela roja Y cierre por debajo del mínimo anterior.
        if latest['close'] < latest['open'] and latest['close'] < previous['low']:
            vol_msg = "con volumen fuerte." if confirmation_volume_ok else "aunque con volumen débil."
            message = f"✅ CONFIRMACIÓN FUERTE para '{pattern}': La vela cerró por debajo del mínimo anterior {vol_msg}"
        else:
            message = f"❌ CONFIRMACIÓN DÉBIL O NULA para '{pattern}': La vela no rompió el mínimo anterior. ¡Señal invalidada!"
    
    clear_state()

    if message:
        return message
    return "No se pudo determinar la confirmación."

# === FUNCIONES AUXILIARES DE ANÁLISIS ===
def check_macd_cross(df, lookback=3, direction="bullish"):
    """Verifica si hubo un cruce de MACD en las últimas 'lookback' velas."""
    recent_candles = df.iloc[-lookback:]
    if direction == "bullish":
        crossed_up = (recent_candles['MACD'] > recent_candles['MACD_signal']) & (recent_candles['MACD'].shift(1) < recent_candles['MACD_signal'].shift(1))
        return crossed_up.any()
    elif direction == "short":
        crossed_down = (recent_candles['MACD'] < recent_candles['MACD_signal']) & (recent_candles['MACD'].shift(1) > recent_candles['MACD_signal'].shift(1))
        return crossed_down.any()
    return False

# === EVALUACIÓN DE SEÑALES ===
def evaluate_trade(df, args):
    logging.info("Evaluando señales de trading...")
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    # --- Lógica de Proximidad al POC ---
    poc_zone_active = False
    if args.poc > 0:
        poc_proximity_percentage = 0.005 # 0.5% de proximidad al POC
        poc_upper_bound = args.poc * (1 + poc_proximity_percentage)
        poc_lower_bound = args.poc * (1 - poc_proximity_percentage)
        if latest['high'] >= poc_lower_bound and latest['low'] <= poc_upper_bound:
            poc_zone_active = True
            logging.info(f"La vela actual está interactuando con la zona del POC ({poc_lower_bound:.2f} - {poc_upper_bound:.2f}).")

    # --- Condiciones Técnicas Generales (opcional, se pueden comentar si se prefiere solo patrones) ---
    long_conditions = [
        latest['EMA_50'] > latest['EMA_200'],
        latest['RSI'] < args.rsi_low,
        check_macd_cross(df, lookback=3, direction="bullish"),
        latest['volume'] > latest['volume_sma'] * args.volume_multiplier
    ]
    short_conditions = [
        latest['EMA_50'] < latest['EMA_200'],
        latest['RSI'] > args.rsi_high,
        check_macd_cross(df, lookback=3, direction="short"),
        latest['volume'] > latest['volume_sma'] * args.volume_multiplier
    ]
    if all(long_conditions):
        signals.append("📈 Señal técnica de entrada en LARGO (compra)")
    if all(short_conditions):
        signals.append("📉 Señal técnica de entrada en CORTO (venta)")

    # --- Patrones de Velas con Confluencia de POC ---
    if is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier):
        signal_text = "🕯️ Vela tipo MARTILLO detectada"
        if poc_zone_active:
            signal_text += f" en ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥. ¡Alta probabilidad de rebote!"
        else:
            signal_text += ": posible reversión alcista."
        signals.append(signal_text)
        pending_state = {"pattern": "hammer", "price": latest['close']}

    if is_bullish_engulfing(previous['open'], previous['close'], latest['open'], latest['close']):
        signal_text = "🕯️ Vela ENVOLVENTE ALCISTA detectada"
        if poc_zone_active:
            signal_text += f" en ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥. ¡Alta probabilidad de rebote!"
        else:
            signal_text += ": posible entrada en largo."
        signals.append(signal_text)
        pending_state = {"pattern": "bullish_engulfing", "price": latest['close']}

    if is_doji(latest['open'], latest['close'], latest['high'], latest['low']) and latest['RSI'] < args.rsi_doji_low:
        signal_text = "🕯️ DOJI en zona de sobreventa"
        if poc_zone_active:
            signal_text += f" y en ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥. ¡Señal de agotamiento vendedor muy fuerte!"
        else:
            signal_text += ": posible rebote."
        signals.append(signal_text)
        pending_state = {"pattern": "doji_oversold", "price": latest['close']}

    if is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier):
        signal_text = "🕯️ Vela tipo ESTRELLA FUGAZ detectada"
        if poc_zone_active:
            signal_text += f" en ZONA DE RESISTENCIA POC (${args.poc:.2f})  रेजि. ¡Alta probabilidad de rechazo!"
        else:
            signal_text += ": posible reversión bajista."
        signals.append(signal_text)
        pending_state = {"pattern": "shooting_star", "price": latest['close']}

    if is_bearish_engulfing(previous['open'], previous['close'], latest['open'], latest['close']):
        signal_text = "🕯️ Vela ENVOLVENTE BAJISTA detectada"
        if poc_zone_active:
            signal_text += f" en ZONA DE RESISTENCIA POC (${args.poc:.2f})  रेजि. ¡Alta probabilidad de rechazo!"
        else:
            signal_text += ": posible entrada en corto."
        signals.append(signal_text)
        pending_state = {"pattern": "bearish_engulfing", "price": latest['close']}

    if is_doji(latest['open'], latest['close'], latest['high'], latest['low']) and latest['RSI'] > args.rsi_doji_high:
        signal_text = "🕯️ DOJI en zona de sobrecompra"
        if poc_zone_active:
            signal_text += f" y en ZONA DE RESISTENCIA POC (${args.poc:.2f})  रेजि. ¡Señal de agotamiento comprador muy fuerte!"
        else:
            signal_text += ": posible caída inminente."
        signals.append(signal_text)
        pending_state = {"pattern": "doji_overbought", "price": latest['close']}

    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(latest['timestamp'], unit='ms').isoformat()
        save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    if not signals:
        return "⏳ No hay señal clara de entrada en este momento"
    
    return "\n".join(signals)

# === ALERTA TELEGRAM ===
def send_telegram_message(message, telegram_token, chat_id):
    if not telegram_token or not chat_id or telegram_token == 'TU_TOKEN_AQUI':
        logging.warning("Token de Telegram o Chat ID no configurados. Omitiendo notificación.")
        return
    url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logging.info("Mensaje de Telegram enviado con éxito.")
    except requests.exceptions.RequestException as e:
        logging.error(f"No se pudo enviar el mensaje de Telegram: {e}")

# === LÓGICA DE EJECUCIÓN DEL BOT (UNA SOLA VEZ) ===
def execute_single_run(args, telegram_token, chat_id):
    logging.info(f"Iniciando análisis para {args.symbol} en temporalidad {args.interval}...")
    
    pending_state = load_state()
    signal = None

    try:
        df = get_klines(args.symbol, args.interval, args.limit)
        if df.empty or len(df) < 2: # Necesitamos al menos 2 velas para evaluar patrones
            logging.warning("No se recibieron suficientes datos de Binance. Abortando ciclo.")
            return
        
        if pending_state:
            latest_candle_time = pd.to_datetime(df.iloc[-1]['timestamp'], unit='ms')
            signal_time = pd.to_datetime(pending_state.get("signal_time"))
            expected_diff = pd.to_timedelta(args.interval)

            if (latest_candle_time - signal_time) > expected_diff:
                logging.warning(f"El estado pendiente para '{pending_state.get('pattern')}' es obsoleto. Descartando.")
                clear_state()
                pending_state = None
            else:
                df = calculate_indicators(df, args.volume_sma_period)
                signal = check_confirmation(df, pending_state, args.volume_multiplier)
        
        if not pending_state:
            df = calculate_indicators(df, args.volume_sma_period)
            signal = evaluate_trade(df, args)
        
        message = f"--- Análisis para {args.symbol} ({args.interval}) ---\n{signal}"
        logging.info(f"Señal generada:\n{message}")
        
        if "⏳" not in signal:
            send_telegram_message(message, telegram_token, chat_id)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error de red al contactar con la API de Binance: {e}")
    except Exception as e:
        logging.critical(f"Ha ocurrido un error inesperado en la ejecución del bot: {e}", exc_info=True)

# === INICIO ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bot de trading para Binance v5.")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Símbolo del par (ej: BTCUSDT).")
    parser.add_argument("--interval", type=str, default="1h", help="Temporalidad (ej: 15m, 1h, 4h).")
    parser.add_argument("--limit", type=int, default=202, help="Número de velas a obtener (200 para indicadores + 2 para patrones).")
    parser.add_argument(
        "--log",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Establece el nivel de detalle del log."
    )
    parser.add_argument("--sleep", type=int, default=3600, help="Intervalo de espera en segundos entre cada ciclo (por defecto 1 hora).")
    # --- Argumentos de Estrategia ---
    parser.add_argument("--rsi-low", type=int, default=40, help="Umbral bajo de RSI para señales de compra.")
    parser.add_argument("--rsi-high", type=int, default=60, help="Umbral alto de RSI para señales de venta.")
    parser.add_argument("--rsi-doji-low", type=int, default=30, help="Umbral de sobreventa para Doji.")
    parser.add_argument("--rsi-doji-high", type=int, default=70, help="Umbral de sobrecompra para Doji.")
    parser.add_argument("--hammer-multiplier", type=float, default=2.0, help="Multiplicador de cuerpo para patrón Martillo.")
    parser.add_argument("--shooting-star-multiplier", type=float, default=2.0, help="Multiplicador de cuerpo para Estrella Fugaz.")
    parser.add_argument("--volume-sma-period", type=int, default=20, help="Periodo para la media móvil del volumen.")
    parser.add_argument("--poc", type=float, default=0, help="Nivel de precio del Punto de Control (POC) para análisis de confluencia.")
    parser.add_argument("--volume-multiplier", type=float, default=1.5, help="Multiplicador para la confirmación de volumen anómalo.")
    args = parser.parse_args()

    logging.basicConfig(level=args.log.upper(), format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

    telegram_token = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()

    logging.info("Iniciando bot de trading v5 en modo continuo...")
    logging.info(f"El análisis se ejecutará cada {args.sleep} segundos.")
    logging.info("Presiona Ctrl+C para detener el bot.")

    while True:
        try:
            execute_single_run(args, telegram_token, chat_id)
            logging.info(f"Ciclo completado. Esperando {args.sleep} segundos para el próximo análisis.")
            time.sleep(args.sleep)
        except KeyboardInterrupt:
            logging.info("Bot detenido manualmente por el usuario. ¡Hasta luego!")
            clear_state() # Limpia el estado al salir
            sys.exit(0)
        except Exception as e:
            logging.error(f"Ocurrió un error inesperado en el ciclo principal: {e}. Reintentando en 60 segundos.", exc_info=True)
            time.sleep(60)