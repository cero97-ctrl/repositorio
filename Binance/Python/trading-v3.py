import pandas as pd
import requests
import ta
import time
import logging
import os
import sys
import argparse # Importa argparse

# === CONFIGURACIÓN ===
# Las constantes ahora se pasan como argumentos o se leen de variables de entorno

# === FUNCIONES DE BINANCE ===
def get_klines(symbol, interval, limit):
    logging.info(f"Obteniendo datos de velas para {symbol}...")
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    response = requests.get(url, timeout=10) # Añadido timeout para mayor robustez
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    # Convertir columnas a tipos numéricos
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col])
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
    sma_vol_indicator = ta.trend.SMAIndicator(close=df['volume'], window=volume_sma_period)
    df['volume_sma'] = sma_vol_indicator.sma_indicator()
    logging.info("Indicadores calculados: EMAs, RSI, MACD, Volume SMA.")
    return df

# === FUNCIONES DE VELAS ALCISTAS ===
def is_hammer(open, close, high, low, body_multiplier=2.0):
    body = abs(close - open)
    if body == 0: return False # Evita división por cero
    lower_shadow = min(open, close) - low
    upper_shadow = high - max(open, close)
    return lower_shadow > (body * body_multiplier) and upper_shadow < body

def is_bullish_engulfing(prev_open, prev_close, curr_open, curr_close):
    return prev_close < prev_open and curr_close > curr_open and curr_open < prev_close and curr_close > prev_open

def is_doji(open, close, high, low):
    body = abs(close - open)
    range_ = high - low
    return range_ > 0 and body / range_ < 0.1  # Evitar división por cero

# === FUNCIONES DE VELAS BAJISTAS ===
def is_shooting_star(open, close, high, low, body_multiplier=2.0):
    body = abs(close - open)
    if body == 0: return False # Evita división por cero
    upper_shadow = high - max(open, close)
    lower_shadow = min(open, close) - low
    return upper_shadow > body * body_multiplier and lower_shadow < body

def is_bearish_engulfing(prev_open, prev_close, curr_open, curr_close):
    return prev_close > prev_open and curr_close < curr_open and curr_open > prev_close and curr_close < prev_open

# === EVALUACIÓN DE SEÑALES ===
# SUGERENCIA: Modificamos la función para que devuelva una lista de señales, no solo la primera que encuentre.
def evaluate_trade(df, rsi_low, rsi_high, rsi_doji_low, rsi_doji_high, hammer_multiplier, shooting_star_multiplier, volume_multiplier):
    logging.info("Evaluando señales de trading...")
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []

    # Condiciones técnicas
    long_conditions = [
        latest['EMA_50'] > latest['EMA_200'],                                           # Tendencia alcista
        latest['RSI'] < rsi_low,                                                        # RSI bajo, posible sobreventa
        latest['MACD'] > latest['MACD_signal'] and previous['MACD'] < previous['MACD_signal'], # Cruce alcista de MACD
        latest['volume'] > latest['volume_sma'] * volume_multiplier                     # Volumen anómalo
    ]

    short_conditions = [
        latest['EMA_50'] < latest['EMA_200'],                                           # Tendencia bajista
        latest['RSI'] > rsi_high,                                                       # RSI alto, posible sobrecompra
        latest['MACD'] < latest['MACD_signal'] and previous['MACD'] > previous['MACD_signal'], # Cruce bajista de MACD
        latest['volume'] > latest['volume_sma'] * volume_multiplier                     # Volumen anómalo
    ]

    # Patrones de velas
    if all(long_conditions):
        signals.append("📈 Señal técnica de entrada en LARGO (compra)")
    if all(short_conditions):
        signals.append("📉 Señal técnica de entrada en CORTO (venta)")

    if is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], hammer_multiplier):
        signals.append("🕯️ Vela tipo MARTILLO detectada: posible reversión alcista")
    if is_bullish_engulfing(previous['open'], previous['close'], latest['open'], latest['close']):
        signals.append("🕯️ Vela ENVOLVENTE ALCISTA detectada: posible entrada en largo")
    if is_doji(latest['open'], latest['close'], latest['high'], latest['low']) and latest['RSI'] < rsi_doji_low:
        signals.append("🕯️ DOJI en zona de sobreventa: posible rebote")

    if is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], shooting_star_multiplier):
        signals.append("🕯️ Vela tipo ESTRELLA FUGAZ detectada: posible reversión bajista")
    if is_bearish_engulfing(previous['open'], previous['close'], latest['open'], latest['close']):
        signals.append("🕯️ Vela ENVOLVENTE BAJISTA detectada: posible entrada en corto")
    if is_doji(latest['open'], latest['close'], latest['high'], latest['low']) and latest['RSI'] > rsi_doji_high:
        signals.append("🕯️ DOJI en zona de sobrecompra: posible caída inminente")

    if not signals:
        return "⏳ No hay señal clara de entrada en este momento"
    
    return "\n".join(signals) # Unimos todas las señales encontradas con un salto de línea

# === ALERTA TELEGRAM ===
def send_telegram_message(message, telegram_token, chat_id): # Ahora recibe token y chat_id
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
# Renombramos la función para que sea más clara, ya que el bucle está en el __main__
def execute_single_run(args, telegram_token, chat_id):
    logging.info(f"Iniciando análisis para {args.symbol} en temporalidad {args.interval}...")
    try:
        df = get_klines(args.symbol, args.interval, args.limit)
        if df.empty:
            logging.warning("No se recibieron datos de Binance. Abortando.")
            return
        
        df = calculate_indicators(df, args.volume_sma_period)
        signal = evaluate_trade(
            df, args.rsi_low, args.rsi_high, args.rsi_doji_low, 
            args.rsi_doji_high, args.hammer_multiplier, args.shooting_star_multiplier, args.volume_multiplier
        )
        
        message = f"--- Análisis para {args.symbol} ({args.interval}) ---\n{signal}"
        logging.info(f"Señal generada:\n{message}")
        
        # Solo enviar a Telegram si no es la señal de "espera"
        if "⏳" not in signal:
            send_telegram_message(message, telegram_token, chat_id)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error de red al contactar con la API de Binance: {e}")
    except Exception as e:
        logging.critical(f"Ha ocurrido un error inesperado en la ejecución del bot: {e}", exc_info=True) # Añadido exc_info para el traceback completo

# === INICIO ===
if __name__ == "__main__":
    # --- Configuración de Argumentos de Línea de Comandos ---
    parser = argparse.ArgumentParser(description="Bot de trading para Binance.")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Símbolo del par (ej: BTCUSDT).")
    parser.add_argument("--interval", type=str, default="1h", help="Temporalidad (ej: 15m, 1h, 4h).")
    parser.add_argument("--limit", type=int, default=200, help="Número de velas a obtener.")
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
    parser.add_argument("--volume-multiplier", type=float, default=1.5, help="Multiplicador para la confirmación de volumen anómalo.")
    args = parser.parse_args()

    # --- Configuración de Logging ---
    logging.basicConfig(level=args.log.upper(), format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

    # --- Configuración de Telegram ---
    telegram_token = os.getenv('TELEGRAM_TOKEN')
    if telegram_token:
        telegram_token = telegram_token.strip()
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    if chat_id:
        chat_id = chat_id.strip()

    logging.info("Iniciando bot de trading en modo continuo...")
    logging.info(f"El análisis se ejecutará cada {args.sleep} segundos.")
    logging.info("Presiona Ctrl+C para detener el bot.")

    while True:
        try:
            execute_single_run(args, telegram_token, chat_id)
            logging.info(f"Ciclo completado. Esperando {args.sleep} segundos para el próximo análisis.")
            time.sleep(args.sleep)
        except KeyboardInterrupt:
            logging.info("Bot detenido manualmente por el usuario. ¡Hasta luego!")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Ocurrió un error inesperado en el ciclo principal: {e}. Reintentando en 60 segundos.", exc_info=True)
            time.sleep(60)
