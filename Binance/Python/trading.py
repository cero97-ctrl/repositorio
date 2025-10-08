import pandas as pd
import requests
import ta
import time
import os
import logging
import sys
import argparse

# === CONFIGURACIÓN ===
# Las constantes se mueven a la función main o se pasan como argumentos

# === FUNCIONES ===
def get_klines(symbol, interval, limit):
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    logging.info(f"Obteniendo datos de velas para {symbol}...")
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Lanza una excepción para errores HTTP (4xx o 5xx)
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    logging.info("Datos de velas obtenidos y procesados correctamente.")
    return df

def calculate_indicators(df):
    logging.info("Calculando indicadores técnicos...")
    # Crear una instancia del indicador MACD
    macd_indicator = ta.trend.MACD(df['close'])
    
    # Calcular los indicadores y asignarlos al DataFrame
    df['EMA_50'] = ta.trend.ema_indicator(df['close'], window=50)
    df['EMA_200'] = ta.trend.ema_indicator(df['close'], window=200)
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    df['MACD'] = macd_indicator.macd()
    df['MACD_signal'] = macd_indicator.macd_signal()

    # --- Patrones de Velas ---
    df['bullish_engulfing'] = ta.pattern.ta_bullish_engulfing(df['open'], df['high'], df['low'], df['close'])
    df['bearish_engulfing'] = ta.pattern.ta_bearish_engulfing(df['open'], df['high'], df['low'], df['close'])

    logging.info("Indicadores calculados: EMAs, RSI, MACD y Patrones de Velas.")
    return df

def evaluate_trade(df):
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    long_conditions = [
        latest['EMA_50'] > latest['EMA_200'],  # Tendencia alcista
        latest['RSI'] < 40,                     # RSI en zona de sobreventa o baja
        latest['MACD'] > latest['MACD_signal'] and previous['MACD'] < previous['MACD_signal'], # Cruce de MACD
        latest['bullish_engulfing'] == 100      # Patrón de vela envolvente alcista
    ]

    short_conditions = [
        latest['EMA_50'] < latest['EMA_200'],  # Tendencia bajista
        latest['RSI'] > 60,                     # RSI en zona de sobrecompra o alta
        latest['MACD'] < latest['MACD_signal'] and previous['MACD'] > previous['MACD_signal'], # Cruce de MACD
        latest['bearish_engulfing'] == -100     # Patrón de vela envolvente bajista
    ]

    if all(long_conditions):
        return "📈 Señal de entrada en LARGO (compra)"
    elif all(short_conditions):
        return "📉 Señal de entrada en CORTO (venta)"
    else:
        return "⏳ No hay señal clara de entrada en este momento"

def send_telegram_message(message, token, chat_id):
    if not token or not chat_id:
        logging.warning("Token de Telegram o Chat ID no configurados. Omitiendo notificación.")
        return
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logging.info("Mensaje de Telegram enviado con éxito.")
    except requests.exceptions.RequestException as e:
        logging.error(f"No se pudo enviar el mensaje de Telegram: {e}")

# === EJECUCIÓN PRINCIPAL ===
def main():
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
    parser.add_argument("--sleep", type=int, default=900, help="Intervalo de espera en segundos entre cada ciclo.")
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

    try:
        df = get_klines(args.symbol, args.interval, args.limit)
        if df.empty:
            logging.warning("No se recibieron datos de Binance. Abortando.")
            return

        df = calculate_indicators(df)
        signal = evaluate_trade(df)
        
        message = f"[{args.symbol} - {args.interval}] {signal}"
        logging.info(f"Señal generada: {message}")
        send_telegram_message(message, telegram_token, chat_id)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error de red al contactar con la API de Binance: {e}")
    except Exception as e:
        logging.critical(f"Ha ocurrido un error inesperado en la ejecución del bot: {e}", exc_info=True)

# === LOOP (opcional para ejecución continua) ===
if __name__ == "__main__":
    # --- Configuración del bucle ---
    # Intervalo de ejecución en segundos (ej: 15 minutos = 900s, 1 hora = 3600s)
    RUN_INTERVAL_SECONDS = 900

    logging.info("Iniciando bot de trading en modo continuo...")
    logging.info(f"El análisis se ejecutará cada {RUN_INTERVAL_SECONDS} segundos.")
    logging.info("Presiona Ctrl+C para detener el bot.")

    while True:
        try:
            main()
            logging.info(f"Ciclo completado. Esperando {RUN_INTERVAL_SECONDS} segundos para el próximo análisis.")
            time.sleep(RUN_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logging.info("Bot detenido manualmente por el usuario. ¡Hasta luego!")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Ocurrió un error inesperado en el ciclo principal: {e}. Reintentando en 60 segundos.")
            time.sleep(60)