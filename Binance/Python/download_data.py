# download_data.py
import pandas as pd
import requests
import logging
import argparse
import sys
import os

# Configuración básica de logging
logging.basicConfig(
    level="INFO",
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def download_historical_data(symbol, interval, limit, output_file):
    """
    Descarga datos históricos de Binance y los guarda en un archivo CSV.
    """
    logging.info(f"Iniciando descarga de {limit} velas para {symbol} en intervalo {interval}...")

    # La API de Binance tiene un límite de 1000 velas por solicitud.
    # Para simplificar, este script asume que 'limit' es <= 1000.
    # Para una versión más avanzada, se necesitaría un bucle para obtener más de 1000 velas.
    if limit > 1000:
        logging.warning("La API de Binance tiene un límite de 1000 velas por solicitud. Se descargarán 1000.")
        limit = 1000

    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Se recibieron {len(data)} velas de la API.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos de Binance: {e}")
        return

    # Columnas requeridas por el script de backtesting
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])

    # Convertir a numérico y seleccionar solo las columnas necesarias
    for col in required_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df_to_save = df[required_columns]
    df_to_save.dropna(inplace=True)

    try:
        df_to_save.to_csv(output_file, index=False)
        logging.info(f"✅ Datos guardados exitosamente en '{os.path.abspath(output_file)}'")
    except Exception as e:
        logging.error(f"Error al guardar el archivo CSV: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descargador de datos históricos de Binance.")
    
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Símbolo del par a descargar (ej: BTCUSDT).")
    parser.add_argument("--interval", type=str, default="1h", help="Intervalo de las velas (ej: 1h, 4h, 1d).")
    parser.add_argument("--limit", type=int, default=1000, help="Número de velas a descargar (máximo 1000 por solicitud).")
    parser.add_argument("--output", type=str, default="historical_data.csv", help="Nombre del archivo de salida CSV.")
    
    args = parser.parse_args()

    download_historical_data(
        symbol=args.symbol,
        interval=args.interval,
        limit=args.limit,
        output_file=args.output
    )