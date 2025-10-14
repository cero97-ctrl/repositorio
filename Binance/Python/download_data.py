# download_data.py
import pandas as pd
import requests
import logging
import argparse
import sys
import os
import time
from math import ceil

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
    logging.info(f"Iniciando descarga de hasta {limit} velas para {symbol} en intervalo {interval}...")
    
    base_url = 'https://api.binance.com/api/v3/klines'
    all_klines = []
    klines_to_fetch = limit
    end_time = None
    
    while klines_to_fetch > 0:
        fetch_limit = min(klines_to_fetch, 1000)
        logging.info(f"Petición para obtener {fetch_limit} velas...")
        
        params = {'symbol': symbol, 'interval': interval, 'limit': fetch_limit}
        if end_time:
            params['endTime'] = end_time
        
        try:
            response = requests.get(base_url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                logging.info("No se recibieron más datos de la API. Finalizando descarga.")
                break
            
            all_klines.extend(data)
            end_time = data[0][0] - 1  # Timestamp de la primera vela para la siguiente petición
            klines_to_fetch -= len(data)
            time.sleep(0.5) # Pausa para no sobrecargar la API
        except requests.exceptions.RequestException as e:
            logging.error(f"Error al obtener datos de Binance: {e}")
            break

    # Columnas requeridas por el script de backtesting
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    
    # Invertimos la lista para que los datos queden en orden cronológico correcto
    all_klines.reverse()
    
    df = pd.DataFrame(all_klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])

    if df.empty:
        logging.warning("No se descargaron datos. No se generará el archivo CSV.")
        return

    # Convertir a numérico y seleccionar solo las columnas necesarias
    for col in required_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Seleccionamos y limpiamos filas con datos nulos en las columnas clave
    df_to_save = df[required_columns].dropna()

    try:
        df_to_save.to_csv(output_file, index=False)
        logging.info(f"✅ Datos guardados exitosamente en '{os.path.abspath(output_file)}'")
    except Exception as e:
        logging.error(f"Error al guardar el archivo CSV: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descargador de datos históricos de Binance.")
    
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Símbolo del par a descargar (ej: BTCUSDT).")
    parser.add_argument("--interval", type=str, default="1h", help="Intervalo de las velas (ej: 1h, 4h, 1d).")
    parser.add_argument("--limit", type=int, default=1000, help="Número de velas a descargar. Puede ser mayor a 1000.")
    parser.add_argument("--output", type=str, default="historical_data.csv", help="Nombre del archivo de salida CSV.")
    
    args = parser.parse_args()

    download_historical_data(
        symbol=args.symbol,
        interval=args.interval,
        limit=args.limit,
        output_file=args.output
    )