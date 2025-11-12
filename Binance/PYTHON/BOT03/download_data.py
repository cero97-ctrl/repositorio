# download_data.py
import argparse
import logging
import pandas as pd
import common_utils as utils

logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")


def download_historical_data(symbol, interval, limit, output_file):
    """
    Descarga datos históricos de Binance y los guarda en un archivo CSV.
    """
    logging.info(f"Descargando {limit} velas para {symbol} con intervalo {interval}...")

    try:
        # Usamos la función get_klines de common_utils que ya tenemos
        # --- CORRECCIÓN CRÍTICA: Asegurar que se descarguen los datos más recientes ---
        # La función get_klines ya está diseñada para obtener las últimas 'limit' velas
        # si no se especifican start_time y end_time. Nos aseguramos de usar esa lógica.
        df = utils.get_klines(symbol, interval, limit=limit)

        if not df.empty:
            # --- CORRECCIÓN: Convertir el timestamp de ms a formato legible antes de guardar ---
            # Esto estandariza el formato en todos los archivos CSV y evita problemas de conversión.
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.to_csv(output_file, index=False)

            logging.info(
                f"✅ Datos guardados exitosamente en '{output_file}'. Se descargaron {len(df)} velas."
            )
        else:
            logging.warning(
                "No se recibieron datos de Binance. El DataFrame está vacío."
            )

    except Exception as e:
        logging.error(f"Ocurrió un error al descargar los datos: {e}")
        # Salir con un código de error para que el script de backtest se detenga
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Descargador de datos históricos de Binance."
    )
    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Símbolo del par a descargar (ej: BTCUSDT).",
    )
    parser.add_argument(
        "--interval",
        type=str,
        required=True,
        help="Intervalo de las velas (ej: 1h, 4h, 1d).",
    )
    parser.add_argument(
        "--limit", type=int, default=500, help="Número de velas a descargar."
    )
    parser.add_argument(
        "--output", type=str, required=True, help="Nombre del archivo CSV de salida."
    )

    args = parser.parse_args()

    download_historical_data(args.symbol, args.interval, args.limit, args.output)
