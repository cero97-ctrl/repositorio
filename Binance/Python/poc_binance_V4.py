import requests
import numpy as np
from datetime import datetime
import sys
import logging
import argparse

# --- Constantes ---
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
# Indices for kline data from Binance API
KLINE_OPEN_TIME_INDEX = 0
KLINE_CLOSE_PRICE_INDEX = 4
KLINE_VOLUME_INDEX = 5


def parse_interval_to_ms(interval: str) -> int:
    """Convierte el string de intervalo de Binance (ej. '1m', '1h', '1d') a milisegundos."""
    unit = interval[-1]
    try:
        value = int(interval[:-1])
    except ValueError:
        raise ValueError(f"Intervalo de tiempo inválido: {interval}")

    if unit == 'm':
        return value * 60 * 1000
    if unit == 'h':
        return value * 60 * 60 * 1000
    if unit == 'd':
        return value * 24 * 60 * 60 * 1000
    raise ValueError(f"Unidad de intervalo no soportada: '{unit}' en '{interval}'")


def get_timestamp_ms(fecha_str: str) -> int:
    """Convierte una fecha en formato 'YYYY-MM-DD' a timestamp en milisegundos."""
    try:
        dt = datetime.strptime(fecha_str, "%Y-%m-%d")
        return int(dt.timestamp() * 1000)
    except ValueError as e:
        raise ValueError(f"El formato de la fecha '{fecha_str}' es incorrecto. Use YYYY-MM-DD.") from e


def get_binance_klines(symbol: str, interval: str, start_time: int, end_time: int) -> list:
    """Obtiene los datos de velas (klines) de Binance para un símbolo y rango de fechas."""
    logging.info(f"Iniciando descarga de klines para {symbol} ({interval})")
    klines = []
    limit = 1000
    
    try:
        interval_ms = parse_interval_to_ms(interval)
    except ValueError as e:
        # Propagamos la excepción para que sea manejada en main
        raise e

    current_time = start_time
    while current_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_time,
            "endTime": end_time,
            "limit": limit
        }
        try:
            response = requests.get(BINANCE_API_URL, params=params)
            response.raise_for_status()  # Lanza un error para respuestas 4xx/5xx
            data = response.json()
        except requests.exceptions.RequestException as e:
            # Lanzamos la excepción para que el llamador decida cómo manejar el error de red
            raise e

        if not data:
            break
        
        klines.extend(data)
        # Avanza al siguiente lote usando el timestamp de la última vela recibida
        last_kline_timestamp = data[-1][KLINE_OPEN_TIME_INDEX]
        current_time = last_kline_timestamp + interval_ms
        logging.debug(f"Obtenidas {len(data)} velas. Próximo lote desde {datetime.fromtimestamp(current_time / 1000)}")

    return klines


def calculate_poc(klines: list, bins: int = 500) -> float:
    """Calcula el Punto de Control (POC) a partir de una lista de klines de forma eficiente."""
    if not klines:
        return 0.0

    # Convertir a NumPy array para un procesamiento vectorizado y eficiente
    klines_np = np.array(klines, dtype=np.float64)
    
    precios = klines_np[:, KLINE_CLOSE_PRICE_INDEX]
    volumenes = klines_np[:, KLINE_VOLUME_INDEX]
    
    # np.histogram es más eficiente con arrays de NumPy
    hist, edges = np.histogram(precios, bins=bins, weights=volumenes)
    idx_poc = np.argmax(hist)
    poc = (edges[idx_poc] + edges[idx_poc + 1]) / 2
    return poc


def main():
    """Función principal para ejecutar el script desde la línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Calcula el Punto de Control (POC) para un par de criptomonedas de Binance.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Símbolo del par (ej: BTCUSDT, ETHUSDT).")
    parser.add_argument("--start", required=True, type=str, help="Fecha de inicio en formato YYYY-MM-DD.")
    parser.add_argument("--end", required=True, type=str, help="Fecha de finalización en formato YYYY-MM-DD.")
    parser.add_argument("--interval", type=str, default="1h", help="Temporalidad (ej: 1m, 5m, 1h, 4h, 1d).")
    parser.add_argument("--prev-poc", required=True, type=float, help="Valor anterior del POC para comparar.")
    parser.add_argument(
        "--log",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Establece el nivel de detalle del log."
    )
    
    args = parser.parse_args()

    # --- Configuración del Logging ---
    logging.basicConfig(
        level=args.log.upper(),
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    try:
        # --- Procesamiento de Datos ---
        start_time = get_timestamp_ms(args.start)
        # Para incluir todo el día de finalización, sumamos 1 día y restamos 1 ms
        end_time = get_timestamp_ms(args.end) + (24 * 60 * 60 * 1000) - 1

        klines = get_binance_klines(
            symbol=args.symbol,
            interval=args.interval,
            start_time=start_time, 
            end_time=end_time
        )

        if not klines:
            logging.warning("No se encontraron datos para el rango especificado.")
            return

        # --- Cálculo y Visualización de Resultados ---
        poc = calculate_poc(klines)
        poc_anterior = args.prev_poc
        cambio_pct = ((poc - poc_anterior) / poc_anterior) * 100 if poc_anterior != 0 else float('inf')
        
        # Códigos de color ANSI para la consola
        COLOR_GREEN = "\033[92m"
        COLOR_RED = "\033[91m"
        COLOR_RESET = "\033[0m"
        color = COLOR_GREEN if cambio_pct >= 0 else COLOR_RED
        
        # El resultado final se imprime directamente a la consola, no se loguea.
        print("\n--- Resultados ---")
        print(f"El Punto de Control (POC) para {args.symbol} del {args.start} al {args.end} ({args.interval}) es: {poc:.2f}")
        print(f"Cambio porcentual respecto al POC anterior ({poc_anterior}): {color}{cambio_pct:.2f}%{COLOR_RESET}")

    except (ValueError, requests.exceptions.RequestException) as e:
        logging.error(f"Ha ocurrido un error crítico: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
