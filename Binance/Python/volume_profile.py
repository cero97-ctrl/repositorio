import pandas as pd
import numpy as np
import requests
import logging

# Es una buena práctica configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def volume_profile_analysis(symbol='BTCUSDT', interval='1h', limit=1000, num_bins=100):
    """
    Calcula un Fixed Range Volume Profile (FRVP) más preciso para un par y temporalidad.

    Esta versión mejorada distribuye el volumen de cada vela a través de su rango (high-low)
    y utiliza un número fijo de 'bins' para que el análisis sea adaptable a cualquier activo.

    Args:
        symbol (str): Símbolo del par (ej. 'BTCUSDT').
        interval (str): Temporalidad de las velas (ej. '1h').
        limit (int): Número de velas a obtener.
        num_bins (int): Número de niveles de precio en los que se dividirá el rango.

    Returns:
        dict: Un diccionario con el POC, HVNs, LVNs y el perfil completo.
              Retorna None si ocurre un error.
    """
    logging.info(f"Iniciando análisis de Volume Profile para {symbol} en {interval}...")
    
    # === 1. Obtener datos de velas de forma robusta ===
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Lanza una excepción para errores HTTP (4xx o 5xx)
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener datos de Binance: {e}")
        return None

    if not data:
        logging.warning("No se recibieron datos de Binance.")
        return None

    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
    ])
    
    # Convertir todas las columnas relevantes a tipo numérico
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df.dropna(inplace=True)

    # === 2. Calcular el tamaño dinámico del bin ===
    price_range = df['high'].max() - df['low'].min()
    bin_size = price_range / num_bins
    logging.info(f"Rango de precios total: {price_range:.2f}, Tamaño del bin: {bin_size:.4f}")

    # Inicializar el perfil de volumen (un diccionario para los bins)
    volume_profile = {i: 0 for i in np.arange(df['low'].min(), df['high'].max(), bin_size)}

    # === 3. Distribuir el volumen de cada vela en su rango (low-high) ===
    for _, row in df.iterrows():
        volume_per_candle = row['volume']
        price_low = row['low']
        price_high = row['high']
        
        # Identificar los bins que esta vela tocó
        bins_touched = [bin_start for bin_start in volume_profile.keys() if bin_start >= price_low and bin_start < price_high]
        
        if not bins_touched:
            continue # Evitar división por cero si la vela es muy pequeña

        # Distribuir el volumen de la vela equitativamente entre los bins que tocó
        volume_per_bin = volume_per_candle / len(bins_touched)
        for bin_start in bins_touched:
            volume_profile[bin_start] += volume_per_bin

    # Convertir el diccionario a una Serie de Pandas para un análisis más fácil
    volume_series = pd.Series(volume_profile).sort_index()

    # === 4. Detectar POC, HVN y LVN ===
    if volume_series.empty:
        logging.warning("El perfil de volumen está vacío, no se puede analizar.")
        return None

    # Punto de Control (POC): El nivel de precio con el mayor volumen
    poc_price = volume_series.idxmax()
    poc_volume = volume_series.max()

    # Zonas de Alto Volumen (HVN - High Volume Nodes): por encima del percentil 75
    hvn_threshold = np.percentile(volume_series.values, 75)
    high_volume_nodes = volume_series[volume_series >= hvn_threshold]

    # Zonas de Bajo Volumen (LVN - Low Volume Nodes): por debajo del percentil 25
    lvn_threshold = np.percentile(volume_series.values, 25)
    low_volume_nodes = volume_series[volume_series <= lvn_threshold]

    # === 5. Formatear el resultado ===
    result = {
        'symbol': symbol,
        'interval': interval,
        'point_of_control': {'price_level': poc_price, 'volume': poc_volume},
        'high_volume_nodes': high_volume_nodes.to_dict(),
        'low_volume_nodes': low_volume_nodes.to_dict(),
        'full_profile': volume_series.to_dict()
    }
    
    logging.info(f"Análisis completado. POC encontrado en: {poc_price:.2f}")
    return result

# Ejemplo de uso:
if __name__ == '__main__':
    profile = volume_profile_analysis(symbol='BTCUSDT', interval='1h', limit=2000, num_bins=700)
    if profile:
        print("\n--- RESULTADO DEL ANÁLISIS DE PERFIL DE VOLUMEN ---")
        print(f"Punto de Control (POC): Precio ~${profile['point_of_control']['price_level']:.2f} con un volumen de {profile['point_of_control']['volume']:.2f}")
        
        # Imprimir las 3 zonas de mayor volumen
        hvns = sorted(profile['high_volume_nodes'].items(), key=lambda item: item[1], reverse=True)
        print("\nPrincipales Zonas de Alto Volumen (HVN):")
        for price, vol in hvns[:3]:
            print(f"  - Precio ~${price:.2f} (Volumen: {vol:.2f})")

        # Imprimir las 3 zonas de menor volumen
        lvns = sorted(profile['low_volume_nodes'].items(), key=lambda item: item[1])
        print("\nPrincipales Zonas de Bajo Volumen (LVN):")
        for price, vol in lvns[:3]:
            print(f"  - Precio ~${price:.2f} (Volumen: {vol:.2f})")


