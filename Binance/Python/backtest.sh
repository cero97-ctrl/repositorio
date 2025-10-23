#!/bin/bash

# ==============================================================================
# Script para ejecutar el flujo completo de backtesting y visualización
# ==============================================================================

# Detiene la ejecución del script si algún comando falla
set -e

# --- CONFIGURACIÓN ---
# Modifica estas variables para cambiar los parámetros del backtest
# Los parámetros se pueden pasar como argumentos o usar los valores por defecto.
# Uso: ./backtest.sh [SYMBOL] [INTERVAL] [STRATEGY_SCRIPT]
# Ejemplo: ./backtest.sh ETHUSDT 4h wyckoff-multiframe-v2.py

SYMBOL=${1:-"BTCUSDT"}
INTERVAL=${2:-"1h"}
STRATEGY_SCRIPT=${3:-"patrones-velas.py"} # Script de estrategia a probar

# Parámetros de descarga de datos
LIMIT=2160 # Número de velas a descargar

# Parámetros de simulación de cuenta
INITIAL_BALANCE=1000
RISK_PER_TRADE=0.02 # Arriesgar el 2% del capital por operación
MAX_OPEN_TRADES=1 # Límite de operaciones abiertas simultáneamente

# Parámetros específicos de la estrategia (se pasarán como argumentos al script)
# Asegúrate de que estos argumentos son aceptados por el script de estrategia.
POC_PRICE="112840.16" # Precio del Punto de Control (POC)


# Nombres de los archivos generados
DATA_FILE="${SYMBOL,,}_${INTERVAL}_data.csv" # ej: btc_1h_data.csv
TRADES_LOG_FILE="trades_log_backtest.csv"

echo "--- CONFIGURACIÓN DEL BACKTEST ---"
echo "Símbolo: $SYMBOL"
echo "Intervalo: $INTERVAL"
echo "Estrategia: $STRATEGY_SCRIPT"
echo "Balance Inicial: $INITIAL_BALANCE"
echo "Riesgo por Trade: $RISK_PER_TRADE"
echo "Máx. Trades Abiertos: $MAX_OPEN_TRADES"
echo "POC: $POC_PRICE"
echo "------------------------------------"

# --- EJECUCIÓN DEL FLUJO ---

echo "PASO 1: Descargando datos históricos para ${SYMBOL}..."
python download_data.py --symbol "$SYMBOL" --interval "$INTERVAL" --limit "$LIMIT" --output "$DATA_FILE"

echo -e "\nPASO 2: Ejecutando backtest de detección de señales con ${STRATEGY_SCRIPT}..."
# Se pasan los parámetros universales. El resto se cargan desde .env o los valores por defecto del script.
python "$STRATEGY_SCRIPT" \
    --backtest \
    --backtest-file "$DATA_FILE" \
    --symbol "$SYMBOL" \
    --interval "$INTERVAL" \
    --poc "$POC_PRICE"

echo -e "\nPASO 3: Simulando resultados de los trades..."
python simulate_trades.py --trades-file "$TRADES_LOG_FILE" --data-file "$DATA_FILE" --initial-balance "$INITIAL_BALANCE" --risk-per-trade "$RISK_PER_TRADE" --max-open-trades "$MAX_OPEN_TRADES"

echo -e "\nPASO 4: Iniciando el dashboard de visualización..."
streamlit run dashboard.py
