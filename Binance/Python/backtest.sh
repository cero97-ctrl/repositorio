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
STRATEGY_SCRIPT=${3:-"wyckoff-multiframe-v3.py"} # Script de estrategia a probar

# Parámetros de descarga de datos
LIMIT=2000 # Número de velas a descargar

# Parámetros de simulación de cuenta
INITIAL_BALANCE=1000
RISK_PER_TRADE=0.02 # Arriesgar el 2% del capital por operación

# Parámetros específicos de la estrategia (se pasarán como argumentos al script)
# Asegúrate de que estos argumentos son aceptados por el script de estrategia.
WYCKOFF_PROFESSIONAL="--wyckoff-professional" # Añade esto para activar el modo profesional, o déjalo vacío "" para desactivarlo.
HTF_INTERVAL="4h" # Timeframe superior para el análisis de contexto
POC_PRICE="65000" # Precio del Punto de Control (POC)
# Parámetros específicos para la v2 (Análisis Estructural)
HTF_LOOKBACK=100 # Velas a mirar en HTF para análisis estructural
# HTF_RANGE_THRESH ya no se usa en v3
# Parámetros específicos para la v3 (Máquina de Estados)
HTF_CLIMACTIC_VOL_MULT=2.5
HTF_TEST_VOL_MULT=0.8


# Nombres de los archivos generados
DATA_FILE="${SYMBOL,,}_${INTERVAL}_data.csv" # ej: btc_1h_data.csv
TRADES_LOG_FILE="trades_log_backtest.csv"

echo "--- CONFIGURACIÓN DEL BACKTEST ---"
echo "Símbolo: $SYMBOL"
echo "Intervalo: $INTERVAL"
echo "Estrategia: $STRATEGY_SCRIPT"
echo "Balance Inicial: $INITIAL_BALANCE"
echo "Riesgo por Trade: $RISK_PER_TRADE"
echo "Modo Profesional: ${WYCKOFF_PROFESSIONAL:-'No'}"
echo "POC: $POC_PRICE"
echo "HTF Interval: $HTF_INTERVAL"
echo "HTF Lookback: $HTF_LOOKBACK"
echo "HTF Climactic Volume Multiplier: $HTF_CLIMACTIC_VOL_MULT"
echo "HTF Test Volume Multiplier: $HTF_TEST_VOL_MULT"
echo "------------------------------------"

# --- EJECUCIÓN DEL FLUJO ---

echo "PASO 1: Descargando datos históricos para ${SYMBOL}..."
python download_data.py --symbol "$SYMBOL" --interval "$INTERVAL" --limit "$LIMIT" --output "$DATA_FILE" > /dev/null 2>&1

echo -e "\nPASO 2: Ejecutando backtest de detección de señales con ${STRATEGY_SCRIPT}..."
# CORRECCIÓN: Se pasan todos los parámetros relevantes al script de estrategia.
python "$STRATEGY_SCRIPT" \
    --backtest \
    --backtest-file "$DATA_FILE" \
    --symbol "$SYMBOL" \
    --interval "$INTERVAL" \
    --poc "$POC_PRICE" \
    $WYCKOFF_PROFESSIONAL --htf-interval "$HTF_INTERVAL" \
    --htf-lookback "$HTF_LOOKBACK" \
    --htf-climactic-vol-mult "$HTF_CLIMACTIC_VOL_MULT" \
    --htf-test-vol-mult "$HTF_TEST_VOL_MULT"

echo -e "\nPASO 3: Simulando resultados de los trades..."
python simulate_trades.py --trades-file "$TRADES_LOG_FILE" --data-file "$DATA_FILE" --initial-balance "$INITIAL_BALANCE" --risk-per-trade "$RISK_PER_TRADE"

echo -e "\nPASO 4: Iniciando el dashboard de visualización..."
streamlit run dashboard.py
