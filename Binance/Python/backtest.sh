#!/bin/bash

# ==============================================================================
# Script para ejecutar el flujo completo de backtesting y visualización
# ==============================================================================

# Detiene la ejecución del script si algún comando falla
set -e

# --- CONFIGURACIÓN ---
# Modifica estas variables para cambiar los parámetros del backtest
# Los parámetros se pueden pasar como argumentos o usar los valores por defecto.
# Uso: ./backtest.sh [SYMBOL] [STRATEGY_SCRIPT]
# Ejemplo: ./backtest.sh ETHUSDT trading_wyckoff.py

SYMBOL=${1:-"BTCUSDT"}
INTERVAL="1h"
LIMIT=2000
INITIAL_BALANCE=60
RISK_PER_TRADE=0.01

STRATEGY_SCRIPT=${2:-"trading-v6.py"} # Opciones: "trading-v6.py" o "trading_wyckoff.py"

# Nombres de los archivos generados
DATA_FILE="${SYMBOL,,}_${INTERVAL}_data.csv" # ej: btc_1h_data.csv
TRADES_LOG_FILE="trades_log_backtest.csv"

# --- EJECUCIÓN DEL FLUJO ---

echo "PASO 1: Descargando datos históricos para ${SYMBOL}..."
python download_data.py --symbol "$SYMBOL" --interval "$INTERVAL" --limit "$LIMIT" --output "$DATA_FILE"

echo -e "\nPASO 2: Ejecutando backtest de detección de señales con ${STRATEGY_SCRIPT}..."
python "$STRATEGY_SCRIPT" --backtest --backtest-file "$DATA_FILE"

echo -e "\nPASO 3: Simulando resultados de los trades..."
python simulate_trades.py --trades-file "$TRADES_LOG_FILE" --data-file "$DATA_FILE" --initial-balance "$INITIAL_BALANCE" --risk-per-trade "$RISK_PER_TRADE"

echo -e "\nPASO 4: Iniciando el dashboard de visualización..."
streamlit run dashboard.py
