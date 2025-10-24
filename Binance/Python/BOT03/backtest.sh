#!/bin/bash

# ==============================================================================
# Script para ejecutar el flujo completo de backtesting y visualización
# ==============================================================================

# Detiene la ejecución del script si algún comando falla
set -e

# --- MEJORA: Hacer el script consciente de su ubicación ---
# Esto permite ejecutar el script desde cualquier directorio.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# --- CONFIGURACIÓN ---
# Modifica estas variables para cambiar los parámetros del backtest
# Los parámetros se pueden pasar como argumentos o usar los valores por defecto.
# Uso: ./backtest.sh [SYMBOL] [INTERVAL] [STRATEGY_SCRIPT]
# Ejemplo: ./backtest.sh BTCUSDT 1h patrones-velas.py

SYMBOL=${1:-"BTCUSDT"}
INTERVAL=${2:-"1h"}
# --- SELECCIÓN DE ESTRATEGIA ---
# Cambia esta línea para probar diferentes bots.
# Opciones: "patrones-velas.py", "range-breakout-v1.py"
STRATEGY_SCRIPT=${3:-"range-breakout-v1.py"} # Script de estrategia a probar

# Parámetros de descarga de datos
LIMIT=1440

# Parámetros de simulación de cuenta
INITIAL_BALANCE=1000
RISK_PER_TRADE=0.02 # Arriesgar el 2% del capital por operación
MAX_OPEN_TRADES=1 # Límite de operaciones abiertas simultáneamente
PARTIAL_TP_COUNT=0 # Desactivamos los TPs parciales para usar solo Trailing Stop
TRAILING_SL_BREAKEVEN="--trailing-sl-breakeven" # Activa el SL a break-even. Comentar para desactivar.
TRAILING_SL_ATR_MULT=2.0 # Multiplicador de ATR para el Trailing Stop dinámico. 0 para desactivar.

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
echo "TPs Parciales: $PARTIAL_TP_COUNT"
echo "Trailing SL a Break-Even: ${TRAILING_SL_BREAKEVEN:+Activado}"
echo "Trailing SL Dinámico (ATR): ${TRAILING_SL_ATR_MULT}"
echo "POC: $POC_PRICE"
echo "------------------------------------"

# --- EJECUCIÓN DEL FLUJO ---

echo "PASO 1: Descargando datos históricos para ${SYMBOL}..."
PYTHONPATH="$SCRIPT_DIR" python3 "$SCRIPT_DIR/download_data.py" --symbol "$SYMBOL" --interval "$INTERVAL" --limit "$LIMIT" --output "$DATA_FILE"

echo -e "\nPASO 2: Ejecutando backtest de detección de señales con ${STRATEGY_SCRIPT}..."
# Se pasan los parámetros universales. El resto se cargan desde .env o los valores por defecto del script.
PYTHONPATH="$SCRIPT_DIR" python3 "$SCRIPT_DIR/$STRATEGY_SCRIPT" \
    --backtest \
    --backtest-file "$DATA_FILE" \
    --symbol "$SYMBOL" \
    --interval "$INTERVAL" \
    --poc "$POC_PRICE" \
    --limit "$LIMIT"

echo -e "\nPASO 3: Simulando resultados de los trades..."
PYTHONPATH="$SCRIPT_DIR" python3 "$SCRIPT_DIR/simulate_trades.py" --trades-file "$TRADES_LOG_FILE" --data-file "$DATA_FILE" --initial-balance "$INITIAL_BALANCE" --risk-per-trade "$RISK_PER_TRADE" --max-open-trades "$MAX_OPEN_TRADES" --partial-tp-count "$PARTIAL_TP_COUNT" $TRAILING_SL_BREAKEVEN --trailing-sl-atr-mult "$TRAILING_SL_ATR_MULT"

echo -e "\nPASO 4: Iniciando el dashboard de visualización..."

# --- MEJORA: Abrir el dashboard en Opera y cerrarlo automáticamente al salir ---
# 1. Iniciar Streamlit en modo "headless" para que no abra el navegador por defecto.
# 2. Esperar un par de segundos para que el servidor esté listo.
# 3. Abrir la URL con 'opera' en segundo plano y capturar su PID.
# 4. Establecer una trampa (trap) para que, al salir del script (Ctrl+C), se mate el proceso de Opera.

echo "Servidor del dashboard iniciado. Abriendo en Opera..."
# Lanzamos Opera en segundo plano y redirigimos su salida para no ensuciar la terminal
(sleep 2 && opera http://localhost:8501 > /dev/null 2>&1) &
OPERA_PID=$! # Capturamos el PID del proceso de Opera

# --- SOLUCIÓN CON XDOTOL: Simular Ctrl+W para cerrar la pestaña activa ---
# La trampa se activa cuando el script recibe la señal de salida (EXIT), por ejemplo, con Ctrl+C.
# Este método busca la ventana de Opera, la activa y le envía el atajo de teclado para cerrar la pestaña.
# Asegúrate de tener 'xdotool' instalado: sudo apt-get install xdotool
trap "echo -e '\nEnviando señal de cierre de pestaña (Ctrl+W) a Opera...'; xdotool search --onlyvisible --class 'opera' windowactivate --sync key ctrl+w 2>/dev/null || true" EXIT

# Ejecutar Streamlit en primer plano. Cuando se presiona Ctrl+C aquí, el script sale y la trampa se activa.
PYTHONPATH="$SCRIPT_DIR" streamlit run "$SCRIPT_DIR/dashboard.py" --server.headless true
