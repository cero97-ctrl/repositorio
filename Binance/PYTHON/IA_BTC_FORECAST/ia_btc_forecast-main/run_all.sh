#!/usr/bin/env bash
set -euo pipefail

# run_all.sh — Orquesta la recolección de datos y el entrenamiento del modelo.
# Uso: desde el directorio del proyecto:
#   ./run_all.sh [--log FILE]

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

LOG_FILE="run_all.log"
if [[ ${1-} == "--log" && ${2-} != "" ]]; then
  LOG_FILE="$2"
fi

echo "[run_all] Inicio: $(date)" | tee -a "$LOG_FILE"

echo "[run_all] Ejecutando recolección de datos: get_data_btc.py" | tee -a "$LOG_FILE"
python get_data_btc.py 2>&1 | tee -a "$LOG_FILE"

echo "[run_all] Ejecutando entrenamiento: model_forecast.py" | tee -a "$LOG_FILE"
python model_forecast.py 2>&1 | tee -a "$LOG_FILE"

echo "[run_all] Fin: $(date)" | tee -a "$LOG_FILE"

echo "Resultados y logs en: $LOG_FILE"
