#!/bin/bash

echo "📨 Ejecutando bot de patrones y alertas por Telegram..."
python bot04-v5.py

echo "🟢 Iniciando visualización con Streamlit..."
streamlit run app_streamlit.py

echo "🛑 Streamlit finalizado. Script completo."

