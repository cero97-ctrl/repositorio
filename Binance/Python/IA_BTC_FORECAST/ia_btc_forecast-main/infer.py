#!/usr/bin/env python3
"""Ejemplo simple de inferencia usando el modelo y scaler guardados.

Uso:
  python infer.py

Lee `data/prices.csv`, toma los últimos `seq_length` puntos, escala con el scaler
guardado, realiza una predicción y muestra el precio previsto (desescalado).
"""
import numpy as np
import pandas as pd
import os
import joblib
from keras._tf_keras.keras.models import load_model

# Config
MODEL_PATH = "model/btc_forecast_model.keras"
SCALER_PATH = "model/scaler.pkl"
DATA_PATH = "data/prices.csv"
SEQ_LENGTH = 30

if not os.path.exists(MODEL_PATH) or not os.path.exists(SCALER_PATH):
    raise SystemExit("Modelo o scaler no encontrados. Ejecuta primero model_forecast.py para entrenar y guardar el modelo.")

# Cargar cosas
model = load_model(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)

prices = pd.read_csv(DATA_PATH)
if "price" not in prices.columns:
    raise SystemExit("El archivo de datos no contiene la columna 'price'.")

last_prices = prices["price"].values[-SEQ_LENGTH:]
if len(last_prices) < SEQ_LENGTH:
    raise SystemExit(f"No hay suficientes datos: se necesitan {SEQ_LENGTH} muestras, hay {len(last_prices)}.")

# Escalar y preparar input
scaled = scaler.transform(last_prices.reshape(-1, 1))
X = scaled.reshape(1, SEQ_LENGTH, 1)

# Predecir y desescalar
pred = model.predict(X)
pred_value = scaler.inverse_transform(pred.reshape(-1, 1)).flatten()[0]

print(f"Predicción para el siguiente paso: {pred_value:.2f}")
