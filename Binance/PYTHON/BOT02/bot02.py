import pandas as pd
import numpy as np
from telegram import Bot
import logging
import time
import os
import json

# --- Configuración de Logging ---
logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")

# === CONFIGURACIÓN ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "TU_TOKEN_AQUI")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "TU_CHAT_ID_AQUI")
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
RANGE_LOOKBACK = 50
ATR_PERIOD = 14
ABSORPTION_RATIO = 0.3
REJECTION_THRESHOLD = 0.7
VOLUME_AVG_PERIOD = 20
STATE_FILE = "bot02_state.json"

bot = Bot(token=TELEGRAM_TOKEN)


# --- MEJORA: Gestión de Estado ---
def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"last_alert": None, "in_range": False}
    with open(STATE_FILE, "r") as f:
        return json.load(f)


# === FUNCIONES DE ALERTA ===
def send_telegram_alert(message):
    try:
        bot.send_message(chat_id=CHAT_ID, text=message)
        logging.info(f"Alerta enviada a Telegram: {message}")
    except Exception as e:
        logging.error(f"Error al enviar alerta a Telegram: {e}")


# === FUNCIONES DE ANÁLISIS ===
def calculate_atr(df, period=ATR_PERIOD):
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def detect_range(df, atr_multiplier=2.0):
    recent = df[-RANGE_LOOKBACK:]
    max_high = recent["high"].max()
    min_low = recent["low"].min()
    range_size = max_high - min_low

    # Usar el ATR ya calculado en el DataFrame principal
    atr = df["atr"].iloc[-1]

    if range_size > 0 and range_size < atr * atr_multiplier:
        return True, min_low, max_high
    return False, None, None


def detect_absorption(df, level, direction="support"):
    recent = df[-5:]
    signals = []
    for i, row in recent.iterrows():
        body = abs(row["close"] - row["open"])
        lower_wick = (
            row["open"] - row["low"]
            if row["close"] > row["open"]
            else row["close"] - row["low"]
        )
        upper_wick = (
            row["high"] - row["close"]
            if row["close"] > row["open"]
            else row["high"] - row["open"]
        )
        if (
            direction == "support"
            and row["low"] <= level
            and lower_wick > 0
            and body / lower_wick < ABSORPTION_RATIO
        ):
            signals.append((i, "Absorción en soporte"))
        elif (
            direction == "resistance"
            and row["high"] >= level
            and upper_wick > 0
            and body / upper_wick < ABSORPTION_RATIO
        ):
            signals.append((i, "Absorción en resistencia"))
    return signals


def detect_rejection(df, level, direction="resistance"):
    recent = df[-5:]
    signals = []
    for i, row in recent.iterrows():
        upper_wick = row["high"] - max(row["open"], row["close"])
        lower_wick = min(row["open"], row["close"]) - row["low"]
        total_range = row["high"] - row["low"]
        if (
            total_range > 0
            and direction == "resistance"
            and row["high"] >= level
            and upper_wick / total_range > REJECTION_THRESHOLD
        ):
            signals.append((i, "Rechazo en resistencia"))
        elif (
            total_range > 0
            and direction == "support"
            and row["low"] <= level
            and lower_wick / total_range > REJECTION_THRESHOLD
        ):
            signals.append((i, "Rechazo en soporte"))
    return signals


# === EJECUCIÓN DE ÓRDENES (simulada) ===
def execute_trade(direction, price):
    msg = f"🟢 Ejecutando orden {direction.upper()} en {price:.2f}"
    logging.info(msg)
    send_telegram_alert(msg)
    # Aquí iría la lógica real con Binance API


# === FLUJO PRINCIPAL ===
def main_flow(df):
    state = load_state()

    # 1. Detección de Rango
    in_range, low, high = detect_range(df)

    # Si el estado del rango ha cambiado, notificar y actualizar estado.
    if in_range and not state.get("in_range"):
        msg = f"🔔 RANGO DETECTADO para {SYMBOL} entre {low:.2f} y {high:.2f}"
        send_telegram_alert(msg)
        state["in_range"] = True
        state["range_low"] = low
        state["range_high"] = high
        save_state(state)
    elif not in_range and state.get("in_range"):
        msg = f"🔔 RANGO ROTO en {SYMBOL}. Volviendo a modo de búsqueda."
        send_telegram_alert(msg)
        state["in_range"] = False
        save_state(state)

    # 2. Si estamos en un rango, buscar señales en los límites
    if state.get("in_range"):
        low, high = state["range_low"], state["range_high"]
        absorptions = detect_absorption(df, low, "support") + detect_absorption(
            df, high, "resistance"
        )
        rejections = detect_rejection(df, high, "resistance") + detect_rejection(
            df, low, "support"
        )
        signals = absorptions + rejections

        for idx, alert in signals:
            # MEJORA: Evitar alertas duplicadas
            alert_id = f"{idx}-{alert}"
            if state.get("last_alert") != alert_id:
                msg = f"🧠 Alerta para {SYMBOL}: {alert} en vela de las {df.loc[idx, 'timestamp']}"
                send_telegram_alert(msg)
                state["last_alert"] = alert_id
                save_state(state)

    # 3. Comprobar rupturas (independientemente de si el rango se acaba de romper)
    last = df.iloc[-1]
    avg_volume = df["volume_sma"].iloc[-1]

    # Usar los límites del estado para consistencia
    if state.get("in_range"):
        high_level = state["range_high"]
        low_level = state["range_low"]

        if last["close"] > high_level and last["volume"] > avg_volume:
            msg = f"🚀 Ruptura alcista con volumen en {last['close']:.2f}"
            send_telegram_alert(msg)
            execute_trade("long", last["close"])
            state["in_range"] = False  # La ruptura termina el rango
            save_state(state)
        elif last["close"] < low_level and last["volume"] > avg_volume:
            msg = f"⚠️ Ruptura bajista con volumen en {last['close']:.2f}"
            send_telegram_alert(msg)
            execute_trade("short", last["close"])
            state["in_range"] = False  # La ruptura termina el rango
            save_state(state)


if __name__ == "__main__":
    logging.info(f"Iniciando Bot02 para el símbolo {SYMBOL} en intervalo {INTERVAL}")
    send_telegram_alert(f"🚀 Bot de Rangos (Bot02) iniciado para {SYMBOL}.")

    # --- MEJORA: Bucle principal para operación en vivo ---
    # Esta estructura es un ejemplo. Se puede integrar con common_utils.
    from common_utils import get_klines, interval_to_ms

    while True:
        try:
            logging.info("Obteniendo nuevos datos de velas...")
            # Pedimos más velas de las necesarias para el calentamiento de indicadores
            df = get_klines(
                SYMBOL, INTERVAL, limit=RANGE_LOOKBACK + ATR_PERIOD + VOLUME_AVG_PERIOD
            )

            if not df.empty:
                # Calcular indicadores necesarios
                df["atr"] = calculate_atr(df, ATR_PERIOD)
                df["volume_sma"] = df["volume"].rolling(window=VOLUME_AVG_PERIOD).mean()
                df.dropna(inplace=True)  # Eliminar filas sin datos de indicadores

                if not df.empty:
                    main_flow(df)
                else:
                    logging.warning("DataFrame vacío después de calcular indicadores.")
            else:
                logging.warning("No se pudieron obtener datos de Binance.")

            sleep_time = interval_to_ms(INTERVAL) / 1000
            logging.info(
                f"Ciclo completado. Esperando {sleep_time} segundos para la próxima vela..."
            )
            time.sleep(sleep_time)

        except Exception as e:
            logging.error(f"Ocurrió un error en el bucle principal: {e}")
            time.sleep(
                60
            )  # Esperar un minuto antes de reintentar en caso de error grave
