# range-breakout-v1.py (Bot enfocado en la detección y ruptura de rangos)

import pandas as pd
import numpy as np
import logging
import sys
import os

import common_utils as utils

# === FUNCIONES DE ANÁLISIS DE RANGO (Adaptadas de bot02.py) ===

def detect_range(df, lookback_period, atr_multiplier):
    """Detecta si el mercado está en un rango."""
    recent = df.iloc[-lookback_period:]
    max_high = recent['high'].max()
    min_low = recent['low'].min()
    range_size = max_high - min_low
    atr = df['ATR'].iloc[-1]

    if range_size > 0 and range_size < atr * atr_multiplier:
        return True, min_low, max_high
    return False, None, None

def detect_absorption(df, level, direction, ratio):
    """Detecta velas de absorción en un nivel."""
    # Solo analizamos la última vela para evitar alertas repetidas
    row = df.iloc[-1]
    body = abs(row['close'] - row['open'])
    lower_wick = row['open'] - row['low'] if row['close'] > row['open'] else row['close'] - row['low']
    upper_wick = row['high'] - row['close'] if row['close'] > row['open'] else row['high'] - row['open']
    
    if direction == 'support' and row['low'] <= level and lower_wick > 0 and body / lower_wick < ratio:
        return f"Absorción en soporte ({level:.2f})"
    elif direction == 'resistance' and row['high'] >= level and upper_wick > 0 and body / upper_wick < ratio:
        return f"Absorción en resistencia ({level:.2f})"
    return None

def detect_rejection(df, level, direction, threshold):
    """Detecta velas de rechazo en un nivel."""
    row = df.iloc[-1]
    upper_wick = row['high'] - max(row['open'], row['close'])
    lower_wick = min(row['open'], row['close']) - row['low']
    total_range = row['high'] - row['low']

    if total_range > 0:
        if direction == 'resistance' and row['high'] >= level and upper_wick / total_range > threshold:
            return f"Rechazo en resistencia ({level:.2f})"
        elif direction == 'support' and row['low'] <= level and lower_wick / total_range > threshold:
            return f"Rechazo en soporte ({level:.2f})"
    return None

# === EVALUACIÓN DE SEÑALES ===
def evaluate_trade(df, args, log_file=None):
    # --- CORRECCIÓN CRÍTICA: Inicializar el estado si no existe ---
    # Si load_state() devuelve None, lo convertimos en un diccionario vacío.
    state = utils.load_state() or {}
    signals = []

    # 1. Detección de Rango
    in_range, low, high = detect_range(df, args.range_lookback, args.range_atr_multiplier)

    # Si el estado del rango ha cambiado, notificar y actualizar estado.
    if in_range and not state.get('in_range'):
        msg = f"🔔 RANGO DETECTADO para {args.symbol} entre {low:.2f} y {high:.2f}"
        signals.append(msg)
        state['in_range'] = True
        state['range_low'] = low
        state['range_high'] = high
        utils.save_state(state)
    elif not in_range and state.get('in_range'):
        msg = f"🔔 RANGO ROTO en {args.symbol}. Volviendo a modo de búsqueda."
        signals.append(msg)
        state['in_range'] = False
        utils.save_state(state)

    # 2. Si estamos en un rango, buscar señales contextuales en los límites
    if state.get('in_range'):
        range_low, range_high = state['range_low'], state['range_high']
        
        # Buscar absorción/rechazo (solo para alertas, no para trades)
        absorption_signal = detect_absorption(df, range_low, 'support', args.absorption_ratio) or \
                            detect_absorption(df, range_high, 'resistance', args.absorption_ratio)
        rejection_signal = detect_rejection(df, range_high, 'resistance', args.rejection_threshold) or \
                           detect_rejection(df, range_low, 'support', args.rejection_threshold)

        context_signal = absorption_signal or rejection_signal
        if context_signal:
            alert_id = f"{df.iloc[-1]['timestamp']}-{context_signal}"
            if state.get('last_alert_id') != alert_id:
                signals.append(f"🧠 Alerta: {context_signal}")
                state['last_alert_id'] = alert_id
                utils.save_state(state)

    # 3. Comprobar rupturas (breakouts) para generar trades
    latest = df.iloc[-1]
    avg_volume = latest['volume_sma']
    
    if state.get('in_range'):
        range_high = state['range_high']
        range_low = state['range_low']

        # Ruptura alcista
        if latest['close'] > range_high and latest['volume'] > avg_volume * args.volume_multiplier:
            signal_text = f"🚀 Ruptura alcista con volumen en {latest['close']:.2f}"
            entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
            formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
            signals.append(formatted_signal)
            
            # Registrar el trade
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'LONG_BREAKOUT', entry, sl, tp, latest['ATR'], rr, notes='range_breakout_up', timestamp=latest['timestamp'])
            
            # Salir del estado de rango
            state['in_range'] = False
            utils.save_state(state)

        # Ruptura bajista
        elif latest['close'] < range_low and latest['volume'] > avg_volume * args.volume_multiplier:
            signal_text = f"⚠️ Ruptura bajista con volumen en {latest['close']:.2f}"
            entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
            formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
            signals.append(formatted_signal)

            # Registrar el trade
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'SHORT_BREAKOUT', entry, sl, tp, latest['ATR'], rr, notes='range_breakout_down', timestamp=latest['timestamp'])

            # Salir del estado de rango
            state['in_range'] = False
            utils.save_state(state)

    return "\n".join(signals) if signals else "⏳ Sin señales claras."

# === MODO BACKTESTING (Adaptado del framework) ===
def run_backtest(args):
    """
    Ejecuta el backtest para la estrategia de ruptura de rangos.
    Esta lógica es similar a la de otros bots, pero adaptada para esta estrategia.
    """
    logging.info(f"Iniciando backtest de detección de señales desde: {args.backtest_file}")
    try:
        df = pd.read_csv(args.backtest_file)
        # Asegurarse de que el timestamp se maneje como datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    except Exception as e:
        logging.error(f"Error al cargar CSV: {e}")
        return

    if not {'timestamp', 'open', 'high', 'low', 'close', 'volume'}.issubset(df.columns):
        logging.error("El CSV debe contener columnas: timestamp, open, high, low, close, volume")
        return

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
    backtest_log_file = args.trades_log_file.replace('.csv', '_backtest.csv')
    
    if os.path.exists(backtest_log_file):
        os.remove(backtest_log_file)
    utils.ensure_trades_log_exists(backtest_log_file)

    logging.info("Recorriendo velas para encontrar y registrar señales...")
    utils.clear_state() # Asegurar un estado limpio al inicio del backtest

    # Empezamos más tarde para asegurar que todos los indicadores están maduros
    start_index = max(201, args.range_lookback, args.volume_sma_period)
    for i in range(start_index, len(df)):
        sub_df = df.iloc[:i+1].copy()
        signal_message = evaluate_trade(sub_df, args, backtest_log_file)
        
        if signal_message and "⏳ Sin señales claras." not in signal_message:
            logging.info(f"[{i}] {signal_message}")

    logging.info(f"\n✅ Backtest de detección de señales completado. Se encontraron trades en '{backtest_log_file}'.")

# === MAIN ===
if __name__ == "__main__":
    # Añadir argumentos específicos para esta estrategia
    def add_strategy_args(parser):
        parser.add_argument("--range-lookback", type=int, default=50, help="Periodo para detectar el rango.")
        parser.add_argument("--range-atr-multiplier", type=float, default=2.0, help="Multiplicador de ATR para definir el ancho máximo del rango.")
        parser.add_argument("--absorption-ratio", type=float, default=0.3, help="Ratio cuerpo/mecha para detectar absorción.")
        parser.add_argument("--rejection-threshold", type=float, default=0.7, help="Umbral de mecha/rango total para detectar rechazo.")
        return parser

    args, telegram_token, chat_id = utils.setup_logging_and_config(add_strategy_args_func=add_strategy_args)

    logging.info("==========================================")
    logging.info("Iniciando bot de Ruptura de Rangos con la siguiente configuración:")
    logging.info(f"Símbolo: {args.symbol} | Intervalo: {args.interval}")
    logging.info(f"Lookback de Rango: {args.range_lookback} | Multiplicador ATR de Rango: {args.range_atr_multiplier}")
    logging.info(f"Risk Stop Mult: {args.risk_stop_mult} | Risk TP Mult: {args.risk_tp_mult}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live
    utils.clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    startup_message = (
        f"🚀 *Bot de Ruptura de Rangos Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"Estrategia: Detección de rangos y operación en rupturas con volumen."
    )

    # Esta estrategia no necesita confirmación, pasamos None
    utils.run_bot_main_loop(args, telegram_token, chat_id, evaluate_trade, check_confirmation_func=None, startup_message=startup_message)