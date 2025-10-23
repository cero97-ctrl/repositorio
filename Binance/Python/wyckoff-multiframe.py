# wyckoff-multiframe.py — (Modo profesional Wyckoff multi-timeframe + Gestión de riesgo básica)
# Indicadores: EMA50/200, ATR, Volumen SMA.
# Parametros: POC (Point of Control), Backtesting, dotenv

import pandas as pd
import numpy as np
import time
import logging
import os
import sys
import argparse
from datetime import datetime
import common_utils as utils

# -----------------------------
# CONFIRMACIÓN
# -----------------------------
def check_confirmation(df, state, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")
    signal_time_str = state.get("signal_time")

    message = ""
    trade_type = None

    # Ensure we are confirming the *next* candle after the signal
    if signal_time_str:
        signal_time = pd.to_datetime(signal_time_str)
        if pd.to_datetime(latest['timestamp'], unit='ms') <= signal_time:
            return f"⏳ Esperando vela de confirmación para el patrón {pattern.upper()}..."
    
    sl_buffer_val = getattr(args, 'sl_buffer', 0.0)

    if pattern == "hammer" and latest['close'] > previous['high']:
        trade_type = 'long'
        signal_text = f"✅ Confirmación alcista para patrón {pattern.upper()}"
    elif pattern == "shooting_star" and latest['close'] < previous['low']:
        trade_type = 'short'
        signal_text = f"✅ Confirmación bajista para patrón {pattern.upper()}"
    elif pattern == "wyckoff_spring" and latest['close'] > previous['high']:
        trade_type = 'long'
        signal_text = f"✅ Confirmación alcista para SPRING"
    elif pattern == "wyckoff_upthrust" and latest['close'] < previous['low']:
        trade_type = 'short'
        signal_text = f"✅ Confirmación bajista para UPTHRUST"
    else:
        message = f"❌ Sin confirmación para el patrón {pattern.upper()}"

    utils.clear_state() # Clear state regardless of confirmation outcome
    if trade_type:
        entry_price, stop, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], trade_type, args.risk_stop_mult, args.risk_tp_mult, sl_buffer_val)
        message = utils.format_risk_management_message(signal_text, entry_price, stop, tp, rr)
        utils.record_trade(args.trades_log_file, args.symbol, f'CONFIRMED_{trade_type.upper()}_{pattern.upper()}', entry_price, stop, tp, latest['ATR'], rr, notes=f'confirmation_for_{pattern}', timestamp=latest['timestamp'])
    return message

# -----------------------------
# MÓDULO WYCKOFF (LTF events) + MTF HTF confirmation
# -----------------------------
def detect_wyckoff_event(df, args, sl_buffer_val):
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    vol_ok = latest['volume'] > latest['volume_sma'] * args.wyckoff_volume_mult
    atr_mean = utils.get_atr_mean_for_volatility(df)
    atr_ok = False
    if pd.notna(latest['ATR']) and pd.notna(atr_mean) and atr_mean > 0:
        atr_ok = latest['ATR'] > atr_mean * args.wyckoff_atr_thresh

    # SPRING
    if latest['low'] < previous['low'] and latest['close'] > previous['close'] and vol_ok and atr_ok:
        entry = latest['close']
        entry_price, stop, tp, rr = utils.compute_risk_levels(entry, latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, sl_buffer_val)
        msg = f"🌱 SPRING detectado | Entrada: {entry_price:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if stop and tp:
            msg = utils.format_risk_management_message(msg, entry_price, stop, tp, rr)
        return 'SPRING', msg, entry_price, stop, tp, latest['ATR'], rr

    # UPTHRUST
    if latest['high'] > previous['high'] and latest['close'] < previous['close'] and vol_ok and atr_ok:
        entry = latest['close']
        entry_price, stop, tp, rr = utils.compute_risk_levels(entry, latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, sl_buffer_val)
        msg = f"🏔️ UPTHRUST detectado | Entrada: {entry_price:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if stop and tp:
            msg = utils.format_risk_management_message(msg, entry_price, stop, tp, rr)
        return 'UPTHRUST', msg, entry_price, stop, tp, latest['ATR'], rr

    return None, None, None, None, None, None, None


def detect_htf_phase(df_htf, args):
    if df_htf.empty or len(df_htf) < 20:
        return 'range_unknown'
    
    # Re-implementing this logic to be more robust or use common_utils if available
    # For now, keeping the original logic but ensuring it works with the df_htf
    # This function is not in common_utils, so it remains here.
    
    # Simplified phase detection (can be improved)
    # Check if price is trending or ranging
    if df_htf['EMA_50'].iloc[-1] > df_htf['EMA_200'].iloc[-1] and df_htf['EMA_50'].iloc[-2] < df_htf['EMA_200'].iloc[-2]:
        return 'accumulation' # Golden Cross
    elif df_htf['EMA_50'].iloc[-1] < df_htf['EMA_200'].iloc[-1] and df_htf['EMA_50'].iloc[-2] > df_htf['EMA_200'].iloc[-2]:
        return 'distribution' # Death Cross
    elif df_htf['EMA_50'].iloc[-1] > df_htf['EMA_200'].iloc[-1]:
        return 'markup' # Uptrend
    elif df_htf['EMA_50'].iloc[-1] < df_htf['EMA_200'].iloc[-1]:
        return 'markdown' # Downtrend
    else:
        return 'range_unknown' # Sideways

# -----------------------------
# EVALUACIÓN DE SEÑALES (principal)
# -----------------------------
def evaluate_trade(df, args, log_file=None):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    atr_mean = utils.get_atr_mean_for_volatility(df)
    if pd.isna(latest['ATR']) or pd.isna(atr_mean) or latest['ATR'] < atr_mean:
        logging.info("ATR bajo: mercado sin volatilidad significativa, no se generan señales.")
        return "⚠️ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = utils.check_poc_zone(latest, args.poc)
    sl_buffer_val = getattr(args, 'sl_buffer', 0.0)

    # Detect Wyckoff event in LTF
    ev_type, ev_msg, entry_wyckoff, stop_wyckoff, tp_wyckoff, ev_atr, ev_rr = detect_wyckoff_event(df, args, sl_buffer_val)

    # HTF confirmation when professional mode active
    htf_phase = None
    if args.wyckoff_professional:
        df_htf = utils.get_klines(args.symbol, args.htf_interval, args.htf_limit)
        if df_htf.empty:
            logging.warning("No se pudieron obtener datos HTF para confirmación Wyckoff profesional.")
        else:
            df_htf = utils.calculate_indicators(df_htf, args.volume_sma_period, args.atr_window, getattr(args, 'bollinger_window', 20))
            htf_phase = detect_htf_phase(df_htf, args)
            logging.info(f"HTF phase detected: {htf_phase}")

    # Hammer
    if utils.is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier):
        entry_p, sl, tp_p, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, sl_buffer_val)
        signal_text = f"🕯️ Martillo detectado | Entrada: {entry_p:.8f} | ATR: {latest['ATR']:.8f}\n"
        if poc_zone:
            signal_text += f"- En ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥\n"
        if latest['ATR'] > atr_mean:
            signal_text += "- Alta volatilidad 🔥\n"
        if sl and tp_p:
            signal_text = utils.format_risk_management_message(signal_text, entry_p, sl, tp_p, rr)
        if args.wyckoff_professional:
            if htf_phase in ('accumulation', 'markup'):
                signals.append(signal_text)
                utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'LONG', entry_p, sl, tp_p, latest['ATR'], rr, notes='hammer', timestamp=latest['timestamp'])
                pending_state = {"pattern": "hammer", "price": entry_p}
            else:
                logging.info(f"Martillo detectado pero HTF no compatible ({htf_phase}), descartado en modo profesional.")
        else:
            signals.append(signal_text)
            utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'LONG', entry_p, sl, tp_p, latest['ATR'], rr, notes='hammer', timestamp=latest['timestamp'])
            pending_state = {"pattern": "hammer", "price": entry_p}

    # Shooting star
    if utils.is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier):
        entry_p, sl, tp_p, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, sl_buffer_val)
        signal_text = f"🕯️ Estrella Fugaz detectada | Entrada: {entry_p:.8f} | ATR: {latest['ATR']:.8f}\n"
        if poc_zone:
            signal_text += f"- En ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️\n"
        if latest['ATR'] > atr_mean:
            signal_text += "- Alta volatilidad ⚡\n"
        if sl and tp_p:
            signal_text = utils.format_risk_management_message(signal_text, entry_p, sl, tp_p, rr)
        if args.wyckoff_professional:
            if htf_phase in ('distribution', 'markdown'):
                signals.append(signal_text)
                utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'SHORT', entry_p, sl, tp_p, latest['ATR'], rr, notes='shooting_star', timestamp=latest['timestamp'])
                pending_state = {"pattern": "shooting_star", "price": entry_p}
            else:
                logging.info(f"Estrella Fugaz detectada pero HTF no compatible ({htf_phase}), descartado en modo profesional.")
        else:
            signals.append(signal_text)
            utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'SHORT', entry_p, sl, tp_p, latest['ATR'], rr, notes='shooting_star', timestamp=latest['timestamp'])
            pending_state = {"pattern": "shooting_star", "price": entry_p}

    # Wyckoff LTF events
    if ev_type:
        if args.wyckoff_professional:
            if ev_type == 'SPRING' and htf_phase in ('accumulation', 'markup'):
                signals.append(ev_msg)
                utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'LONG_WYCKOFF', entry_wyckoff, stop_wyckoff, tp_wyckoff, ev_atr, ev_rr, notes='wyckoff_spring', timestamp=latest['timestamp'])
                pending_state = {"pattern": "wyckoff_spring", "price": entry_wyckoff}
            elif ev_type == 'UPTHRUST' and htf_phase in ('distribution', 'markdown'):
                signals.append(ev_msg)
                utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'SHORT_WYCKOFF', entry_wyckoff, stop_wyckoff, tp_wyckoff, ev_atr, ev_rr, notes='wyckoff_upthrust', timestamp=latest['timestamp'])
                pending_state = {"pattern": "wyckoff_upthrust", "price": entry_wyckoff}
            else:
                logging.info(f"Wyckoff event {ev_type} detectado pero HTF no compatible ({htf_phase}), descartado en modo profesional.")
        else:
            signals.append(ev_msg)
            utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, f'{ev_type}_WYCKOFF', entry_wyckoff, stop_wyckoff, tp_wyckoff, ev_atr, ev_rr, notes='wyckoff_event', timestamp=latest['timestamp'])
            pending_state = {"pattern": f'wyckoff_{ev_type.lower()}', "price": entry_wyckoff}

    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(latest['timestamp'], unit='ms').isoformat()
        utils.save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    return "\n".join(signals) if signals else "⏳ Sin señales claras."

# -----------------------------
# BACKTEST
# -----------------------------
def run_backtest(args):
    logging.info(f"Iniciando backtest con datos: {args.backtest_file}")
    try:
        df = pd.read_csv(args.backtest_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    except Exception as e:
        logging.error(f"Error al cargar CSV: {e}")
        return

    if not {'timestamp', 'open', 'high', 'low', 'close', 'volume'}.issubset(df.columns):
        logging.error("El CSV debe contener columnas: timestamp, open, high, low, close, volume")
        return

    # --- CORRECCIÓN: Respetar el límite de velas para el backtest ---
    if args.limit > 0 and len(df) > args.limit:
        logging.info(f"El archivo de datos tiene {len(df)} velas. Usando las últimas {args.limit} para el backtest.")
        df = df.tail(args.limit).reset_index(drop=True)

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, getattr(args, 'bollinger_window', 20))
    
    backtest_log_file = args.trades_log_file.replace('.csv', '_backtest.csv')
    
    if os.path.exists(backtest_log_file):
        os.remove(backtest_log_file)
    utils.ensure_trades_log_exists(backtest_log_file)

    logging.info("Recorriendo velas para encontrar y registrar señales...")
    start_index = max(201, args.atr_window, args.volume_sma_period)

    utils.clear_state() # Ensure a clean state at the beginning of backtest

    for i in range(start_index, len(df)):
        sub_df = df.iloc[:i+1].copy()
        
        # Simulate the common_utils.execute_single_run_common logic
        pending_state = utils.load_state()
        signal_message = ""

        if pending_state:
            # If there's a pending state, try to confirm it
            signal_message = check_confirmation(sub_df, pending_state, args)
            # check_confirmation will clear the state if it confirms or fails to confirm
        else:
            # If no pending state, evaluate for new signals
            # evaluate_trade will save state if a pending signal is found
            signal_message = evaluate_trade(sub_df, args, backtest_log_file)
        
        if signal_message and "⏳ Sin señales claras." not in signal_message and "⚠️ Volatilidad baja" not in signal_message:
            logging.info(f"[{i}] {signal_message}")

    # --- CORRECCIÓN CRÍTICA: Ordenar el log de backtest por fecha ---
    logging.info(f"Ordenando el archivo de log de backtest '{backtest_log_file}' por fecha...")
    try:
        log_df = pd.read_csv(backtest_log_file)
        log_df['timestamp'] = pd.to_datetime(log_df['timestamp'])
        log_df.sort_values(by='timestamp', inplace=True)
        log_df.to_csv(backtest_log_file, index=False)
    except Exception as e:
        logging.error(f"No se pudo ordenar el archivo de log de backtest: {e}")

    logging.info(f"\n✅ Backtest de detección de señales completado. Se encontraron trades en '{backtest_log_file}'.")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    # Usar la función de configuración común
    args, telegram_token, chat_id = utils.setup_logging_and_config()

    # Añadir parámetros específicos de este bot que no están en common_utils.load_config
    # Estos deben ser definidos en common_utils.load_config para una consistencia total
    # Pero si no lo están, los cargamos aquí como fallback o específicos.
    # Para este ejercicio, asumimos que common_utils.load_config ya maneja la mayoría.
    # Sin embargo, los parámetros de Wyckoff profesional son específicos de este script.
    parser = argparse.ArgumentParser(description="Bot Wyckoff profesional multi-timeframe + gestión de riesgo básica")
    # Añadir argumentos que puedan faltar en common_utils.load_config o sean específicos
    parser.add_argument("--wyckoff", action='store_true', default=str(os.getenv('WYCKOFF', 'false')).lower() in ('true', '1', 't'))
    parser.add_argument("--wyckoff-volume-mult", type=float, default=float(os.getenv('WYCKOFF_VOLUME_MULT', 1.3)))
    parser.add_argument("--wyckoff-atr-thresh", type=float, default=float(os.getenv('WYCKOFF_ATR_THRESH', 1.1)))
    parser.add_argument("--wyckoff-professional", action='store_true', default=str(os.getenv('WYCKOFF_PROFESSIONAL', 'false')).lower() in ('true', '1', 't'))
    parser.add_argument("--htf-interval", type=str, default=os.getenv('HTF_INTERVAL', "4h"))
    parser.add_argument("--htf-limit", type=int, default=int(os.getenv('HTF_LIMIT', 120)))
    parser.add_argument("--htf-range-thresh", type=float, default=float(os.getenv('HTF_RANGE_THRESH', 0.03)))
    parser.add_argument("--htf-vol-mult", type=float, default=float(os.getenv('HTF_VOL_MULT', 1.2)))
    parser.add_argument("--htf-slope-thresh", type=float, default=float(os.getenv('HTF_SLOPE_THRESH', 0.0005)))
    
    # Parsear solo los argumentos que no fueron manejados por common_utils.load_config
    # Esto es un poco hacky, la mejor solución sería centralizar *todos* los args en common_utils.load_config
    # Para este ejercicio, vamos a actualizar el objeto args existente con los específicos.
    known_args, _ = parser.parse_known_args()
    for arg_name in vars(known_args):
        if not hasattr(args, arg_name): # Solo añadir si no existe ya
            setattr(args, arg_name, getattr(known_args, arg_name))

    # Asegurarse de que bollinger_window y sl_buffer estén en args, ya que common_utils los espera
    # common_utils.load_config ya los carga, pero si este script se ejecuta solo, podrían faltar.
    # Para este ejercicio, asumimos que common_utils.load_config ya los proporciona.
    # Si no, se deberían añadir a common_utils.load_config o aquí como fallback.
    if not hasattr(args, 'bollinger_window'):
        args.bollinger_window = int(os.getenv('BOLLINGER_WINDOW', 20))
    if not hasattr(args, 'sl_buffer'):
        args.sl_buffer = float(os.getenv('SL_BUFFER', 0.002))

    logging.info("==========================================")
    logging.info(f"ATR Window: {args.atr_window}")
    logging.info(f"Volumen SMA Period: {args.volume_sma_period}")
    logging.info(f"POC: {args.poc}")
    logging.info(f"Símbolo: {args.symbol}")
    logging.info(f"Intervalo: {args.interval}")
    logging.info(f"Risk stop mult: {args.risk_stop_mult} | Risk TP mult: {args.risk_tp_mult} | SL Buffer: {args.sl_buffer}")
    logging.info(f"Trades log file: {args.trades_log_file}")
    logging.info(f"Wyckoff active: {args.wyckoff} | vol mult: {args.wyckoff_volume_mult} | atr thresh: {args.wyckoff_atr_thresh}")
    logging.info(f"Wyckoff PROFESSIONAL: {args.wyckoff_professional} | HTF: {args.htf_interval} | HTF limit: {args.htf_limit}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    utils.clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    startup_message = (
        f"🚀 *Bot Wyckoff Multi\\-Timeframe Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"POC configurado en: `{args.poc}`\n"
        f"Wyckoff activado: `{'Sí' if args.wyckoff else 'No'}`\n"
        f"Modo profesional Wyckoff: `{'Sí' if args.wyckoff_professional else 'No'}`\n\n"
        "El bot está en línea y funcionando correctamente\\."
    )

    utils.run_bot_main_loop(args, telegram_token, chat_id, evaluate_trade, check_confirmation, startup_message)
