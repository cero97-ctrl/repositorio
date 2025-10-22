# trading-v6.py (versión con GESTIÓN DE RIESGO Opción A + Módulo Wyckoff simplificado)

import pandas as pd
import logging
import os
import sys
from datetime import datetime
import common_utils as utils

# === CONFIRMACIÓN ===
def check_confirmation(df, state, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")

    message = ""
    trade_type = None
    
    if pattern == "hammer" and latest['close'] > previous['high']:
        trade_type = 'long'
        signal_text = f"✅ Confirmación alcista para patrón {pattern.upper()}"
    elif pattern == "shooting_star" and latest['close'] < previous['low']:
        trade_type = 'short'
        signal_text = f"✅ Confirmación bajista para patrón {pattern.upper()}"
    else:
        message = f"❌ Sin confirmación para el patrón {pattern.upper()}"

    utils.clear_state()
    if trade_type:
        entry, stop, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], trade_type, args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        message = utils.format_risk_management_message(signal_text, entry, stop, tp, rr)
        utils.record_trade(args.trades_log_file, args.symbol, f'CONFIRMED_{trade_type.upper()}_{pattern.upper()}', entry, stop, tp, latest['ATR'], rr, notes=f'confirmation_for_{pattern}', timestamp=latest['timestamp'])
    return message

# === MÓDULO WYCKOFF SIMPLIFICADO ===
def detect_wyckoff_event(df, args):
    """Detecta Spring (long) o Upthrust (short) en la última vela según reglas simplificadas.
    Retorna (event_type, message, entry, stop, tp, atr, rr) o (None, None, ...).
    """
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    atr_mean = utils.get_atr_mean_for_volatility(df)

    # Condiciones comunes
    vol_ok = latest['volume'] > latest['volume_sma'] * args.wyckoff_volume_mult
    atr_ok = False
    if pd.notna(latest['ATR']) and pd.notna(atr_mean) and atr_mean > 0:
        atr_ok = latest['ATR'] > atr_mean * args.wyckoff_atr_thresh

    # SPRING: falsa ruptura a la baja seguida de recuperación
    if latest['low'] < previous['low'] and latest['close'] > previous['close'] and vol_ok and atr_ok:
        entry, stop, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        msg = f"🌱 SPRING detectado | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += "- Falsa ruptura por debajo del mínimo previo y recuperación\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if entry and stop and tp:
            msg = utils.format_risk_management_message(msg, entry, stop, tp, rr)
        return 'SPRING', msg, entry, stop, tp, latest['ATR'], rr

    # UPTHRUST: falsa ruptura al alza seguida de rechazo
    if latest['high'] > previous['high'] and latest['close'] < previous['close'] and vol_ok and atr_ok:
        entry, stop, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        msg = f"🏔️ UPTHRUST detectado | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += "- Falsa ruptura por encima del máximo previo y rechazo\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if entry and stop and tp:
            msg = utils.format_risk_management_message(msg, entry, stop, tp, rr)
        return 'UPTHRUST', msg, entry, stop, tp, latest['ATR'], rr

    # Asegurarse de devolver 7 valores, incluso si son None
    return None, None, None, None, None, None, None 


# === EVALUACIÓN DE SEÑALES (con gestión de riesgo y Wyckoff) ===
def evaluate_trade(df, args, log_file=None):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    atr_mean = utils.get_atr_mean_for_volatility(df)
    if pd.isna(latest['ATR']) or pd.isna(atr_mean) or latest['ATR'] < atr_mean:
        logging.info("ATR bajo: mercado sin volatilidad significativa, no se generan señales.")
        return "⏳ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = utils.check_poc_zone(latest, args.poc)

    # Patrones clásicos
    if utils.is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier):
        entry, stop_loss, take_profit, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        signal_text = f"🕯️ Martillo detectado | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        if latest['close'] <= latest['Boll_Lower']:
            signal_text += "- Tocando banda inferior de Bollinger 📉\n"
        if poc_zone:
            signal_text += f"- En ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥\n"
        if latest['ATR'] > atr_mean:
            signal_text += "- Alta volatilidad 🔥\n"
        if entry and stop_loss and take_profit:
            signal_text = utils.format_risk_management_message(signal_text, entry, stop_loss, take_profit, rr)
        signals.append(signal_text)
        utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'LONG_HAMMER', entry, stop_loss, take_profit, latest['ATR'], rr, notes='hammer', timestamp=latest['timestamp'])
        pending_state = {"pattern": "hammer", "price": entry}

    if utils.is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier):
        entry, stop_loss, take_profit, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        signal_text = f"🕯️ Estrella Fugaz detectada | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        if latest['close'] >= latest['Boll_Upper']:
            signal_text += "- Tocando banda superior de Bollinger 📈\n"
        if poc_zone:
            signal_text += f"- En ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️\n"
        if latest['ATR'] > atr_mean:
            signal_text += "- Alta volatilidad ⚡\n"
        if entry and stop_loss and take_profit:
            signal_text = utils.format_risk_management_message(signal_text, entry, stop_loss, take_profit, rr)
        signals.append(signal_text)
        utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'SHORT_SHOOTINGSTAR', entry, stop_loss, take_profit, latest['ATR'], rr, notes='shooting_star', timestamp=latest['timestamp'])
        pending_state = {"pattern": "shooting_star", "price": entry}

    # Wyckoff events (opcional)
    if args.wyckoff:
        ev_type, ev_msg, entry, stop, tp, ev_atr, ev_rr = detect_wyckoff_event(df, args)
        if ev_type == 'SPRING':
            signals.append(ev_msg) # ev_msg ya viene formateado con la gestión de riesgo
            utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'LONG_WYCKOFF_SPRING', entry, stop, tp, ev_atr, ev_rr, notes='wyckoff_spring', timestamp=latest['timestamp'])
            pending_state = {"pattern": "wyckoff_spring", "price": entry}
        elif ev_type == 'UPTHRUST':
            signals.append(ev_msg) # ev_msg ya viene formateado con la gestión de riesgo
            utils.record_trade(log_file if log_file else args.trades_log_file, args.symbol, 'SHORT_WYCKOFF_UPTHRUST', entry, stop, tp, ev_atr, ev_rr, notes='wyckoff_upthrust', timestamp=latest['timestamp'])
            pending_state = {"pattern": "wyckoff_upthrust", "price": entry}

    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(latest['timestamp'], unit='ms').isoformat()
        utils.save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    return "\n".join(signals) if signals else "⏳ Sin señales claras."

# === MODO BACKTESTING ===
def run_backtest(args):
    logging.info(f"Iniciando backtest con simulación de trades desde: {args.backtest_file}")
    try:
        df = pd.read_csv(args.backtest_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    except Exception as e:
        logging.error(f"Error al cargar CSV: {e}")
        return

    if not {'timestamp', 'open', 'high', 'low', 'close', 'volume'}.issubset(df.columns):
        logging.error("El CSV debe contener columnas: timestamp, open, high, low, close, volume")
        return

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
    # --- MEJORA: Usar un archivo de log específico para el backtest ---
    backtest_log_file = args.trades_log_file.replace('.csv', '_backtest.csv')
    
    # Limpiar el log de trades antes de empezar un nuevo backtest
    if os.path.exists(backtest_log_file):
        os.remove(backtest_log_file)
    utils.ensure_trades_log_exists(backtest_log_file)

    logging.info("Recorriendo velas para encontrar y registrar señales...")
    for i in range(201, len(df)): # Empezamos más tarde para asegurar que todos los indicadores están maduros
        sub_df = df.iloc[:i].copy()
        # La función evaluate_trade ya se encarga de registrar los trades
        evaluate_trade(sub_df, args, backtest_log_file) # Pasamos el nombre del archivo de log

    # --- CORRECCIÓN CRÍTICA: Ordenar el log de backtest por fecha ---
    # Esto es esencial para que la simulación posterior sea cronológicamente correcta.
    logging.info(f"Ordenando el archivo de log de backtest '{backtest_log_file}' por fecha...")
    try:
        log_df = pd.read_csv(backtest_log_file)
        log_df['timestamp'] = pd.to_datetime(log_df['timestamp'])
        log_df.sort_values(by='timestamp', inplace=True)
        log_df.to_csv(backtest_log_file, index=False)
    except Exception as e:
        logging.error(f"No se pudo ordenar el archivo de log de backtest: {e}")

    logging.info(f"\n✅ Backtest de detección de señales completado. Se encontraron trades en '{backtest_log_file}'.")

# === EJECUCIÓN ===
# === MAIN ===
if __name__ == "__main__":
    args, telegram_token, chat_id = utils.setup_logging_and_config()

    logging.info("==========================================")
    logging.info("Iniciando bot Wyckoff con la siguiente configuración:")
    logging.info(f"Símbolo: {args.symbol} | Intervalo: {args.interval}")
    logging.info(f"POC: {args.poc} | ATR Window: {args.atr_window} | Bollinger Window: {args.bollinger_window}")
    logging.info(f"Volumen SMA Period: {args.volume_sma_period}")
    logging.info(f"Risk Stop Mult: {args.risk_stop_mult} | Risk TP Mult: {args.risk_tp_mult} | SL Buffer: {args.sl_buffer}")
    logging.info(f"Wyckoff activo: {args.wyckoff} | Vol Mult: {args.wyckoff_volume_mult} | ATR Thresh: {args.wyckoff_atr_thresh}")
    logging.info(f"Trades Log File: {args.trades_log_file}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    utils.clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    startup_message = (
        f"🚀 *Bot Wyckoff Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"POC configurado en: `{args.poc}`\n"
        f"Wyckoff activado: `{'Sí' if args.wyckoff else 'No'}`\n\n"
        "El bot está en línea y funcionando correctamente\\."
    )
    utils.run_bot_main_loop(args, telegram_token, chat_id, evaluate_trade, check_confirmation, startup_message)
