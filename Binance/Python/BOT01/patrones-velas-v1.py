# patrones-velas.py (Bot enfocado en patrones de velas + filtro de volatilidad ATR)

import pandas as pd
import logging
import sys
import os
import common_utils as utils

# === CONFIRMACIÓN ===
def check_confirmation(df, state, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")

    message = ""

    # --- CORRECCIÓN: Usar risk_tp_mult para consistencia con otros bots y common_utils ---
    if pattern == "hammer" and latest['close'] > previous['high']:
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        message = utils.format_risk_management_message(f"✅ Confirmación alcista para patrón {pattern.upper()}", entry, sl, tp, rr)
        utils.record_trade(args.trades_log_file, args.symbol, f'CONFIRMED_LONG_{pattern.upper()}', entry, sl, tp, latest['ATR'], rr, notes=f'confirmation_for_{pattern}', timestamp=latest['timestamp'])
    elif pattern == "shooting_star" and latest['close'] < previous['low']:
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        message = utils.format_risk_management_message(f"✅ Confirmación bajista para patrón {pattern.upper()}", entry, sl, tp, rr)
        utils.record_trade(args.trades_log_file, args.symbol, f'CONFIRMED_SHORT_{pattern.upper()}', entry, sl, tp, latest['ATR'], rr, notes=f'confirmation_for_{pattern}', timestamp=latest['timestamp'])
    else:
        message = f"❌ Sin confirmación para el patrón {pattern.upper()}"

    utils.clear_state()
    return message

# === EVALUACIÓN DE SEÑALES ===
def evaluate_trade(df, args, log_file=None):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    atr_mean = utils.get_atr_mean_for_volatility(df) # Usamos 50 como en wyckoff para consistencia
    if latest['ATR'] < atr_mean:
        logging.info("ATR bajo: mercado sin volatilidad significativa, no se generan señales.")
        return "⚠️ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = utils.check_poc_zone(latest, args.poc)

    # --- ESTRATEGIA: PATRONES DE VELAS + FILTROS ---

    # --- Filtro de RSI para evitar comprar en sobrecompra o vender en sobreventa ---
    is_not_overbought = latest['RSI'] < 70
    is_not_oversold = latest['RSI'] > 30

    if utils.is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier) and is_not_overbought:
        signal_text = "🕯️ Hammer detectado"
        if latest['close'] <= latest['Boll_Lower']:
            signal_text += " tocando banda inferior de Bollinger 📉"
        if poc_zone:
            signal_text += f" en ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥"
        if latest['ATR'] > atr_mean:
            signal_text += " con alta volatilidad 🔥"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📈"
        
        # MEJORA: Entrada por retroceso al 50% de la vela de la señal
        pullback_entry = (latest['high'] + latest['low']) / 2
        entry, sl, tp, rr = utils.compute_risk_levels(pullback_entry, latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'LONG_HAMMER', entry, sl, tp, latest['ATR'], rr, notes='hammer', timestamp=latest['timestamp'])
        pending_state = {"pattern": "hammer", "price": latest['close']}

    if utils.is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier) and is_not_oversold:
        signal_text = "🕯️ Shooting Star detectado"
        if latest['close'] >= latest['Boll_Upper']:
            signal_text += " tocando banda superior de Bollinger 📈"
        if poc_zone:
            signal_text += f" en ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️"
        if latest['ATR'] > atr_mean:
            signal_text += " con fuerte volatilidad ⚡"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📉"

        # MEJORA: Entrada por retroceso al 50% de la vela de la señal
        pullback_entry = (latest['high'] + latest['low']) / 2
        entry, sl, tp, rr = utils.compute_risk_levels(pullback_entry, latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'SHORT_SHOOTINGSTAR', entry, sl, tp, latest['ATR'], rr, notes='shooting_star', timestamp=latest['timestamp'])
        pending_state = {"pattern": "shooting_star", "price": latest['close']}

    # --- NUEVO: Patrón Envolvente Alcista (Bullish Engulfing) ---
    if utils.is_bullish_engulfing(latest, previous) and is_not_overbought:
        signal_text = "🕯️ Envolvente Alcista detectado"
        if poc_zone:
            signal_text += f" en ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📈"
        
        # MEJORA: Entrada por retroceso al 50% de la vela de la señal
        pullback_entry = (latest['high'] + latest['low']) / 2
        entry, sl, tp, rr = utils.compute_risk_levels(pullback_entry, latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'LONG_ENGULFING', entry, sl, tp, latest['ATR'], rr, notes='bullish_engulfing', timestamp=latest['timestamp'])
        # Los patrones envolventes son fuertes y no necesitan confirmación para este ejemplo
        # Si quisiéramos confirmación, añadiríamos: pending_state = {"pattern": "bullish_engulfing", ...}

    # --- NUEVO: Patrón Envolvente Bajista (Bearish Engulfing) ---
    if utils.is_bearish_engulfing(latest, previous) and is_not_oversold:
        signal_text = "🕯️ Envolvente Bajista detectado"
        if poc_zone:
            signal_text += f" en ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📉"

        # MEJORA: Entrada por retroceso al 50% de la vela de la señal
        pullback_entry = (latest['high'] + latest['low']) / 2
        entry, sl, tp, rr = utils.compute_risk_levels(pullback_entry, latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'SHORT_ENGULFING', entry, sl, tp, latest['ATR'], rr, notes='bearish_engulfing', timestamp=latest['timestamp'])

    # --- NUEVO: Patrón de Continuación 'Three White Soldiers' ---
    if utils.is_three_white_soldiers(df.tail(3)) and is_not_overbought:
        signal_text = "=> Three White Soldiers detectado"
        # Para patrones de momentum, entramos al cierre, sin esperar retroceso.
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'LONG_3SOLDIERS', entry, sl, tp, latest['ATR'], rr, notes='three_white_soldiers', timestamp=latest['timestamp'])

    # --- NUEVO: Patrón de Continuación 'Three Black Crows' ---
    if utils.is_three_black_crows(df.tail(3)) and is_not_oversold:
        signal_text = "=> Three Black Crows detectado"
        # Para patrones de momentum, entramos al cierre, sin esperar retroceso.
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'SHORT_3CROWS', entry, sl, tp, latest['ATR'], rr, notes='three_black_crows', timestamp=latest['timestamp'])


    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(latest['timestamp'], unit='ms').isoformat()
        utils.save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    return "\n".join(signals) if signals else "⏳ Sin señales claras."

# === MODO BACKTESTING ===
def run_backtest(args):
    logging.info(f"Iniciando backtest de detección de señales desde: {args.backtest_file}")
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

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
    # --- MEJORA: Usar un archivo de log específico para el backtest ---
    backtest_log_file = args.trades_log_file.replace('.csv', '_backtest.csv')
    
    # Limpiar el log de trades antes de empezar un nuevo backtest
    if os.path.exists(backtest_log_file):
        os.remove(backtest_log_file)
    utils.ensure_trades_log_exists(backtest_log_file)

    logging.info("Recorriendo velas para encontrar y registrar señales...")
    utils.clear_state() # Asegurar un estado limpio al inicio del backtest

    for i in range(201, len(df)): # Empezamos más tarde para asegurar que todos los indicadores están maduros
        sub_df = df.iloc[:i].copy()
        
        pending_state = utils.load_state()
        signal_message = ""

        if pending_state:
            signal_message = check_confirmation(sub_df, pending_state, args)
        else:
            signal_message = evaluate_trade(sub_df, args, backtest_log_file)
        
        if signal_message and "⏳ Sin señales claras." not in signal_message and "⚠️ Volatilidad baja" not in signal_message:
            logging.info(f"[{i}] {signal_message}")

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
    logging.info("Iniciando bot de Patrones de Velas con la siguiente configuración:")
    logging.info(f"Símbolo: {args.symbol} | Intervalo: {args.interval}")
    logging.info(f"POC: {args.poc} | ATR Window: {args.atr_window} | Bollinger Window: {args.bollinger_window}")
    logging.info(f"Volumen SMA Period: {args.volume_sma_period}")
    logging.info(f"Risk Stop Mult: {args.risk_stop_mult} | Risk TP Mult: {args.risk_tp_mult} | SL Buffer: {args.sl_buffer}")
    logging.info(f"Trades Log File: {args.trades_log_file}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    utils.clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    startup_message = (
        f"🚀 *Bot de Patrones de Velas Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"Estrategia: Detección de Hammers y Shooting Stars con filtro de volatilidad ATR\\."
    )

    utils.run_bot_main_loop(args, telegram_token, chat_id, evaluate_trade, check_confirmation, startup_message)
